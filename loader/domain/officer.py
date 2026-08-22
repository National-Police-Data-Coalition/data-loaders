from loader.domain.types.enums import PropertyEnum, State, Ethnicity, Gender
from loader.domain.source import HasCitations

from neomodel import (
    AsyncStructuredNode, AsyncRelationship, AsyncRelationshipTo,
    StringProperty, IntegerProperty, FloatProperty,
    UniqueIdProperty, DateTimeProperty, BooleanProperty,
    AsyncOne, AsyncZeroOrOne
)


# Enums - Not yet used for validation, but could be in the future
class StateIDType(PropertyEnum):
    TAX_ID_NUMBER = "TAX_ID_NUMBER"
    NPI_ID = "NPI_ID"


class StateIDNamespace(AsyncStructuredNode, HasCitations):
    """
    Represents an identifier namespace, such as IL / NPI ID or IL / CPDP UID.

    Sources can claim authority over these namespaces, and individual StateID
    nodes belong to one namespace based on their state and id_name.
    """
    uid = UniqueIdProperty()
    id_name = StringProperty(required=True)
    state = StringProperty(choices=State.choices(), required=True)
    description = StringProperty()

    def __repr__(self):
        return f"<StateIDNamespace: {self.id_name}, {self.state}>"


class StateID(AsyncStructuredNode):
    """
    Represents a Statewide ID that follows an officer even as they move between
    law enforcement agencies. For example, in New York, this would be
    the Tax ID Number.
    """
    id_name = StringProperty()  # e.g. "Tax ID Number"
    state = StringProperty(choices=State.choices())  # e.g. "NY"
    value = StringProperty()  # e.g. "958938"
    officer = AsyncRelationship('Officer', "HAS_STATE_ID", cardinality=AsyncOne)
    namespace = AsyncRelationshipTo(
        "StateIDNamespace",
        "IN_NAMESPACE",
        cardinality=AsyncOne,
    )

    def __repr__(self):
        return f"<StateID: {self.id_name}, {self.state}>"


class IdentityAssertionStatus(PropertyEnum):
    PROPOSED = "PROPOSED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    SUPERSEDED = "SUPERSEDED"


class IdentityAssertionPredicate(PropertyEnum):
    SAME_AS = "SAME_AS"
    NOT_SAME_AS = "NOT_SAME_AS"


class IdentityAssertionActorType(PropertyEnum):
    SYSTEM = "SYSTEM"
    SOURCE = "SOURCE"
    USER = "USER"


class IdentityAssertion(AsyncStructuredNode, HasCitations):
    """
    A claim about whether two source-specific identifiers describe the same entity.

    Assertions exist for unresolved identity claims. Authoritative identity is
    represented structurally by attaching multiple StateID nodes to one Officer.
    """
    uid = UniqueIdProperty()
    predicate = StringProperty(
        choices=IdentityAssertionPredicate.choices(),
        default=IdentityAssertionPredicate.SAME_AS.value,
        required=True,
    )
    status = StringProperty(
        choices=IdentityAssertionStatus.choices(),
        default=IdentityAssertionStatus.PROPOSED.value,
        required=True,
    )
    confidence = FloatProperty()
    method = StringProperty()
    reasons = StringProperty()
    basis = StringProperty()
    suggested_by_type = StringProperty(choices=IdentityAssertionActorType.choices())
    reviewed_by_type = StringProperty(choices=IdentityAssertionActorType.choices())
    created_at = DateTimeProperty(default_now=True)
    reviewed_at = DateTimeProperty()

    subject_state_id = AsyncRelationshipTo(
        "StateID",
        "ASSERTION_SUBJECT",
        cardinality=AsyncOne,
    )
    object_state_id = AsyncRelationshipTo(
        "StateID",
        "ASSERTION_OBJECT",
        cardinality=AsyncOne,
    )
    suggested_by_source = AsyncRelationshipTo(
        "loader.domain.source.Source",
        "SUGGESTED_BY",
        cardinality=AsyncZeroOrOne,
    )
    suggested_by_user = AsyncRelationshipTo(
        "loader.domain.user.User",
        "SUGGESTED_BY",
        cardinality=AsyncZeroOrOne,
    )
    accepted_by_sources = AsyncRelationshipTo(
        "loader.domain.source.Source",
        "ACCEPTED_BY",
    )
    rejected_by_sources = AsyncRelationshipTo(
        "loader.domain.source.Source",
        "REJECTED_BY",
    )
    reviewed_by_user = AsyncRelationshipTo(
        "loader.domain.user.User",
        "REVIEWED_BY",
        cardinality=AsyncZeroOrOne,
    )

    def __repr__(self):
        return f"<IdentityAssertion {self.predicate} {self.status}>"


class OfficerRecordFieldReview(AsyncStructuredNode):
    """
    Manual review metadata for one field on a source-specific officer record.

    Loader updates should not overwrite a field with an active review lock.
    """
    uid = UniqueIdProperty()
    field_name = StringProperty(required=True, index=True)
    reviewed_at = DateTimeProperty(default_now=True)
    lock_loader_updates = BooleanProperty(default=True)
    is_active = BooleanProperty(default=True)
    notes = StringProperty()

    reviewed_by = AsyncRelationshipTo(
        "loader.domain.user.User",
        "REVIEWED_BY",
        cardinality=AsyncOne,
    )
    reviewed_for_source = AsyncRelationshipTo(
        "loader.domain.source.Source",
        "REVIEWED_FOR_SOURCE",
        cardinality=AsyncOne,
    )

    def __repr__(self):
        return f"<OfficerRecordFieldReview {self.field_name}>"


class OfficerRecord(AsyncStructuredNode, HasCitations):
    """
    A source-specific representation of an officer.

    OfficerRecord preserves exactly how a source represents attributes such as
    name capitalization. The shared Officer node represents resolved identity.
    """
    uid = UniqueIdProperty()
    record_id = StringProperty(index=True)
    first_name = StringProperty()
    middle_name = StringProperty()
    last_name = StringProperty()
    suffix = StringProperty()
    ethnicity = StringProperty()
    gender = StringProperty()
    year_of_birth = IntegerProperty()

    officer = AsyncRelationshipTo(
        "Officer",
        "DESCRIBES_OFFICER",
        cardinality=AsyncOne,
    )
    source = AsyncRelationshipTo(
        "loader.domain.source.Source",
        "FROM_SOURCE",
        cardinality=AsyncOne,
    )
    state_ids = AsyncRelationshipTo(
        "StateID",
        "USES_STATE_ID",
    )
    field_reviews = AsyncRelationshipTo(
        "OfficerRecordFieldReview",
        "HAS_FIELD_REVIEW",
    )

    def __repr__(self):
        return f"<OfficerRecord {self.uid}>"


class OfficerCanonicalFieldPolicy(PropertyEnum):
    SOURCE_PRIORITY = "SOURCE_PRIORITY"
    MANUAL_SELECTION = "MANUAL_SELECTION"
    MOST_COMPLETE = "MOST_COMPLETE"
    MOST_RECENT = "MOST_RECENT"


class OfficerCanonicalField(AsyncStructuredNode, HasCitations):
    """
    Selects which OfficerRecord field supplies one denormalized Officer property.

    The selected value is cached here and on Officer for display/search, while
    the source record remains the durable representation from its source.
    """
    uid = UniqueIdProperty()
    field_name = StringProperty(required=True, index=True)
    value = StringProperty()
    policy = StringProperty(
        choices=OfficerCanonicalFieldPolicy.choices(),
        required=True,
    )
    selected_at = DateTimeProperty(default_now=True)
    is_active = BooleanProperty(default=True)
    notes = StringProperty()

    officer = AsyncRelationshipTo(
        "Officer",
        "CANONICAL_FIELD_FOR",
        cardinality=AsyncOne,
    )
    source_record = AsyncRelationshipTo(
        "OfficerRecord",
        "SELECTED_FROM_RECORD",
        cardinality=AsyncOne,
    )
    selected_by_source = AsyncRelationshipTo(
        "loader.domain.source.Source",
        "SELECTED_BY",
        cardinality=AsyncZeroOrOne,
    )
    selected_by_user = AsyncRelationshipTo(
        "loader.domain.user.User",
        "SELECTED_BY",
        cardinality=AsyncZeroOrOne,
    )

    def __repr__(self):
        return f"<OfficerCanonicalField {self.field_name}>"


class Officer(AsyncStructuredNode, HasCitations):
    __property_order__ = [
        "uid", "first_name", "middle_name",
        "last_name", "suffix", "ethnicity",
        "gender", "date_of_birth"
    ]

    uid = UniqueIdProperty()
    first_name = StringProperty()
    middle_name = StringProperty()
    last_name = StringProperty()
    suffix = StringProperty()
    ethnicity = StringProperty(choices=Ethnicity.choices())
    gender = StringProperty(choices=Gender.choices())
    year_of_birth = IntegerProperty()
    canonical_fields = AsyncRelationshipTo(
        "OfficerCanonicalField",
        "HAS_CANONICAL_FIELD",
    )

    def __repr__(self):
        return f"<Officer {self.uid}>"
