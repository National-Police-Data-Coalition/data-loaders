from loader.domain.types.enums import PropertyEnum
from loader.domain.source import HasCitations
from neomodel import (
    AsyncStructuredNode,
    StringProperty,
    AsyncRelationship,
    AsyncRelationshipTo,
    AsyncRelationshipFrom,
    DateProperty,
    UniqueIdProperty,
    AsyncOne
)


class LegalCaseType(str, PropertyEnum):
    CIVIL = "CIVIL"
    CRIMINAL = "CRIMINAL"


class CourtLevel(str, PropertyEnum):
    MUNICIPAL_OR_COUNTY = "Municipal or County"
    STATE_TRIAL = "State Trial Court"
    STATE_INTERMEDIATE_APPELLATE = "State Intermediate Appellate"
    STATE_HIGHEST = "State Highest"
    FEDERAL_DISTRICT = "Federal District"
    FEDERAL_APPELLATE = "Federal Appellate"
    US_SUPREME_COURT = "U.S. Supreme"


class Litigation(AsyncStructuredNode, HasCitations):
    uid = UniqueIdProperty()
    case_title = StringProperty()
    docket_number = StringProperty()
    court_name = StringProperty()
    court_level = StringProperty(choices=CourtLevel.choices())
    jurisdiction = StringProperty()
    state = StringProperty()
    description = StringProperty()
    start_date = DateProperty()
    settlement_date = DateProperty()
    settlement_amount = StringProperty()
    url = StringProperty()
    case_type = StringProperty(choices=LegalCaseType.choices())

    # Relationships
    defendants = AsyncRelationshipTo("Officer", "NAMED_IN")

    def __repr__(self):
        return f"<Litigation {self.uid}:{self.case_title}>"


class Document(AsyncStructuredNode):
    uid = UniqueIdProperty()
    title = StringProperty()
    description = StringProperty()
    url = StringProperty()

    # Relationships
    litigation = AsyncRelationship("Litigation", "HAS_DOCUMENT", cardinality=AsyncOne)


class Disposition(AsyncStructuredNode):
    description = StringProperty()
    date = DateProperty()
    disposition = StringProperty()

    # Relationships
    litigation = AsyncRelationshipFrom("Litigation", "DISPOSED_IN", cardinality=AsyncOne)
