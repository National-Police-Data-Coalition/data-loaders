from __future__ import annotations  # allows type hinting of class itself
from loader.domain.types.enums import PropertyEnum
from datetime import datetime
from neomodel import (
    AsyncStructuredNode, AsyncStructuredRel,
    AsyncRelationship, AsyncRelationshipTo, AsyncRelationshipFrom,
    StringProperty, DateTimeProperty,
    UniqueIdProperty, BooleanProperty,
    EmailProperty,
    AsyncZeroOrOne, AsyncOne
)

from loader.domain.contact import EmailContact, SocialMediaContact


class MemberRole(str, PropertyEnum):
    ADMIN = "Administrator"
    PUBLISHER = "Publisher"
    MEMBER = "Member"
    SUBSCRIBER = "Subscriber"

    def get_value(self):
        if self == MemberRole.ADMIN:
            return 1
        elif self == MemberRole.PUBLISHER:
            return 2
        elif self == MemberRole.MEMBER:
            return 3
        elif self == MemberRole.SUBSCRIBER:
            return 4
        else:
            return 5


class Invitation(AsyncStructuredNode):
    uid = UniqueIdProperty()
    role = StringProperty(choices=MemberRole.choices())
    is_accepted = BooleanProperty(default=False)
    # default to not accepted invite

    source_org = AsyncRelationshipFrom("Source", "INVITED_TO")
    user = AsyncRelationshipFrom(
        "loader.domain.user.User", "EXTENDED_TO")
    extender = AsyncRelationshipFrom(
        "loader.domain.user.User", "EXTENDED_BY")

    def serialize(self):
        return {
            'id': self.id,
            'source': self.source,
            'user': self.user,
            'role': self.role,
            'is_accepted': self.is_accepted,
        }


class StagedInvitation(AsyncStructuredNode):
    uid = UniqueIdProperty()
    role = StringProperty(choices=MemberRole.choices())
    email = EmailProperty()

    source_org = AsyncRelationshipFrom("Source", "INVITATION_TO")
    extender = AsyncRelationshipFrom(
        "loader.domain.user.User", "EXTENDED_BY")

    def serialize(self):
        return {
            'uid': self.uid,
            'source_uid': self.source_org,
            'email': self.email,
            'role': self.role
        }


class SourceMember(AsyncStructuredRel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    uid = UniqueIdProperty()
    role = StringProperty(choices=MemberRole.choices(), required=True)
    date_joined = DateTimeProperty(default=datetime.now())
    is_active = BooleanProperty(default=True)

    @property
    def role_enum(self) -> MemberRole:
        """
        Get the role as a MemberRole enum.
        Returns:
            MemberRole: The role as a MemberRole enum.
        """
        return MemberRole(self.role)

    def is_administrator(self):
        return self.role == MemberRole.ADMIN

    def get_default_role():
        return MemberRole.SUBSCRIBER

    def create(self, refresh: bool = True):
        self.date_joined = datetime.now()
        return super().create(refresh)

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<SourceMember( \
        id={self.uid}>"


class SourcePermissionType(str, PropertyEnum):
    ASSERT_SAME_IDENTITY = "ASSERT_SAME_IDENTITY"
    ACCEPT_IDENTITY_ASSERTION = "ACCEPT_IDENTITY_ASSERTION"


class SourcePermission(AsyncStructuredNode):
    """
    Permission granted by one source to another.

    For example, CPDP may allow an NPI-derived load to assert that a CPDP
    officer identifier belongs to the same person as an NPI officer identifier.
    """
    uid = UniqueIdProperty()
    permission_type = StringProperty(
        choices=SourcePermissionType.choices(),
        required=True,
    )
    scope = StringProperty()
    basis = StringProperty()
    granted_at = DateTimeProperty(default_now=True)
    expires_at = DateTimeProperty()
    revoked_at = DateTimeProperty()
    is_active = BooleanProperty(default=True)
    notes = StringProperty()

    granted_to = AsyncRelationshipTo(
        "Source",
        "GRANTED_TO_SOURCE",
        cardinality=AsyncOne
    )
    granted_by_user = AsyncRelationshipTo(
        "loader.domain.user.User",
        "GRANTED_BY",
        cardinality=AsyncOne
    )
    revoked_by_user = AsyncRelationshipTo(
        "loader.domain.user.User",
        "REVOKED_BY",
        cardinality=AsyncZeroOrOne
    )
    applies_to_namespaces = AsyncRelationshipTo(
        "loader.domain.officer.StateIDNamespace",
        "APPLIES_TO_NAMESPACE"
    )

    def __repr__(self):
        return f"<SourcePermission {self.permission_type}>"


class SourceIDNamespaceClaimType(str, PropertyEnum):
    OWNER = "OWNER"
    STEWARD = "STEWARD"
    CONTRIBUTOR = "CONTRIBUTOR"


class SourceIDNamespaceClaim(AsyncStructuredRel):
    """
    A source's claim of authority over an identifier namespace.

    Claims are made at the namespace level so ownership does not need to be
    repeated on every StateID value in that namespace.
    """
    uid = UniqueIdProperty()
    claim_type = StringProperty(
        choices=SourceIDNamespaceClaimType.choices(),
        required=True,
    )
    basis = StringProperty()
    claimed_at = DateTimeProperty(default_now=True)
    expires_at = DateTimeProperty()
    is_active = BooleanProperty(default=True)
    notes = StringProperty()

    def __repr__(self):
        return f"<SourceIDNamespaceClaim {self.claim_type}>"


class Change(AsyncStructuredNode):
    uid = UniqueIdProperty()
    timestamp = DateTimeProperty(
        default_now=True,
        index=True
    )
    url = StringProperty()
    diff = StringProperty()

    source = AsyncRelationshipTo(
        'loader.domain.source.Source',
        "ATTRIBUTED_TO",
        cardinality=AsyncOne
    )
    user = AsyncRelationshipTo(
        "loader.domain.user.User",
        "MADE_BY",
        cardinality=AsyncZeroOrOne
    )

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Change {self.timestamp}>"

    def serialize(self):
        return {
            "uid": self.uid,
            "timestamp": self.timestamp,
            "url": self.url,
            "diff": self.diff,
            "source": self.source,
            "user": self.user,
        }


class HasCitations:
    """Mixin for models whose changes are attributed to a Source."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not issubclass(cls, AsyncStructuredNode):
            raise TypeError(
                f"{cls.__name__} mixes in HasCitations "
                "but does not inherit AsyncStructuredNode"
            )

    changes = AsyncRelationshipFrom("loader.domain.source.Change", "CHANGE_TO")

    async def add_change(
        self,
        source: "Source",
        user: "loader.domain.user.User" | None = None,
        diff: str | None = None,
        url: str | None = None,
    ) -> Change:
        change = await Change(
            timestamp=datetime.now(),
            diff=diff,
            url=url,
        ).save()

        await change.source.connect(source)
        if user is not None:
            await change.user.connect(user)
        await self.changes.connect(change)
        return change


class Source(AsyncStructuredNode):
    __property_order__ = [
        "uid", "name", "url",
        "contact_email"
    ]
    uid = UniqueIdProperty()

    name = StringProperty(unique_index=True)
    url = StringProperty()
    description = StringProperty(max_length=500)

    # Relationships
    primary_email = AsyncRelationship(
        EmailContact, "HAS_CONTACT_EMAIL", cardinality=AsyncOne
    )
    social_media = AsyncRelationship(
        SocialMediaContact, "HAS_SOCIAL_MEDIA_CONTACT", cardinality=AsyncZeroOrOne
    )
    members = AsyncRelationshipFrom(
        "loader.domain.user.User",
        "IS_MEMBER", model=SourceMember)
    permissions_granted = AsyncRelationshipTo(
        "SourcePermission",
        "ISSUED_PERMISSION"
    )
    id_namespaces = AsyncRelationshipTo(
        "loader.domain.officer.StateIDNamespace",
        "CLAIMS_ID_NAMESPACE",
        model=SourceIDNamespaceClaim
    )
    invitations = AsyncRelationshipTo(
        "Invitation", "HAS_PENDING_INVITATION")
    staged_invitations = AsyncRelationshipTo(
        "StagedInvitation", "PENDING_STAGED_INVITATION")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Source {self.uid}>"
