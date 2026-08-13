from __future__ import annotations  # allows type hinting of class itself
from loader.domain.types.enums import PropertyEnum
from loader.utils.change_uid import (
    canonical_change_timestamp,
    canonical_change_url,
    det_change_uid,
)
from datetime import datetime, timezone
from neomodel import (
    AsyncStructuredNode, AsyncStructuredRel,
    AsyncRelationship, AsyncRelationshipTo, AsyncRelationshipFrom,
    StringProperty, DateTimeProperty,
    UniqueIdProperty, BooleanProperty,
    EmailProperty,
    AsyncZeroOrOne, AsyncOne, adb
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


class Change(AsyncStructuredNode):
    uid = StringProperty(unique_index=True, required=True)
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
        timestamp: datetime | None = None,
    ) -> Change:
        timestamp = timestamp or datetime.now(timezone.utc)
        target_uid = getattr(self, "uid", None)
        source_uid = getattr(source, "uid", None)
        if not target_uid or not source_uid:
            raise ValueError("Change creation requires target and source uid values")

        change_uid = det_change_uid(target_uid, source_uid, timestamp, url)
        timestamp_value = canonical_change_timestamp(timestamp)
        url_value = canonical_change_url(url)
        cypher = """
        MERGE (change:Change {uid: $uid})
        SET
            change.timestamp = datetime($timestamp),
            change.url = $url,
            change.diff = $diff
        RETURN change
        """
        result, _ = await adb.cypher_query(
            cypher,
            {
                "uid": change_uid,
                "timestamp": timestamp_value,
                "url": url_value,
                "diff": diff,
            },
            resolve_objects=True,
        )
        change = result[0][0]

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
    invitations = AsyncRelationshipTo(
        "Invitation", "HAS_PENDING_INVITATION")
    staged_invitations = AsyncRelationshipTo(
        "StagedInvitation", "PENDING_STAGED_INVITATION")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Source {self.uid}>"
