from __future__ import annotations  # allows type hinting of class itself
from loader.domain.types.enums import PropertyEnum
from datetime import datetime
from neomodel import (
    AsyncStructuredNode, AsyncStructuredRel,
    AsyncRelationshipTo, AsyncRelationshipFrom,
    StringProperty, DateTimeNeo4jFormatProperty,
    UniqueIdProperty, BooleanProperty,
    EmailProperty, JSONProperty
)


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
    date_joined = DateTimeNeo4jFormatProperty(default_now=True)
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


class Citation(AsyncStructuredRel):
    timestamp = DateTimeNeo4jFormatProperty(
        default_now=True,
        index=True
    )
    url = StringProperty()
    user_uid = StringProperty()
    diff = JSONProperty()

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Citation {self.timestamp}>"

    # @property
    # def diffs(self):
    #     """Read-only access to diffs."""
    #     return self._diffs

    # def add_diff(self, url: str, diff: dict, timestamp: datetime):
    #     new_diff = json.loads({
    #         "url": url,
    #         "diff": diff,
    #         "timestamp": timestamp.isoformat()
    #     })
    #     self._diffs.append(new_diff)
    #     self.save()


class Source(AsyncStructuredNode):
    __property_order__ = [
        "uid", "name", "url",
        "contact_email"
    ]
    uid = UniqueIdProperty()

    name = StringProperty(unique_index=True)
    url = StringProperty()
    contact_email = StringProperty(required=True)

    # Relationships
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
