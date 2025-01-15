"""Define the SQL classes for Users."""

from models.types.enums import PropertyEnum
from neomodel import (
    Relationship, StructuredNode,
    StringProperty, DateProperty, BooleanProperty,
    UniqueIdProperty, EmailProperty
)
from models.source import SourceMember


class UserRole(str, PropertyEnum):
    PUBLIC = "Public"
    PASSPORT = "Passport"
    CONTRIBUTOR = "Contributor"
    ADMIN = "Admin"

    def get_value(self):
        if self == UserRole.PUBLIC:
            return 1
        elif self == UserRole.PASSPORT:
            return 2
        elif self == UserRole.CONTRIBUTOR:
            return 3
        else:
            return 4


# Define the User data-model.
class User(StructuredNode):
    __hidden_properties__ = ["password_hash"]
    __property_order__ = [
        "uid", "first_name", "last_name",
        "email", "email_confirmed_at",
        "phone_number", "role", "active"
    ]

    uid = UniqueIdProperty()
    active = BooleanProperty(default=True)

    # User authentication information. The collation="NOCASE" is required
    # to search case insensitively when USER_IFIND_MODE is "nocase_collation".
    email = EmailProperty(required=True, unique_index=True)
    email_confirmed_at = DateProperty()
    password_hash = StringProperty(required=True)

    # User information
    first_name = StringProperty(required=True)
    last_name = StringProperty(required=True)

    role = StringProperty(
        choices=UserRole.choices(), default=UserRole.PUBLIC.value)

    phone_number = StringProperty()

    # Data Source Relationships
    sources = Relationship(
        'models.source.Source',
        "MEMBER_OF_SOURCE", model=SourceMember)
    received_invitations = Relationship(
        'models.source.Invitation',
        "RECIEVED")
    extended_invitations = Relationship(
        'models.source.Invitation',
        "EXTENDED")
    entended_staged_invitations = Relationship(
        'models.source.StagedInvitation',
        "EXTENDED")

    @property
    def role_enum(self) -> UserRole:
        """
        Get the user's role as an enum.
        Returns:
            UserRole: The user's role as an enum.
        """
        return UserRole(self.role)

    @classmethod
    def get_by_email(cls, email: str) -> "User":
        """
        Get a user by their email address.

        Args:
            email (str): The user's email.

        Returns:
            User: The User instance if found, otherwise None.
        """
        try:
            return cls.nodes.get_or_none(email=email)
        except cls.DoesNotExist:
            return None
