"""Define the Classes for Civilians."""
from models.types.enums import Ethnicity, Gender
from neomodel import (
    StructuredNode,
    StringProperty,
    IntegerProperty,
    RelationshipTo
)


class Civilian(StructuredNode):
    age = IntegerProperty()
    age_group = StringProperty()
    ethnicity = StringProperty(choices=Ethnicity.choices())
    gender = StringProperty(choices=Gender.choices())
