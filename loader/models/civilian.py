"""Define the Classes for Civilians."""
from loader.models.types.enums import Ethnicity, Gender
from neomodel import (
    StructuredNode,
    StringProperty,
    IntegerProperty,
    RelationshipTo
)


class Civilian(StructuredNode):
    civ_id = StringProperty()
    age = IntegerProperty()
    age_range = StringProperty()
    ethnicity = StringProperty(choices=Ethnicity.choices())
    gender = StringProperty(choices=Gender.choices())
