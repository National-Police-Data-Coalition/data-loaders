"""Define the Classes for Civilians."""
from loader.models.types.enums import Ethnicity, Gender
from neomodel import (
    AsyncStructuredNode,
    StringProperty,
    IntegerProperty
)


class Civilian(AsyncStructuredNode):
    age = IntegerProperty()
    age_group = StringProperty()
    ethnicity = StringProperty(choices=Ethnicity.choices())
    gender = StringProperty(choices=Gender.choices())
