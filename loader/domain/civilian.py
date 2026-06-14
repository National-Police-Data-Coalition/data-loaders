"""Define the Classes for Civilians."""
from loader.domain.types.enums import Ethnicity, Gender
from neomodel import (
    AsyncStructuredNode,
    StringProperty,
    IntegerProperty,
    UniqueIdProperty,
)


class Civilian(AsyncStructuredNode):
    uid = UniqueIdProperty()
    civ_id = StringProperty()
    age = IntegerProperty()
    age_range = StringProperty()
    ethnicity = StringProperty(choices=Ethnicity.choices())
    gender = StringProperty(choices=Gender.choices())
