from loader.domain.types.enums import PropertyEnum, State, Ethnicity, Gender
from loader.domain.source import HasCitations

from neomodel import (
    AsyncStructuredNode, AsyncRelationship,
    StringProperty, IntegerProperty,
    UniqueIdProperty, AsyncOne
)


# Enums - Not yet used for validation, but could be in the future
class StateIDType(PropertyEnum):
    TAX_ID_NUMBER = "TAX_ID_NUMBER"
    NPI_ID = "NPI_ID"


class StateID(AsyncStructuredNode):
    """
    Represents a Statewide ID that follows an offcier even as they move between
    law enforcement agencies. For example, in New York, this would be
    the Tax ID Number.
    """
    id_name = StringProperty()  # e.g. "Tax ID Number"
    state = StringProperty(choices=State.choices())  # e.g. "NY"
    value = StringProperty()  # e.g. "958938"
    officer = AsyncRelationship('Officer', "HAS_STATE_ID", cardinality=AsyncOne)

    def __repr__(self):
        return f"<StateID: {self.id_name}, {self.state}>"


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

    def __repr__(self):
        return f"<Officer {self.uid}>"
