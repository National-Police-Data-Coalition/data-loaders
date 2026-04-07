from loader.domain.types.enums import PropertyEnum
from loader.domain.properties.datetime import DateNeo4jFormatProperty
from loader.domain.source import Citation

from neomodel import (
    AsyncStructuredNode,
    StringProperty,
    DateProperty,
    UniqueIdProperty,
    AsyncRelationship,
    AsyncRelationshipTo,
    One
)

class EmploymentType(str, PropertyEnum):
    LAW_ENFORCEMENT = "LAW_ENFORCEMENT"
    CORRECTIONS = "CORRECTIONS"

class EmploymentStatus(PropertyEnum):
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"
    PROVISIONAL = "PROVISIONAL"
    TEMPORARY = "TEMPORARY"
    VOLUNTEER = "VOLUNTEER"

class EmploymentChange(PropertyEnum):
    PROMOTION = "PROMOTION"
    DEMOTION = "DEMOTION"
    TRANSFER = "TRANSFER"
    TERMINATION = "TERMINATION"
    RESIGNATION = "RESIGNATION"


class Employment(AsyncStructuredNode):
    uid = UniqueIdProperty()
    key = StringProperty()
    type = StringProperty()
    earliest_date = DateNeo4jFormatProperty()
    latest_date = DateNeo4jFormatProperty()
    badge_number = StringProperty()
    highest_rank = StringProperty()
    status = StringProperty()
    change = StringProperty()

    # Relationships
    officer = AsyncRelationship("loader.domain.officer.Officer", "HELD_BY", cardinality=One)
    unit = AsyncRelationship("loader.domain.agency.Unit", "IN_UNIT", cardinality=One)
    citations = AsyncRelationshipTo(
        'loader.domain.source.Source', "UPDATED_BY", model=Citation)

    def __repr__(self):
        return f"<Employment {self.uid}>"
    
    
class CommandAssignment(AsyncStructuredNode):
    uid = UniqueIdProperty()
    type = StringProperty()
    title = StringProperty()
    earliest_date = DateNeo4jFormatProperty()
    latest_date = DateNeo4jFormatProperty()
    badge_number = StringProperty()
    highest_rank = StringProperty()
    change = StringProperty()

    # Relationships
    officer = AsyncRelationship("loader.domain.officer.Officer", "COMMANDED_BY", cardinality=One)
    unit = AsyncRelationship("loader.domain.agency.Unit", "UNIT_ASSIGNED", cardinality=One)
    citations = AsyncRelationshipTo(
        'loader.domain.source.Source', "UPDATED_BY", model=Citation)

    def __repr__(self):
        return f"<CommandAssignment {self.uid}>"
