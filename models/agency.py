from models.types.enums import State, PropertyEnum

from neomodel import (
    StructuredNode,
    StructuredRel,
    StringProperty,
    RelationshipTo,
    RelationshipFrom,
    DateProperty,
    UniqueIdProperty
)


class Jurisdiction(str, PropertyEnum):
    FEDERAL = "FEDERAL"
    STATE = "STATE"
    COUNTY = "COUNTY"
    MUNICIPAL = "MUNICIPAL"
    PRIVATE = "PRIVATE"
    OTHER = "OTHER"


class UnitMembership(StructuredRel):
    earliest_date = DateProperty()
    latest_date = DateProperty()
    badge_number = StringProperty()
    highest_rank = StringProperty()


class Unit(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty()
    website_url = StringProperty()
    phone = StringProperty()
    email = StringProperty()
    description = StringProperty()
    address = StringProperty()
    city = StringProperty()
    state = StringProperty(choices=State.choices())
    zip = StringProperty()
    agency_url = StringProperty()
    officers_url = StringProperty()
    date_etsablished = DateProperty()

    # Relationships
    agency = RelationshipFrom("Agency", "ESTABLISHED_BY")
    commander = RelationshipTo(
        "models.officer.Officer",
        "COMMANDED_BY", model=UnitMembership)
    officers = RelationshipTo(
        "models.officer.Officer",
        "MEMBER_OF", model=UnitMembership)

    def __repr__(self):
        return f"<Unit {self.name}>"


class Agency(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty()
    website_url = StringProperty()
    hq_address = StringProperty()
    hq_city = StringProperty()
    hq_state = StringProperty(choices=State.choices())
    hq_zip = StringProperty()
    phone = StringProperty()
    email = StringProperty()
    description = StringProperty()
    jurisdiction = StringProperty(choices=Jurisdiction.choices())

    # Relationships
    units = RelationshipTo("Unit", "HAS_UNIT")

    def __repr__(self):
        return f"<Agency {self.name}>"
