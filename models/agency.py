from datetime import date
from models.types.enums import State, PropertyEnum
from models.infra.locations import StateNode, CountyNode, CityNode
from models.source import Citation
from models.officer import Officer

from neomodel import (
    StructuredNode,
    StructuredRel,
    StringProperty,
    RelationshipTo,
    RelationshipFrom,
    DateProperty,
    UniqueIdProperty,
    One
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
    agency = RelationshipTo("Agency", "ESTABLISHED_BY", cardinality=One)
    commanders = RelationshipTo(
        "models.officer.Officer",
        "COMMANDED_BY", model=UnitMembership)
    officers = RelationshipFrom(
        "models.officer.Officer",
        "MEMBER_OF_UNIT", model=UnitMembership)
    citations = RelationshipTo(
        'models.source.Source', "UPDATED_BY", model=Citation)
    state_node = RelationshipTo(
        "models.infra.locations.StateNode", "WITHIN_STATE")
    county_node = RelationshipTo(
        "models.infra.locations.CountyNode", "WITHIN_COUNTY")
    city_node = RelationshipTo(
        "models.infra.locations.CityNode", "WITHIN_CITY")

    def __repr__(self):
        return f"<Unit {self.name}>"

    def get_current_commander(self):
        """
        Get the current commander of a unit.

        :param unit: The unit to get the commander for

        :return: The current commander
        """
        query = """
        MATCH (u:Unit {uid: $unit_uid})-[r:COMMANDED_BY]->(o:Officer)
        WHERE r.latest_date IS NULL
        RETURN o
        """
        results, meta = self.cypher(query, {
            "unit_uid": self.uid
        })
        if results:
            return Officer.inflate(results[0][0])
        return None

    def update_commander(self, officer: Officer, date: date):
        """
        Update the commander of a unit. Ends the term of the
        current commander (if needed) and creates a new relationship
        with the new commander.

        :param unit: The unit to update the commander for
        :param officer: The officer to set as the commander
        :param date: The date the officer became the commander
        """
        cur_com = self.get_current_commander()
        if cur_com:
            cur_com_rel = self.commanders.relationship(cur_com)
            cur_com_rel.latest_date = date
            cur_com_rel.save()
        self.commanders.connect(officer, {
            "earliest_date": date
        })


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
    units = RelationshipTo("Unit", "ESTABLISHED")
    citations = RelationshipTo(
        'models.source.Source', "UPDATED_BY", model=Citation)
    state_node = RelationshipTo(
        "models.infra.locations.StateNode", "WITHIN_STATE")
    county_node = RelationshipTo(
        "models.infra.locations.CountyNode", "WITHIN_COUNTY")
    city_node = RelationshipTo(
        "models.infra.locations.CityNode", "WITHIN_CITY")

    def __repr__(self):
        return f"<Agency {self.name}>"
