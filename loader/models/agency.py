from datetime import date
from loader.utils.query import RelQuery
from loader.models.types.enums import State, PropertyEnum
from loader.models.infra.locations import StateNode, CountyNode, CityNode
from loader.models.source import Citation
from loader.models.officer import Officer

from neomodel import (
    StructuredNode,
    StructuredRel,
    StringProperty,
    Relationship,
    RelationshipTo,
    DateProperty,
    UniqueIdProperty,
    One,
    db
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
    date_established = DateProperty()

    # Relationships
    agency = Relationship("Agency", "ESTABLISHED_BY", cardinality=One)
    commanders = Relationship(
        "loader.models.officer.Officer",
        "COMMANDED_BY", model=UnitMembership)
    officers = Relationship(
        "loader.models.officer.Officer",
        "MEMBER_OF_UNIT", model=UnitMembership)
    citations = RelationshipTo(
        'loader.models.source.Source', "UPDATED_BY", model=Citation)
    city_node = RelationshipTo(
        "loader.models.infra.locations.CityNode", "WITHIN_CITY")

    def __repr__(self):
        return f"<Unit {self.name}>"

    @property
    def primary_source(self):
        """
        Get the primary source for this unit.
        Returns:
            Source: The primary source node for this unit.
        """
        cy = """
        MATCH (o:Unit {uid: $uid})-[r:UPDATED_BY]->(s:Source)
        RETURN s
        ORDER BY r.date DESC
        LIMIT 1;
        """
        result, meta = db.cypher_query(cy, {'uid': self.uid}, resolve_objects=True)
        if result:
            source_node = result[0][0]
            return source_node
        return None

    @property
    def current_commander(self):
        """
        Get the current commander of the unit.
        Returns:
            Officer: The current commander of the unit.
        """
        cy = """
        MATCH (u:Unit {uid: $uid})-[r:COMMANDED_BY]-(o:Officer)
        WITH u, r, o,
            CASE WHEN r.latest_date IS NULL THEN 1 ELSE 0 END AS isCurrent
        ORDER BY isCurrent DESC, r.earliest_date DESC
        RETURN o AS officer
        LIMIT 1;
        """
        result, meta = db.cypher_query(
            cy, {'uid': self.uid}, resolve_objects=True)
        if result:
            officer_node = result[0][0]
            return officer_node
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
    citations = RelationshipTo(
        'loader.models.source.Source', "UPDATED_BY", model=Citation)
    city_node = RelationshipTo(
        "loader.models.infra.locations.CityNode", "WITHIN_CITY")

    def __repr__(self):
        return f"<Agency {self.name}>"

    @property
    def units(self) -> RelQuery:
        """
        Query the units related to this agency.
        Returns:
            RelQuery: A query object for the Unit nodes associated
            with this agency.
        """
        base = """
        MATCH (a:Agency {uid: $owner_uid})-[:ESTABLISHED_BY]-(u:Unit)
        """
        return RelQuery(self, base, return_alias="u", inflate_cls=Unit)
