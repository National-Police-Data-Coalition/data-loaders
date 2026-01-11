from datetime import date
from loader.utils.query import RelQuery
from loader.domain.types.enums import State, PropertyEnum
from loader.domain.infra.locations import StateNode, CountyNode, CityNode
from loader.domain.source import Citation
from loader.domain.officer import Officer

from neomodel import (
    AsyncStructuredNode,
    StringProperty,
    AsyncRelationship,
    AsyncRelationshipTo,
    DateProperty,
    UniqueIdProperty,
    One,
    adb
)


class Jurisdiction(str, PropertyEnum):
    FEDERAL = "FEDERAL"
    STATE = "STATE"
    COUNTY = "COUNTY"
    MUNICIPAL = "MUNICIPAL"
    PRIVATE = "PRIVATE"
    OTHER = "OTHER"


class Unit(AsyncStructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True, index=True)
    hq_state = StringProperty(choices=State.choices(), required=True)
    hq_address = StringProperty()
    hq_city = StringProperty()
    hq_zip = StringProperty()
    phone = StringProperty()
    email = StringProperty()
    website_url = StringProperty()
    description = StringProperty()
    date_established = DateProperty()

    # Relationships
    agency = AsyncRelationship("Agency", "ESTABLISHED_BY", cardinality=One)
    citations = AsyncRelationshipTo(
        'loader.domain.source.Source', "UPDATED_BY", model=Citation)
    city_node = AsyncRelationshipTo(
        "loader.domain.infra.locations.CityNode", "WITHIN_CITY")

    def __repr__(self):
        return f"<Unit {self.name}>"

    async def get_primary_source(self):
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
        result, meta = await adb.cypher_query(cy, {'uid': self.uid}, resolve_objects=True)
        if result:
            source_node = result[0][0]
            return source_node
        return None

class Agency(AsyncStructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True, index=True)
    hq_state = StringProperty(choices=State.choices(), required=True)
    hq_address = StringProperty()
    hq_city = StringProperty()
    hq_zip = StringProperty()
    phone = StringProperty()
    email = StringProperty()
    website_url = StringProperty()
    description = StringProperty()
    jurisdiction = StringProperty(choices=Jurisdiction.choices())

    # Relationships
    citations = AsyncRelationshipTo(
        'loader.domain.source.Source', "UPDATED_BY", model=Citation)
    city_node = AsyncRelationshipTo(
        "loader.domain.infra.locations.CityNode", "LOCATED_IN")

    def __repr__(self):
        return f"<Agency {self.name}>"

    def get_units(self) -> RelQuery:
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
