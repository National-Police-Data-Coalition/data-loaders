import importlib
import logging
from neomodel import adb


COMPOSITE_INDEX_CYPHER = """
CREATE CONSTRAINT agency_state_name_unique IF NOT EXISTS
FOR (a:Agency)
REQUIRE (a.hq_state, a.name) IS UNIQUE;
"""

async def install_schema() -> None:
    importlib.import_module("loader.domain")

    await adb.install_all_labels()
    logging.info("Neo4j schema installation complete.")

    await adb.cypher_query(COMPOSITE_INDEX_CYPHER)