import importlib
import logging
from neomodel import adb


AGENCY_STATE_NAME_CONSTRAINT = """
CREATE CONSTRAINT agency_state_name_unique IF NOT EXISTS
FOR (a:Agency)
REQUIRE (a.hq_state, a.name) IS UNIQUE;
"""

STATE_ID_KEY_CONSTRAINT = """
CREATE CONSTRAINT state_id_unique IF NOT EXISTS
FOR (sid:StateID)
REQUIRE (sid.state, sid.id_name, sid.value) IS UNIQUE;
"""

async def install_schema() -> None:
    importlib.import_module("loader.domain")

    await adb.install_all_labels()
    logging.info("Neo4j schema installation complete.")

    await adb.cypher_query(AGENCY_STATE_NAME_CONSTRAINT)
    await adb.cypher_query(STATE_ID_KEY_CONSTRAINT)
