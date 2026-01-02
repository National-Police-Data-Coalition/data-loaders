import importlib
import logging
from neomodel import adb

async def install_schema() -> None:
    importlib.import_module("loader.domain")

    await adb.install_all_labels()
    logging.info("Neo4j schema installation complete.")