import os
import csv
import argparse
import logging
import asyncio

from dotenv import dotenv_values
from datetime import datetime
import time
from neo4j.exceptions import TransientError
from neomodel import config, adb
from neomodel.contrib.spatial_properties import NeomodelPoint


from loader.domain.infra.locations import (
    StateNode, CountyNode, CityNode,
    STATE_INFO
)

cfg = dotenv_values(".env.cloud")

log_path = "_loc.log"
log_path = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + log_path

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format='%(asctime)s %(threadName)s %(levelname)s: %(message)s')


LOGGING_LEVELS = [
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR",
    "CRITICAL"
]

# Neomodel setup
neo_url = "bolt://{user}:{pw}@{uri}".format(
    user=cfg.get("GRAPH_USER"),
    pw=cfg.get("GRAPH_PASSWORD"),
    uri=cfg.get("GRAPH_NM_URI")
)
config.DATABASE_URL = neo_url

async def query_with_retry(cy: str, params: dict, attempts: int = 6) -> None:
    """
    Execute a Cypher query with retries on TransientError.

    :param cy: The Cypher query string
    :param params: The parameters for the query
    :param attempts: Number of retry attempts
    """
    for i in range(attempts):
        try:
            await adb.cypher_query(cy, params)
            return
        except TransientError as e:
            logging.warning(f"TransientError on attempt {i+1}/{attempts}: {e}")
            code = getattr(e, 'code', "") or ""
            is_deadlock = "DeadlockDetected" in code or "DeadlockDetected" in str(e)
            if not is_deadlock:
                raise
            if i == attempts - 1:
                logging.error(f"All {attempts} attempts failed for query: {cy} with params: {params}")
                raise
            delay = (0.05 * (2 ** i))  # Exponential backoff + jitter
            logging.info(f"Retrying in {delay:.2f} seconds...")
            await asyncio.sleep(delay)


async def upsert_states():
    """
    Insert states, Washington D.C., and U.S. territories into the database.
    """
    rows = [
        {
            "abbreviation": abbr,
            "name": info["name"],
        }
        for abbr, info in STATE_INFO.items()
    ]

    cy = """
    UNWIND $rows AS row
    MERGE (s:StateNode {abbreviation: row.abbreviation})
    SET
        s:Place,
        s.uid = coalesce(s.uid, randomUUID()),
        s.name = row.name
    """

    try:
        await adb.cypher_query(cy, {'rows': rows})
        logging.info(f"Upserted {len(rows)} states/territories.")
    except Exception as e:
        logging.error(f"Failed to upsert states/territories: {e}")
        raise


async def upsert_counties(batch):
    """
    Upsert a batch of counties into the database.
    Usees the data from SimpleMaps.
    https://simplemaps.com/data/us-cities
    :param batch: A list of county data dictionaries
    """
    cy = """
    UNWIND $rows AS row
    WITH row ORDER BY row.state_abbr, row.county_fips
    MATCH (s:StateNode {abbreviation: row.state_abbr})
    MERGE (c:CountyNode {fips: row.county_fips})
    SET
        c:Place,
        c.uid = coalesce(c.uid, randomUUID()),
        c.name = row.county_name
    MERGE (c)-[:WITHIN_STATE]->(s)
    """
    await query_with_retry(cy, {'rows': batch})


async def upsert_counties_in_batches(
        data: csv.DictReader,
        batch_size: int = 1000,
        concurrency: int = 4,
        queue_maxsize: int = 32
    ) -> None:
    """
    Upsert counties in batches with limited concurrency.
    :param data: An open csv.DictReader over the SimpleMaps US cities file
    :param batch_size: Number of counties to process in each batch
    :param concurrency: Number of concurrent upsert tasks
    :param queue_maxsize: Maximum size of the task queue
    """
    # First pass: collect unique counties keyed by (state_abbr, county_name)
    counties = {}
    for row in data:
        county_fips = row["county_fips"]
        key = county_fips
        if key not in counties:
            state_abbr  = row["state_id"]
            state_name  = row["state_name"]
            county_name = row["county_name"]
            counties[key] = {
                "state_abbr":  state_abbr,
                "state_name":  state_name,
                "county_name": county_name,
                "county_fips": county_fips,
            }

    q: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=queue_maxsize)

    async def worker(worker_id: int) -> None:
        while True:
            batch = await q.get()
            try:
                if batch is None:
                    return
                await upsert_counties(batch)
                logging.info(f"Worker {worker_id} processed batch of {len(batch)} counties")
            except Exception as e:
                logging.exception(f"Worker {worker_id} failed to upsert batch: {e}")
                raise
            finally:
                q.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]

    # Producer: read rows and enqueue batches
    batch: list[dict] = []
    for row in counties.values():
        batch.append(row)
        if len(batch) >= batch_size:
            await q.put(batch)
            batch = []
    if batch:
        await q.put(batch)

    # Signal the workers to exit
    for _ in range(concurrency):
        await q.put(None)

    await q.join()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def upsert_cities(batch):
    """
    Upsert a batch of cities into the database.
    Uses the data from SimpleMaps: https://simplemaps.com/data/us-cities

    :param batch: A list of city data dictionaries
    """
    cy = """
    UNWIND $rows AS row
    WITH row ORDER BY row.county_fips, row.sm_id
    MATCH (s:StateNode {abbreviation: row.state_abbr})
    MATCH (c:CountyNode {fips: row.county_fips})
    MERGE (city:CityNode {sm_id: row.sm_id})
    SET
        city:Place,
        city.uid = coalesce(city.uid, randomUUID()),
        city.name = row.city_name,
        city.population = row.population,
        city.coordinates = point({longitude: row.lng, latitude: row.lat})
    MERGE (city)-[:WITHIN_COUNTY]->(c)

    FOREACH (_ IN CASE WHEN row.is_capitol THEN [1] ELSE [] END |
        MERGE (city)-[:IS_CAPITOL]-(s)
    )
    """
    await query_with_retry(cy, {'rows': batch})


def _parse_city_row(row: dict) -> dict:
    sm_id = row.get("id")
    try:
        lat     = float(row["lat"])
        lng     = float(row["lng"])
    except ValueError:
        lat = 0.0
        lng = 0.0
        logging.error(f"Coordinates for SMID:{sm_id} are not floats; setting to 0.0")

    try:
        population = int(row["population"])
    except ValueError:
        population = 0
        logging.error(f"Population for SMID:{sm_id} is not an integer; setting to 0")

    is_capitol = (STATE_INFO.get(row["state_id"], {}).get("capital") == row["city"])

    return {
        "state_abbr": row["state_id"],
        "county_fips": row["county_fips"],
        "city_name": row["city"],
        "sm_id": sm_id,
        "lat": lat,
        "lng": lng,
        "population": population,
        "is_capitol": is_capitol, 
    }


async def upsert_cities_in_batches(
        data: csv.DictReader,
        batch_size: int = 1000,
        concurrency: int = 4,
        queue_maxsize: int = 32
    ) -> None:
    """
    Upsert cities in batches with limited concurrency.

    :param data: An open csv.DictReader over the SimpleMaps US cities file
    :param batch_size: Number of cities to process in each batch
    :param concurrency: Number of concurrent upsert tasks
    :param queue_maxsize: Maximum size of the task queue
    """
    q: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=queue_maxsize)

    async def worker(worker_id: int) -> None:
        while True:
            batch = await q.get()
            try:
                if batch is None:
                    return
                await upsert_cities(batch)
                logging.info(f"Worker {worker_id} processed batch of {len(batch)} cities")
            except Exception as e:
                logging.exception(f"Worker {worker_id} failed to upsert batch: {e}")
                raise
            finally:
                q.task_done()
    
    workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]

    # Producer: read rows and enqueue batches
    batch: list[dict] = []
    for row in data:
        parsed = _parse_city_row(row)
        batch.append(parsed)
        if len(batch) >= batch_size:
            await q.put(batch)
            batch = []
    
    if batch:
        await q.put(batch)

    # Stop workers
    for _ in workers:
        await q.put(None)
    
    # Wait for all tasks to complete
    await q.join()

    # Surfuce worker exceptions
    results = await asyncio.gather(*workers)
    for r in results:
        if isinstance(r, Exception):
            logging.exception(f"Worker raised exception: {r}")
            raise r


async def load_csv_to_neo4j(csv_filename):
    if not os.path.exists(csv_filename):
        logging.error(f"File {csv_filename} does not exist.")
        return
    
    with open(csv_filename, mode='r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    await adb.install_all_labels()
    await upsert_states()
    await upsert_counties_in_batches(rows)
    await upsert_cities_in_batches(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Load data from JSONL file to Neo4j")
    parser.add_argument(
        "input_file",
        type=os.path.relpath,
        help="Input JSONL file to load data from"
    )
    parser.add_argument(
        "-l", "--logging",
        type=str,
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=4,
        help="Number of worker threads (default: 4)"
    )

    args = parser.parse_args()

    if args.logging:
        log_level = args.logging.upper()
        if log_level not in LOGGING_LEVELS:
            logging.error(f"Invalid logging level: {log_level}")
            return
        logging.getLogger().setLevel(log_level)

    csv_filename = args.input_file

    asyncio.run(load_csv_to_neo4j(csv_filename))


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(end_time - start_time)
