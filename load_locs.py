import os
import sys
import json
import csv
import argparse
import logging
import threading

from dotenv import dotenv_values
from datetime import datetime
import time
from deepdiff import DeepDiff
from neomodel import config, db, DeflateError
from neomodel.contrib.spatial_properties import NeomodelPoint


from models.infra.locations import (
    State, County, City,
    STATE_INFO
)

cfg = dotenv_values(".env")

log_path = "_loc.log"
log_path = datetime.now().strftime("%Y-%m-%d:%H:%M:%S") + log_path

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



def insert_states():
    """
    Insert states, Washington D.C., and U.S. territories into the database.
    """

    for abbreviation, info in STATE_INFO.items():
        # Check if the state already exists
        existing_state = State.nodes.get_or_none(
            abbreviation=abbreviation
        )
        if existing_state:
            logging.info(f"State {info['name']} already exists; skipping")
            continue
        try:
            State(
                name=info['name'],
                abbreviation=abbreviation,
            ).save()
        except Exception as e:
            logging.error(f"Failed to create state {info['name']}: {e}")
            continue

def insert_counties(data: csv.DictReader):
    """
    Insert counties into the database.
    Usees the data from SimpleMaps.
    https://simplemaps.com/data/us-cities
    :param data: The data to insert
    """
    # First pass: collect unique counties keyed by (state_abbr, county_name)
    counties = {}
    for row in data:
        state_abbr  = row["state_id"]
        state_name  = row["state_name"]
        county_name = row["county_name"]
        county_fips = row["county_fips"]
        key = county_fips
        if key not in counties:
            counties[key] = {
                "state_abbr":  state_abbr,
                "state_name":  state_name,
                "county_name": county_name,
                "county_fips": county_fips,
            }

    # Second pass: upsert into Neo4j
    for info in counties.values():
        # 1) get or create the State
        state = State.nodes.get(
            abbreviation=info["state_abbr"]
        )
        if not state:
            logging.error(f"State {info['state_abbr']} not found.")
            continue

        # 2) get or create the County
        county = County.nodes.get_or_none(
            fips=info["county_fips"]
        )
        if county:
            logging.info(f"County {info['county_name']} already exists; skipping")
            continue
        county = County(
            name=info["county_name"],
            fips=info["county_fips"],
        ).save()

        # 3) connect State → County if not already
        if not state.counties.is_connected(county):
            state.counties.connect(county)

        logging.info(f"Inserted county {info['county_name']} in {info['state_abbr']}")


def insert_cities(data: csv.DictReader):
    """
    Insert cities into the database.
    Uses the data from SimpleMaps: https://simplemaps.com/data/us-cities

    :param data: An open csv.DictReader over the SimpleMaps US cities file
    """
    for row in data:
        state_abbr  = row["state_id"]
        county_fips = row["county_fips"]
        city_name   = row["city"]
        lat         = float(row["lat"])
        lng         = float(row["lng"])
        population  = row["population"]
        sm_id      = row["id"]  # if you later want to store or log it

        # 1) Lookup the State
        state = State.nodes.get_or_none(abbreviation=state_abbr)
        if not state:
            logging.warning(f"[Cities] State {state_abbr} not found for {city_name}; skipping")
            continue

        # 2) Lookup the County by its FIPS
        county = County.nodes.get_or_none(fips=county_fips)
        if not county:
            logging.warning(f"[Cities] County FIPS {county_fips} not found for {city_name}; skipping")
            continue

        # 3) See if the City already exists
        city = City.nodes.get_or_none(
            sm_id=sm_id
        )
        if not city:
            city = City(
                name=city_name,
                coordinates=NeomodelPoint((lng, lat), crs="wgs-84"),
                population=population,
                sm_id=sm_id,
            ).save()
            logging.info(f"[Cities] Created city {city_name}")
        else:
            logging.info(f"[Cities] City {city_name} already exists. Skipping insert.")

        # 5) Connect to State & County
        if not county.cities.is_connected(city):
            county.cities.connect(city)
            logging.info(f"[Cities] Connected {city_name} to county {county_fips}")
        if STATE_INFO[state_abbr]["capital"] == city_name:
            if not state.capital.is_connected(city):
                state.capital.connect(city)
                logging.info(f"[Cities] Connected {city_name} as capital of {state_abbr}")

    


# import concurrent.futures
# from functools import partial


def load_csv_to_neo4j(csv_filename, max_workers=4):
    if not os.path.exists(csv_filename):
        logging.error(f"File {csv_filename} does not exist.")
        return
    
    with open(csv_filename, mode='r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    insert_states()
    insert_counties(rows)
    insert_cities(rows)



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
    max_workers = args.workers

    load_csv_to_neo4j(csv_filename, max_workers)


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(end_time - start_time)
