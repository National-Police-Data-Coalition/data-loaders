import os
import json
import argparse
import logging

from neomodel import config
from models.types.enums import Ethnicity
from models.officer import Officer, StateID
from models.agency import UnitMembership, Unit

GRAPH_USER = "neo4j"
GRAPH_NM_URI = "localhost:7687"
GRAPH_PASSWORD = "Vm*i.a3ip9B.6Q"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

# Neomodel setup
neo_url = "bolt://{user}:{pw}@{uri}".format(
    user=GRAPH_USER,
    pw=GRAPH_PASSWORD,
    uri=GRAPH_NM_URI
)
config.DATABASE_URL = neo_url


def map_ethnicity(ethnicity):
    if not ethnicity:
        return None

    ethnicity_mapping = {
        "black": Ethnicity.BLACK_AFRICAN_AMERICAN.value,
        "white": Ethnicity.WHITE.value,
        "asian": Ethnicity.ASIAN.value,
        "hispanic": Ethnicity.HISPANIC_LATINO.value,
        "native american": Ethnicity.AMERICAN_INDIAN_ALASKA_NATIVE.value,
        "native hawaiian": Ethnicity.NATIVE_HAWAIIAN_PACIFIC_ISLANDER.value,
    }

    for key, value in ethnicity_mapping.items():
        if key in ethnicity.lower():
            return value

    return None


def load_officer(data):
    officer_data = data.get("data", {})
    employment_data_list = data.get("employment", [])

    state_id_data = officer_data.pop("state_ids")

    # Get the State ID node
    sid = StateID.nodes.get_or_none(
        id_name=state_id_data[0]['id_name'],
        state=state_id_data[0]['state'],
        value=state_id_data[0]['value']
    )

    if sid is None:
        logging.info(f"State ID not found: {state_id_data[0]}")

        sid = StateID(**state_id_data[0]).save()
        if "ethnicity" in officer_data:
            officer_data["ethnicity"] = map_ethnicity(
                officer_data["ethnicity"])

        o = Officer(**officer_data).save()
        logging.info(f"Created Officer: {o}")

        sid.officer.connect(o)

    # # Create Employment nodes and relationships
    # for employment_data in employment_data_list:
    #     employment = UnitMembership(
    #         earliest_date=employment_data.get("earliest_date"),
    #         latest_date=employment_data.get("latest_date"),
    #         badge_number=employment_data.get("badge_number"),
    #         highest_rank=employment_data.get("highest_rank")
    #     )
    #     employment.save()
    #     employment.officer.connect(officer)


def load_jsonl_to_neo4j(jsonl_filename):
    if not os.path.exists(jsonl_filename):
        logging.error(f"File {jsonl_filename} does not exist.")
        return

    with open(jsonl_filename, mode='r', encoding='utf-8') as jsonl_file:
        for line in jsonl_file:
            data = json.loads(line)
            model = data.get("model")

            if model == "officer":
                load_officer(data)
            else:
                print(f"Unknown model: {model}")


def main():
    parser = argparse.ArgumentParser(
        description="Load data from JSONL file to Neo4j")
    parser.add_argument(
        "input_file",
        type=os.path.relpath,
        help="Input JSONL file to load data from"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    jsonl_filename = parser.parse_args().input_file

    load_jsonl_to_neo4j(jsonl_filename)


if __name__ == "__main__":
    main()
