import os
import sys
import json
import argparse
import logging
import threading

from dotenv import dotenv_values
from datetime import datetime
import time
from deepdiff import DeepDiff
from neomodel import config, db, DeflateError
from models.complaint import (
    Complaint, Allegation, Penalty,
    Location
)
from models.civilian import Civilian
from models.attachment import Attachment
from models.officer import Officer, StateID
from models.agency import Unit, Agency
from models.source import Source
from models.infra.locations import (
    StateNode, CountyNode, CityNode, PrecinctNode
)

cfg = dotenv_values(".env")

log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_end = "_load.log"
log_path = log_dir + "/" + datetime.now().strftime("%Y-%m-%d:%H:%M:%S") + log_end

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

# Missing Data log
missing = []


def identify_source(data):
    """
    Identify the source of the data and return the Source node.

    :param data: The incoming data

    :return: The Source node
    """
    source_uid = data.get("source_uid")
    source = Source.nodes.get_or_none(uid=source_uid)
    if source is None:
        return None
    return source


def identify_agency(agency_label):
    """
    Identify the agency node by label. This should be either the UID or the name.

    :param agency_label: The label of the agency

    :return: The Agency node or None
    """
    a = Agency.nodes.get_or_none(uid=agency_label)
    if a is None:
        a = Agency.nodes.get_or_none(name=agency_label)
        if a is None:
            return None
        else:
            logging.info(f"Found Agency {a.uid} by name: {agency_label}")
            logging.warning(
                "Using agency name to find agency is not recommended." +
                " Use UID instead.")
    return a


def identify_unit(unit_label, agency):
    """
    Identify the unit node by label. This should be either the UID or the name.

    :param unit_label: The label of the unit
    :param agency: The agency the unit belongs to

    :return: The Unit node or None
    """
    try:
        u = agency.units.filter("u.uid = $unit_uid", unit_uid=unit_label).one_or_none()
        if u is None:
            u = agency.units.filter("u.name = $unit_name", unit_name=unit_label).one_or_none()
            if u is None:
                return None
            else:
                logging.info(f"Found Unit {u.uid} by name: {unit_label}")
                logging.warning(
                    "Using unit name to find unit is not recommended." +
                    " Use UID instead.")
    except ValueError as e:
        raise ValueError(f"Multiple Matching Units found for {unit_label}: {e}")
    return u


def get_scrape_date(data):
    """
    Get the scrape date from the incoming data.

    :param data: The incoming data

    :return: The scrape date
    """
    return datetime.strptime(
        data.get("scraped_at"), "%Y-%m-%d %H:%M:%S")


def source_outdated(item, source, data):
    """
    Check to see if the incoming data is more recent than
    the existing updates from the source.

    :param item: The item being updated
    :param source: The source of the update
    :param data: The incoming data

    :return: True if the incoming data is outdated, False otherwise
    """
    scrape_date = get_scrape_date(data)
    scrape_url = data.get("url")
    query = """
    MATCH (i {uid: $item_uid})-[c:UPDATED_BY]->(s:Source {uid: $source_uid})
    WHERE datetime({epochSeconds: toInteger(c.date)}) >= datetime($scrap_date)
    AND c.url = $url
    RETURN c LIMIT 25
    """
    results, meta = db.cypher_query(query, {
        "item_uid": item.uid,
        "source_uid": source.uid,
        "scrap_date": scrape_date,
        "url": scrape_url
    })
    if results:
        logging.info(
            "Found {} more recent citations for item {} from {}".format(
                len(results),
                item.uid,
                scrape_url
            ))
        return True
    return False


def detect_diff(item, incoming_data):
    """
    Detect differences between existing and incoming data.

    :param item: The existing data node to be updated
    :param incoming_data: The incoming data

    :return: The differences detected
    """
    incoming_data_mapped = {
        k: v for k, v in incoming_data.items() if v is not None}
    # Ignore dynamically generated fields like uid
    ignore_fields = ['uid', 'element_id_property']
    existing_data = {
        k: v
        for k, v in item.__properties__.items()
        if k not in ignore_fields and v is not None
    }
    return DeepDiff(existing_data, incoming_data_mapped, ignore_order=True)


def link_location(item, state=None, city=None):
    """
    Link a node to the relevant location nodes.

    :param item: The item to link
    :param state: The state. Should be a two-letter State code or name.
    :param county: The county. Should be a FIPS code or county name.
    :param city: The city. Should be a SimpleMaps ID or city name.
    """
    if state is not None:
        state_node = StateNode.nodes.get_or_none(
            abbreviation=state)
        if state_node:
            # Add county and city if provided
            if city is not None:
                query = """
                MATCH (c:CityNode {name: $city})-[]-(:CountyNode)-[]-(s:StateNode {uid: $state})
                RETURN c LIMIT 25
                """
                results, meta = db.cypher_query(query, {
                    "city": city,
                    "state": state_node.uid
                })
                if results:
                    city_node = CityNode.inflate(results[0][0])
                    item.city_node.connect(city_node)
                    logging.info(f"Linked {item.uid} to City {city_node.uid}")
        else:
            logging.error(f"State not found: {state}")


def update_item(item, incoming_data):
    updates = {
        key: value
        for key, value in incoming_data.items()
        if value is not None
    }
    for key, value in updates.items():
        setattr(item, key, value)
    item.save()


def add_citation(item, source, data, diff: dict = None):
    """
    Add a citation to an item from a source.

    :param item: The item to add the citation to
    :param source: The source of the citation
    :param data: The citation data
    """
    scrape_date = get_scrape_date(data)

    context = {k: v for k, v in {
        "date": scrape_date,
        "url": data.get("url"),
        "diff": diff
    }.items() if v is not None}
    try:
        item.citations.connect(source, context)
    except Exception as e:
        logging.error(f"Error adding citation: {e} to {item.uid}")


def find_via_citation(label, source):
    logging.info(f"Finding {label} via citation")
    logging.info(f"Source: {source.uid}")
    """
    Find a node by a citation from a source.

    :param label: The label of the node to find
    :param source: The source of the citation

    :return: The node found, or None
    """
    query = """
    MATCH (n)-[c:UPDATED_BY]->(s:Source {uid: $source_uid})
    WHERE c.url ENDS WITH $label
    RETURN elementID(n) AS node_id, n, COLLECT(c) AS citations LIMIT 10
    """
    results, meta = db.cypher_query(query, {
        "label": label,
        "source_uid": source.uid
    })
    if results:
        if len(results) > 1:
            logging.warning(f"Found multiple nodes for label {label}")
            return None
        return results[0][1]
    return None


def follow_officer_ref(officer_ref, source):
    if officer_ref is None:
        return None
    o = Officer.nodes.get_or_none(uid=officer_ref)
    if o is None:
        o = find_via_citation(officer_ref, source)
        if o is None:
            logging.error(f"Officer not found: {officer_ref}")
            missing.append(officer_ref)
            return
        try:
            o = Officer.inflate(o)
        except Exception as e:
            logging.error(f"Error inflating officer {o}: {e}")
            return None
    return o


def convert_string_to_date(date_string):
    """
    Convert a string to a date object. Accepts YYYY-MM-DD or Month Year format.

    :param date_string: The string to convert

    :return: The date object
    """
    if date_string is None or date_string.strip() == "":
        return None
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        try:
            return datetime.strptime(date_string, "%B %Y").date()
        except ValueError:
            logging.error(f"Invalid date format: {date_string}")
            return None


def local_get_current_commander(unit):
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
    results, meta = db.cypher_query(query, {
        "unit_uid": unit.uid
    })
    if results:
        return results[0][0]
    return None


def find_existing_complaint(url, complaint_data, source):
    """
    Find an existing complaint that matches the incoming data.
    """

    query = """
    MATCH (s:Source {uid: $source_uid})-[:REPORTED]->(c:Complaint)
    WHERE c.record_id = $c_record_id
    RETURN c
    """
    results, meta = db.cypher_query(query, {
        "c_record_id": complaint_data['record_id'],
        "source_uid": source.uid
    })
    if results:
        if len(results) > 1:
            logging.warning(
                "Found multiple complaints with Record ID {} from {}".format(
                    complaint_data['record_id'],
                    source.uid
                ))
        return Complaint.inflate(results[0][0])
    else:
        c = find_via_citation(url, source)
        if c is None:
            return None
        try:
            c = Complaint.inflate(c)
        except Exception as e:
            logging.error(f"Error inflating complaint: {e}")
            return None
        return c


def create_allegation(data, source):
    o_uid = data.pop('perpetrator_uid', None)
    civ = data.pop('complainant', None)

    o = follow_officer_ref(o_uid, source)

    c = None
    if civ is not None:
        try:
            c = Civilian(**civ).save()
        except DeflateError as e:
            logging.error(f"Failed to create civilian {civ}: {e}")

    try:
        a = Allegation(**data).save()
    except DeflateError as e:
        logging.error(f"Failed to create allegation {data}: {e}")
        if c:
            c.delete()
        return None
    if c:
        a.complainant.connect(c)
    if o:
        a.accused.connect(o)
    return a


def create_penalty(data, source):
    o_uid = data.pop('officer_uid')

    o = follow_officer_ref(o_uid, source)
    try:
        p = Penalty(**data).save()
    except DeflateError as e:
        logging.error(f"Failed to create penalty {data}: {e}")
        return None
    if o:
        p.officer.connect(o)
    return p


def load_complaint(data):
    complaint_data = data.get("data", {})
    source_details = complaint_data.pop('source_details', {})
    location = complaint_data.pop('location', {})
    attachments = complaint_data.pop('attachments', [])
    allegations = complaint_data.pop('allegations', [])
    # investigations = complaint_data.pop('investigations', [])
    penalties = complaint_data.pop('penalties', [])

    source = identify_source(data)
    if source is None:
        logging.error(f"Source not found: {data.get('source')}")
        return
    complaint = find_via_citation(
        data.get("url"),
        source
    )
    if complaint is not None:
        try:
            complaint = Complaint.inflate(complaint)
        except Exception as e:
            logging.error(f"Error inflating complaint: {e}")
            return
        logging.info(f"Updating complaint {complaint.uid}")
        # Check if the incoming data is more recent than the existing data
        if not source_outdated(complaint, source, data):
            diff = detect_diff(complaint, complaint_data)
            if diff:
                logging.info(
                    f"Differences detected for complaint {complaint.uid}: {diff}")
                update_item(complaint, complaint_data)
                add_citation(complaint, source, data, diff.to_dict())
        else:
            logging.info(f"Skipping outdated data for complaint {complaint.uid}")
            return
        return
    logging.info(f"Creating new complaint: {complaint_data['record_id']}")

    # Create Complaint
    complaint_data['incident_date'] = convert_string_to_date(
        complaint_data['incident_date'])
    complaint_data['received_date'] = convert_string_to_date(
        complaint_data['received_date'])
    complaint_data['closed_date'] = convert_string_to_date(
        complaint_data['closed_date'])

    try:
        complaint = Complaint(**complaint_data).save()
    except DeflateError as e:
        logging.error("Failed to create complaint ({}): {}".format(
            json.dumps(complaint_data),
            e
        ))
        return

    # Connect Source
    try:
        complaint.source_org.connect(
            source,
            source_details
        )
        add_citation(complaint, source, data)
    except Exception as e:
        logging.error(
            "Failed to connect source: {} - Discarding complaint {}.".format(
                e,
                complaint_data
            ))
        complaint.delete()
        return

    # Add Location
    try:
        loc = Location(**location).save()
        complaint.location.connect(loc)
    except DeflateError as e:
        logging.error("Failed to create location ({}): {}".format(
            json.dumps(location),
            e
        ))

    # Add Attachements
    for a_data in attachments:
        try:
            a = Attachment(**a_data).save()
            complaint.attachments.connect(a)
        except DeflateError as e:
            logging.error("Failed to create attachement ({}): {}".format(
                json.dumps(complaint_data),
                e
            ))

    # Create Allegations
    for a_data in allegations:
        a = create_allegation(a_data, source)
        if a is not None:
            a.complaint.connect(complaint)
        else:
            logging.error("Failed to create allegation ({}): {}".format(
                json.dumps(a_data),
                e
            ))

    # Create Penalties
    for p_data in penalties:
        p = create_penalty(p_data, source)
        if p is not None:
            p.complaint.connect(complaint)
        else:
            logging.error("Failed to create penalty ({}): {}".format(
                json.dumps(p_data),
                e
            ))

    return


def load_officer(data):
    officer_data = data.get("data", {})
    employment_data_list = data.get("employment", [])

    state_id_data = officer_data.pop("state_ids")

    source = identify_source(data)
    if source is None:
        logging.error(f"Source not found: {data.get('source')}")
        return

    # Get the State ID node
    sid = StateID.nodes.get_or_none(
        id_name=state_id_data[0]['id_name'],
        state=state_id_data[0]['state'],
        value=state_id_data[0]['value']
    )

    if sid is None:
        logging.info(f"State ID not found: {state_id_data[0]}")
        sid = StateID(**state_id_data[0]).save()
        try:
            o = Officer(**officer_data).save()
            sid.officer.connect(o)
        except DeflateError as e:
            sid.delete()
            logging.error("Failed to create officer: {} Data:\n{}".format(
                e,
                json.dumps(officer_data)
            ))
            return
        add_citation(o, source, data)
        logging.info(f"Created Officer: {o}")
    else:
        o = sid.officer.single()
        # Check if the incoming data is more recent than the existing data
        if not source_outdated(o, source, data):
            diff = detect_diff(o, officer_data)
            if diff:
                logging.info(
                    f"Differences detected for officer {o.uid}: {diff}")
                update_item(o, officer_data)
                add_citation(o, source, data, diff.to_dict())
        else:
            logging.warning(f"Skipping outdated data for officer {o.uid}")
            return

    # Create Employment nodes and relationships
    for employment_data in employment_data_list:
        unit_label = employment_data.pop("unit_uid", None)
        agency_label = employment_data.pop("agency_uid", None)
        if unit_label is None or agency_label is None:
            logging.error(f"Incomplete employment data for officer {o.uid}")
            continue
        agency = identify_agency(agency_label)
        if agency is None:
            logging.error(f"Agency not found: {agency_label} for officer {o.uid}")
            missing.append(agency_label)
            continue

        # Check for a citation reference
        if unit_label.startswith("$ref:"):
            unit_ref = unit_label[5:]
            try:
                result = find_via_citation(unit_ref, source)
                u = Unit.inflate(result)
            except Exception as e:
                logging.error(f"Error finding unit by reference: {e}")
                continue
            if u is None:
                logging.error(f"Unit not found: {unit_ref}")
                missing.append(unit_ref)
                continue
        else:
            # Find the unit node by name or UID
            u = identify_unit(unit_label, agency)
            if u is None:
                if unit_label == "Unknown":
                    u = Unit(name="Unknown").save()
                    agency.units.connect(u)
                    u.agency.connect(agency)
                    logging.info(f"Created 'Unknown' unit for agency {agency.uid}")
                else:
                    logging.error(f"Unit not found: {unit_label}")
                    missing.append(unit_label)

        # See if the officer is already connected to the unit
        if u.officers.is_connected(o):
            logging.info(f"Officer {o.uid} already connected to unit {u.uid}")
            continue
        logging.info(f"Found Unit {u.uid} for officer {o.uid}")
        earliest_date = convert_string_to_date(
            employment_data.get("earliest_date"))
        latest_date = convert_string_to_date(
            employment_data.get("latest_date"))
        u.officers.connect(
            o,
            {
                "earliest_date": earliest_date,
                "latest_date": latest_date,
                "badge_number": employment_data.get("badge_number"),
                "highest_rank": employment_data.get("highest_rank")
            }
        )


def load_agency(data):
    agency_data = data.get("data", {})
    source = identify_source(data)
    if source is None:
        logging.error(f"Source not found:  {data.get('source')}")
        return

    a = identify_agency(agency_data.get("name"))
    if a is None:
        logging.info(f"Agency -{agency_data.get("name")}- not found. Creating Agency ")
        try:
            a = Agency(**agency_data).save()
        except Exception as e:
            logging.error(f"Failed to create agency {agency_data}: {e}")
            return
        add_citation(a, source, data)
        logging.info(f"Created Agency: {a.uid}")

        # Link location
        try:
            link_location(
                a,
                state=a.hq_state,
                city=a.hq_city,
            )
        except Exception as e:
            logging.error(f"Error linking location {a.name}: {e}")

    else:
        logging.info(f"Found Agency {a.uid}")
        if source_outdated(a, source, data):
            logging.warning(f"Skipping outdated data for agency {a.uid}")
            return
        else:
            diff = detect_diff(a, agency_data)
            if diff:
                logging.info(
                    f"Differences detected for agency {a.uid}: {diff}")
                update_item(a, agency_data)
                add_citation(a, source, data, diff.to_dict())


def load_unit(data):
    unit_data = data.get("data", {})

    source = identify_source(data)
    if source is None:
        logging.error(f"Source not found for unit {unit_data['name']}")
        return

    # Find the agency node
    agency_label = data.get("agency", None)
    if agency_label is None:
        logging.error(f"No agency data found for unit {unit_data['name']}")
        return
    # UID Search
    a = Agency.nodes.get_or_none(uid=agency_label)
    if a is None:
        logging.info(f"Agency UID not found: {agency_label}. Trying name search.")
        a = Agency.nodes.get_or_none(name=agency_label)
        if a is None:
            logging.error(f"Agency not found: {agency_label}")
            return
        else:
            logging.info(f"Found Agency {a.uid} by name: {agency_label}")
            logging.warning(
                "Using agency name to find agency is not recommended." +
                " Use UID instead.")

    logging.info(f"Found Agency {a.uid} for unit {unit_data['name']}")
    # Check for an existing unit
    try:
        u = identify_unit(unit_data.get("name"), a)
    except ValueError as e:
        logging.error(f"Unable to create unit {unit_data['name']} for agency {a.uid}: {e}")
        return

    commander_label = unit_data.pop("commander_uid", None)

    if u is None:
        logging.info(f"Creating new unit: {unit_data['name']} under Agency {a.uid}")
        u = Unit(**unit_data).save()
        try:
            u.agency.connect(a)
        except Exception as e:
            logging.error(f"Error connecting unit to agency: {e}")
            u.delete()
            return
        try:
            link_location(u, state=u.state, city=u.city)
        except Exception as e:
            logging.error(f"Error linking location {u.name}: {e}")
            return

        add_citation(u, source, data)
        logging.info(f"Created Unit: {u.uid}")
    else:
        logging.info(f"Found Unit {u.uid}")
        if source_outdated(u, source, data):
            logging.warning(f"Skipping outdated data for unit {u.uid}")
        else:
            diff = detect_diff(u, unit_data)
            if diff:
                logging.info(
                    f"Differences detected for unit {u.uid}: {diff}")
                update_item(u, unit_data)
                add_citation(u, source, data, diff.to_dict())

    if commander_label:
        scrape_date = get_scrape_date(data).date()
        # Check for a citation reference
        if commander_label.startswith("$ref:"):
            commander_ref = commander_label[5:]
            result = find_via_citation(commander_ref, source)
            if result is None:
                logging.error(f"Commander not found: {commander_ref}")
                missing.append(commander_ref)
                return
            try:
                c = Officer.inflate(result)
            except Exception as e:
                logging.error(f"Error inflating commander: {e}")
                return
        else:
            # Find the officer node by name or UID
            c = Officer.nodes.get_or_none(uid=commander_label)
            if c is None:
                logging.error(f"Commander not found: {commander_label}")
                missing.append(commander_label)
                return

        # Confirm that the commmander is not already connected to the unit
        if u.commanders.is_connected(c):
            logging.info(
                f"Commander {c.uid} already connected to unit {u.uid}")
            c_rel = u.commanders.relationship(c)
            if c_rel.latest_date is None:
                return
            else:
                # Returning commander. Add a new relationship.
                u.update_commander(c, scrape_date)
        else:
            logging.info(f"Found New Commander {c.uid} for unit {u.uid}")
            u.update_commander(c, scrape_date)


import concurrent.futures
from functools import partial


def load_jsonl_to_neo4j(jsonl_filename, max_workers=4):
    if not os.path.exists(jsonl_filename):
        logging.error(f"File {jsonl_filename} does not exist.")
        return

    def process_line(line, lock):
        data = json.loads(line)
        model = data.get("model")

        with lock:
            if model == "officer":
                load_officer(data)
            elif model == "unit":
                load_unit(data)
            elif model == "complaint":
                load_complaint(data)
            elif model == "agency":
                load_agency(data)
            else:
                logging.error(f"Unknown model: {model}")

    lock = threading.Lock()
    with open(jsonl_filename, mode='r', encoding='utf-8') as jsonl_file:
        lines = jsonl_file.readlines()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(partial(process_line, lock=lock), lines)


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

    # Verify Neo4j connection
    try:
        db.cypher_query("RETURN 1")
    except Exception as e:
        logging.error(f"Failed to connect to Neo4j: {e}")
        print(f"Failed to connect to Neo4j: {e}", file=sys.stderr)
        sys.exit(1)

    jsonl_filename = args.input_file
    max_workers = args.workers

    load_jsonl_to_neo4j(jsonl_filename, max_workers)

    output_path = "_missing.log"
    output_path = datetime.now().strftime("%Y-%m-%d:%H:%M:%S") + output_path
    try:
        with open(output_path, 'w') as output_file:
            output_file.write('\n'.join(missing))
        print(f"Processing complete. Missing refs written to {output_path}")
    except Exception as e:
        print(f"Error writing to the output file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Completed in {end_time - start_time} seconds")
