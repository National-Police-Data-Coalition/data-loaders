from __future__ import annotations

import json
import logging
from typing import Any
from neomodel import adb
from .base import register
from loader.utils.citations import detect_diff_dict, parse_scraped_at


PREFETCH_CYPHER = """
UNWIND $rows AS row

// Resolve the State ID first
OPTIONAL MATCH (sid:StateID {
  state: row.sid_state,
  id_name: row.sid_id_name,
  id_value: row.sid_id_value
})

// Match Officer via StateID
OPTIONAL MATCH (o:Officer)-[:HAS_STATE_ID]-(sid)

MATCH (s:Source {uid: row.source_uid})

// Officer freshness
CALL {
  WITH o, s
  OPTIONAL MATCH (o)-[c:UPDATED_BY]->(s)
  // WHERE c.user_uid IS NULL
  RETURN max(c.timestamp) AS last_ts
}

// Resolve incoming employments
CALL {
  WITH row, o, s
  WITH row, o, s, coalesce(row.employments, []) AS emps
  UNWIND range(0, size(emps)-1) AS i
  WITH emps[i] AS emp, i, o, s

  // Resolve Unit via Agency+Unit name
  MATCH (a:Agency {name: emp.agency_uid, hq_state: emp.a_hq_state})
  OPTIONAL MATCH (a)<-[:ESTABLISHED_BY]-(u:Unit {name: emp.unit_uid, hq_state: emp.u_hq_state})

  // Match an existing Employment node if any
  OPTIONAL MATCH (e:Employment)-[:HELD_BY]-(u)
  WHERE u IS NOT NULL
    AND (e)-[:IN_UNIT]-(u)
    AND e.highest_rank = emp.highest_rank

  OPTIONAL MATCH (e)-[ec:UPDATED_BY]->(s)

  WITH i, emp, a, u, e, max(ec.timestamp) AS emp_last_ts

  RETURN collect({
    i: i,
    a_name: emp.agency_uid,
    a_hq_state: emp.a_hq_state,
    a_uid: CASE WHEN a IS NULL THEN NULL ELSE a.uid END,
    unit_name: emp.unit_uid,
    unit_hq_state: emp.u_hq_state,
    unit_uid: u.uid,

    employment_uid: e.uid,
    matched: (e IS NOT NULL),
    props: CASE WHEN e IS NULL THEN NULL ELSE properties(e) END,
    last_ts: emp_last_ts
  }) AS incoming_employments
}

RETURN {
  row_id: row.row_id,
  sid: CASE WHEN sid IS NULL THEN NULL ELSE sid.uid END,
  exists: o IS NOT NULL,
  existing: CASE WHEN o IS NULL THEN NULL ELSE properties(o) END,
  last_ts: last_ts,
  incoming_employments: incoming_employments
} AS record
"""

PREFETCH_WITH_EMPLOYMENT = """
UNWIND $rows AS row

// Resolve the State ID first
OPTIONAL MATCH (sid:StateID {
  state: row.sid_state,
  id_name: row.sid_id_name,
  id_value: row.sid_id_value
})

// Match Officer via StateID
OPTIONAL MATCH (o:Officer)-[:HAS_STATE_ID]-(sid)

MATCH (s:Source {uid: row.source_uid})

// Officer freshness
CALL {
  WITH o, s
  OPTIONAL MATCH (o)-[c:UPDATED_BY]->(s)
  // WHERE c.user_uid IS NULL
  RETURN max(c.timestamp) AS last_ts
}

// Resolve incoming employments
CALL {
  WITH row, o, s
  WITH row, o, s, coalesce(row.employments, []) AS emps
  UNWIND range(0, size(emps)-1) AS i
  WITH emps[i] AS emp, i, o, s

  // Resolve Unit via Agency+Unit name
  OPTIONAL MATCH (a:Agency {name: emp.a_label, hq_state: emp.a_hq_state})
    -[:ESTABLISHED_BY]-
    (u:Unit {name: emp.unit_label, hq_state: emp.unit_hq_state})

  // Match an existing Employment node if any
  OPTIONAL MATCH (e:Employment)-[:HELD_BY]-(u)
  WHERE u IS NOT NULL
    AND (e)-[:IN_UNIT]-(u)
    AND e.highest_rank = emp.highest_rank

  OPTIONAL MATCH (e)-[ec:UPDATED_BY]->(s)

  WITH i, emp, a, u, e, max(ec.timestamp) AS emp_last_ts

  RETURN collect({
    i: i,
    a_name: emp.a_name,
    a_hq_state: emp.a_hq_state,
    unit_name: emp.unit_name,
    unit_hq_state: emp.unit_hq_state,
    highest_rank: emp.highest_rank,

    // resolved + matched
    agency_uid: a.uid,
    unit_uid: u.uid,
    employment_uid: e.uid,
    matched: (e IS NOT NULL),

    props: CASE WHEN e IS NULL THEN NULL ELSE properties(e) END,
    last_ts: emp_last_ts
  }) AS incoming_employments
}

RETURN {
  row_id: row.row_id,
  sid: CASE WHEN sid IS NULL THEN NULL ELSE sid.uid END,
  exists: o IS NOT NULL,
  existing: CASE WHEN o IS NULL THEN NULL ELSE properties(o) END,
  last_ts: last_ts,
  incoming_employments: incoming_employments
} AS record
"""

UPSERT_CYPHER = """
UNWIND $rows AS row
MATCH (s:Source {uid: row.source_uid})
// Create StateID if missing
MERGE (sid:StateID {
  state: row.sid_state,
  id_name: row.sid_id_name,
  id_value: row.sid_id_value
})
MERGE (sid)-[:HAS_STATE_ID]-(o:Officer)
ON CREATE SET o.uid = replace(randomUUID(), "-", "")
SET o += row.props

MERGE (o)-[cit:UPDATED_BY {
  timestamp: datetime(row.scraped_dt),
  url: coalesce(row.url, \"\")
}]->(s)
SET
  cit.user_uid = NULL,
  cit.diff = row.diff

WITH o, row

// Employment nodes
CALL {
    WITH o, row
    UNWIND coalesce(row.employments, []) AS emp

    MATCH (u:Unit {uid: emp.unit_uid})

    MERGE (o)-[:HELD_BY]-(e:Employment {highest_rank: emp.highest_rank})-[:IN_UNIT]-(u)
    ON CREATE SET e.uid = replace(randomUUID(), "-", "")
    SET e += emp.props

    RETURN count(*) AS employments_updated
}


RETURN count(*) AS applied

"""

OFFICER_FIELDS = (
    "first_name",
    "middle_name",
    "last_name",
    "suffix",
    "ethnicity",
    "gender",
    "year_of_birth",
)

EMPLOYMENT_FIELDS = (
    "type",
    "earliest_date",
    "latest_date",
    "badge_number",
    "highest_rank",
    "status",
    "change",
)

COMMAND_ASSIGNMENT_FIELDS = (
    "type",
    "title",
    "earliest_date",
    "latest_date",
    "badge_number",
    "highest_rank",
    "change",
)


def build_props_map(data: dict[str, Any], fields) -> dict[str, Any]:
    # only include non-null keys so SET a += props won't overwrite with nulls
    props: dict[str, Any] = {}
    for k in fields:
        v = data.get(k)
        if v is not None:
            props[k] = v
    return props


@register("officer")
async def upsert_officer_batch(batch: list[dict[str, Any]]) -> None:
    # Build input rows (one per JSONL object)
    logging.info("Building officer upsert batch...")
    input_rows: list[dict[str, Any]] = []
    incoming_by_id: dict[int, dict[str, Any]] = {}
    incoming_emps_by_id = {}

    output = 5
    skipped = 0

    for i, item in enumerate(batch):
        d = item.get("data") or {}
        employments = item.get("employment", [])
        sids = d.get("state_ids", [])

        # Officer Identifiers
        primary_sid = sids[0] if sids else {}
        id_state = primary_sid.get("state")
        id_name = primary_sid.get("id_name")
        id_value = primary_sid.get("value")
        if not (id_state and id_name and id_value):
            continue
        f_name = d.get("first_name")
        l_name = d.get("last_name")
        if not (f_name and l_name):
            continue

        # Source
        source_uid = item.get("source_uid")
        url = item.get("url")
        scraped_at = item.get("scraped_at")
        if not (source_uid and url and scraped_at):
            continue
        scraped_dt = parse_scraped_at(scraped_at)

        # Employment records
        valid_employments = []
        for emp in employments:
            a_label = emp.get("agency_uid")
            a_hq_state = emp.get("a_hq_state")
            unit_label = emp.get("unit_uid")
            unit_hq_state = emp.get("u_hq_state")
            if not (a_label and a_hq_state and unit_label and unit_hq_state):
                continue
            valid_employments.append(emp)

        # if i < output:
        #     logging.info(f"Valid employment for officer {f_name} {l_name}:")
        #     logging.info(json.dumps(valid_employments))
        row = {
            "row_id": i,
            "sid_state": id_state,
            "sid_id_name": id_name,
            "sid_id_value": id_value,
            "employments": valid_employments,
            "source_uid": source_uid,
            "url": url,
            "scraped_dt": scraped_dt,
        }
        input_rows.append(row)
        incoming_by_id[i] = d
        incoming_emps_by_id[i] = valid_employments

    if not input_rows:
        return

    # --- 1) Prefetch existing + last citation date for (agency, source, url)

    results, _meta = await adb.cypher_query(
        PREFETCH_CYPHER, {"rows": input_rows})
    logging.info(f"Prefetch returned {len(results)} records.")

    # results rows come back as lists/tuples in neomodel; map by row_id
    # row shape: [row_id, agency_uid, exists, existing_map, last_ts]
    prefetch = {}
    for (record,) in results:
        row_id = int(record["row_id"])
        prefetch[row_id] = (
            record["sid"],
            bool(record["exists"]),
            record["existing"],
            record["last_ts"],
            record["incoming_employments"]
        )
        # if  output > 0:
        #     # logging.info(f"Incoming dat for row {row_id}: {incoming_emps_by_id[row_id]}")
        #     logging.info(f"Prefetched for row {row_id}: {prefetch[row_id]}")
        #     output -= 1

    # --- 2) Decide what to apply, and compute diffs only for fresh rows
    to_apply: list[dict[str, Any]] = []

    for r in input_rows:
        row_id = int(r["row_id"])
        incoming_data = incoming_by_id[row_id]
        incoming_emps = incoming_emps_by_id[row_id]

        sid_uid, exists, existing_map, last_ts, employments = prefetch.get(
            row_id, (None, False, None, None, []))

        # Freshness gate: skip if we already have a citation from
        # this source+url at or after this scraped time.
        if last_ts is not None and r["scraped_dt"] <= last_ts:
            continue

        props = build_props_map(incoming_data, OFFICER_FIELDS)
        emps = []

        if len(incoming_emps) != len(employments):
            logging.warning("Employment count mismatch during officer upsert diffing.")
            logging.warning(f"Incoming employments: {incoming_emps}")
            logging.warning(f"Prefetched employments: {employments}")
        for i, fetched in enumerate(employments):
            # Skip if we couldn't resolve unit
            unit_uid = fetched.get("unit_uid")
            if not unit_uid:
                continue
            inc_index = fetched["i"]
            emp = incoming_emps[inc_index]

            emp_props = build_props_map(emp, EMPLOYMENT_FIELDS)
            emps.append({
                "unit_uid": unit_uid,
                "highest_rank": emp.get("highest_rank"),
                "props": emp_props,
            })

        base_apply = {
            **r,
            "props": props,
            "sid": sid_uid,
            "employments": emps,
        }

        if not exists:
            # Creating: apply props + citation
            to_apply.append({
                **base_apply,
                "diff": None,
            })
            continue

        # Existing: only write if there are meaningful diffs
        diff = detect_diff_dict(existing_map or {}, incoming_data)
        if not diff:
            continue

        to_apply.append({
            **base_apply,
            "diff": json.loads(diff.to_json()),
        })

    if not to_apply:
        logging.info("No unit records to upsert.")
        return
    
    logging.info("Example upsert row:")
    logging.info(to_apply[0])

    # --- 3) Apply in one write query (cli wraps this in adb.write_transaction)
    logging.info(f"Upserting {len(to_apply)} unit records...")
    await adb.cypher_query(UPSERT_CYPHER, {"rows": to_apply})
