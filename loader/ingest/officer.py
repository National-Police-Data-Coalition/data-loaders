from __future__ import annotations

import json
import logging
from typing import Any
from neo4j import AsyncManagedTransaction
from .base import register
from .change import latest_change_timestamp_cypher, merge_change_cypher
from loader.utils.citations import detect_diff_dict, parse_scraped_at


PREFETCH_CYPHER = """
UNWIND $rows AS row

// Resolve the State ID first
OPTIONAL MATCH (sid:StateID {
  state: row.sid_state,
  id_name: row.sid_id_name,
  value: row.sid_id_value
})

// Match Officer via StateID
OPTIONAL MATCH (o:Officer)-[:HAS_STATE_ID]-(sid)

MATCH (s:Source {uid: row.source_uid})

// Officer freshness
""" + latest_change_timestamp_cypher(
    "o", "s", change_alias="officer_change", legacy_alias="officer_cit"
) + """

// Resolve incoming employments
CALL (row, o, s) {
  WITH row, o, s, coalesce(row.employments, []) AS emps
  UNWIND range(0, size(emps)-1) AS i
  WITH emps[i] AS emp, i, o, s

  // Resolve Unit via Agency+Unit name
  MATCH (a:Agency {name: emp.agency_label, hq_state: emp.a_hq_state})
  OPTIONAL MATCH (a)<-[:ESTABLISHED_BY]-(u:Unit {name: emp.unit_label, hq_state: emp.u_hq_state})

  // Match an existing Employment node if any
  OPTIONAL MATCH (e:Employment)-[:HELD_BY]-(u)
  WHERE u IS NOT NULL
    AND (e)-[:IN_UNIT]-(u)
    AND e.highest_rank = emp.highest_rank

""" + latest_change_timestamp_cypher(
    "e", "s", result_alias="emp_last_ts",
    change_alias="employment_change", legacy_alias="employment_cit"
) + """

  WITH i, emp, a, u, e, emp_last_ts

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

UPSERT_CYPHER = """
UNWIND $rows AS row
MATCH (s:Source {uid: row.source_uid})
// Create StateID if missing
MERGE (sid:StateID {
  state: row.sid_state,
  id_name: row.sid_id_name,
  value: row.sid_id_value
})
MERGE (sid)<-[:HAS_STATE_ID]-(o:Officer)
ON CREATE SET o.uid = replace(randomUUID(), "-", "")
SET o += row.props

""" + merge_change_cypher("o", "s", "officer_change") + """

WITH s, o, row

// Employment nodes
CALL (s, o, row){
    UNWIND coalesce(row.employments, []) AS emp

    MATCH (u:Unit {uid: emp.unit_uid})

    MERGE (o)<-[:HELD_BY]-(e:Employment {highest_rank: emp.highest_rank})-[:IN_UNIT]->(u)
    ON CREATE SET e.uid = replace(randomUUID(), "-", "")
    SET e += emp.props

""" + merge_change_cypher(
    "e", "s", "employment_change", diff_expr="emp.diff"
) + """

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
    "rank_label",
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
    "rank_label",
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
async def upsert_officer_batch(
    tx: AsyncManagedTransaction,
    batch: list[dict[str, Any]],
    log: logging.LoggerAdapter,
    ) -> None:
    # Build input rows (one per JSONL object)
    log.info("Building officer upsert batch...")
    input_rows: list[dict[str, Any]] = []
    incoming_by_id: dict[int, dict[str, Any]] = {}
    incoming_emps_by_id = {}

    output = 0
    dropped_e_expired = dropped_e_bad = dropped_e_unit = 0

    for i, item in enumerate(batch):
        fields = item.get("data") or {}
        employments = item.get("employment", [])
        sids = fields.get("state_ids", [])

        # Officer Identifiers
        primary_sid = sids[0] if sids else {}
        id_state = primary_sid.get("state")
        id_name = primary_sid.get("id_name")
        id_value = primary_sid.get("value")
        if not (id_state and id_name and id_value):
            continue
        f_name = fields.get("first_name")
        l_name = fields.get("last_name")
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
            a_label = emp.get("agency_label")
            a_hq_state = emp.get("a_hq_state")
            u_label = emp.get("unit_label")
            u_hq_state = emp.get("u_hq_state")
            if not (a_label and a_hq_state and u_label and u_hq_state):
                continue
            valid_employments.append(emp)

        if i < output:
            if len(valid_employments) > 0:
                log.info(f"Valid employments for officer {f_name} {l_name}:")
                log.info(json.dumps(valid_employments))
            else:
                log.info(f"No valid employments for officer {f_name} {l_name}.")
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
        incoming_by_id[i] = fields
        incoming_emps_by_id[i] = valid_employments

    if not input_rows:
        return

    # --- 1) Prefetch existing + last citation date for (officers and employments)
    log.info("Prefetching existing officer records...")
    # log.info(input_rows)
    results = await tx.run(PREFETCH_CYPHER, rows=input_rows)
    prefetch = {}
    async for rec in results:
        record = rec["record"]
        row_id = int(record["row_id"])
        prefetch[row_id] = (
            record["sid"],
            bool(record["exists"]),
            record["existing"],
            record["last_ts"],
            record["incoming_employments"]
        )
        if row_id < output:
            log.info(f"Prefetched officer record for row {row_id}:")
            log.info(json.dumps(record))
    await results.consume()

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

        # if len(incoming_emps) != len(employments):
        #     logging.warning("Employment count mismatch during officer upsert diffing.")
        #     logging.warning(f"Incoming employments: {incoming_emps}")
        #     logging.warning(f"Prefetched employments: {employments}")
        for i, fetched in enumerate(employments):
            ts = fetched.get("last_ts")
            # Freshness gate for employment
            if ts is not None and r["scraped_dt"] <= ts:
                dropped_e_expired += 1
                continue
            # Skip if we couldn't resolve unit
            unit_uid = fetched.get("unit_uid")
            if not unit_uid:
                dropped_e_unit += 1
                continue
            inc_index = fetched["i"]
            emp = incoming_emps[inc_index]

            emp_props = build_props_map(emp, EMPLOYMENT_FIELDS)
            rank = emp.get("highest_rank")
            if not (rank and emp_props):
                dropped_e_bad += 1
                continue
            emp_base = {
                "unit_uid": unit_uid,
                "highest_rank": rank,
                "props": emp_props,
            }
            if not fetched.get("matched"):
                # New employment record
                emps.append({
                    **emp_base,
                    "diff": None,
                })
                continue
            
            diff = detect_diff_dict(fetched.get("props") or {}, emp_props)
            if not diff:
                dropped_e_expired += 1
                continue

            emps.append({
                **emp_base,
                "diff": diff.to_json(),
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
        diff = detect_diff_dict(existing_map or {}, props)
        if not diff:
            continue

        to_apply.append({
            **base_apply,
            "diff": diff.to_json(),
        })

    if not to_apply:
        log.info("No officer records to upsert.")
        return
    

    # --- 3) Apply in one write query (cli wraps this in adb.write_transaction)
    log.info(f"Upserting {len(to_apply)} officer records...")
    if dropped_e_expired or dropped_e_bad or dropped_e_unit:
        log.info(
            "Dropped employment records -" \
            " expired: {expired}, bad: {bad}, missing unit: {mmissing}".format(
                expired=dropped_e_expired,
                bad=dropped_e_bad,
                mmissing=
                dropped_e_unit,
            ))
    merge_results = await tx.run(UPSERT_CYPHER, rows=to_apply)
    await merge_results.consume()
