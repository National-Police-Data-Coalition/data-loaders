from __future__ import annotations

import json
import logging
from typing import Any
from neo4j import AsyncManagedTransaction
from .base import register
from loader.utils.citations import detect_diff_dict, parse_scraped_at


PREFETCH_CYPHER = """
UNWIND $rows AS row

// Resolve the Complaint
MATCH (s:Source {uid: row.source_uid})
MATCH (complaint:Complaint { complaint_key: row.complaint_key })

// Resolve the Allegation
OPTIONAL MATCH (complaint)-[:ALLEGED]->(a:Allegation {record_id: row.record_id })
OPTIONAL MATCH (a)<-[:REPORTED_BY]-(civ:Civilian)

// Resolve the Officer
OPTIONAL MATCH (:StateID {
  state: row.officer_state,
  id_name: row.officer_id_name,
  value: row.officer_id_value
})<-[:HAS_STATE_ID]-(officer:Officer)

// Allegation freshness
CALL (a, s) {
  OPTIONAL MATCH (a)-[cit:UPDATED_BY]->(s)
  // WHERE cit.user_uid IS NULL
  RETURN max(cit.timestamp) AS last_ts
}

RETURN {
  row_id: row.row_id,
  complaint: CASE WHEN complaint IS NULL THEN NULL ELSE complaint.uid END,
  officer: CASE WHEN officer IS NULL THEN NULL ELSE officer.uid END,
  exists: a IS NOT NULL,
  existing: CASE WHEN a IS NULL THEN NULL ELSE properties(a) END,
  complainant: CASE WHEN civ IS NULL THEN NULL ELSE properties(civ) END,
  last_ts: last_ts
} AS record
"""

UPSERT_CYPHER = """
UNWIND $rows AS row
MATCH (s:Source {uid: row.source_uid})
MATCH (complaint:Complaint { uid: row.complaint_uid })

// Merge Allegation node
MERGE (complaint)<-[:ALLEGED]-(a:Allegation { record_id: row.record_id })
ON CREATE SET a.uid = replace(randomUUID(), "-", "")
SET 
  a += row.props

MERGE (a)-[cit:UPDATED_BY {
  timestamp: datetime(row.scraped_dt),
  url: coalesce(row.url, \"\")
}]->(s)
SET
  cit.user_uid = NULL,
  cit.diff = row.diff

// Connect Officer
WITH a, row, s
OPTIONAL MATCH (o:Officer { uid: row.officer_uid })
FOREACH (_ IN CASE WHEN o IS NULL THEN [] ELSE [1] END |
  MERGE (a)<-[:ACCUSED_OF]-(o)
)

// Connect Civilian Complainant
WITH a, row, s
MERGE (a)-[:REPORTED_BY]->(civ:Civilian)
SET
  civ += row.civ_props

WITH civ, row, s
MERGE (civ)-[civ_cit:UPDATED_BY {
  timestamp: datetime(row.scraped_dt),
  url: coalesce(row.url, \"\")
}]->(s)
SET
  civ_cit.user_uid = NULL,
  civ_cit.diff = row.diff

RETURN count(*) AS applied
"""

ALLEGATION_FIELDS = (
    "allegation",
    "type",
    "subtype",
    "recommended_finding",
    "recommended_outcome",
    "finding",
    "outcome",
)

CIVILIAN_FIELDS = (
    "age",
    "age_range",
    "ethnicity",
    "gender",
)

def build_props_map(data: dict[str, Any], fields) -> dict[str, Any]:
    # only include non-null keys so SET a += props won't overwrite with nulls
    props: dict[str, Any] = {}
    for k in fields:
        v = data.get(k)
        if v is not None:
            props[k] = v
    return props


@register("allegation")
async def upsert_allegation_batch(
    tx: AsyncManagedTransaction,
    batch: list[dict[str, Any]],
    log: logging.LoggerAdapter,
    ) -> None:
    # Build input rows (one per JSONL object)
    log.info("Building allegation upsert batch...")
    input_rows: list[dict[str, Any]] = []
    incoming_by_id: dict[int, dict[str, Any]] = {}
    civilian_by_id: dict[int, dict[str, Any]] = {}

    output = 0
    dropped_expired = dropped_bad = 0

    for i, item in enumerate(batch):
        fields = item.get("data") or {}
        officer_sid = item.get("officer_state_id")
        complainant = item.get("complainant") or {}


        # Complaint Identifiers
        record_id = fields.get("record_id")
        if not record_id:
            dropped_bad += 1
            continue

        # Source
        source_uid = item.get("source_uid")
        url = item.get("url")
        scraped_at = item.get("scraped_at")
        if not (source_uid and url and scraped_at):
            dropped_bad += 1
            continue

        # Complaint
        complaint_id = item.get("complaint_id")
        if not complaint_id:
            dropped_bad += 1
            continue

        # Resolve complaint_key for prefetch
        complaint_key = f"{source_uid}:{complaint_id}"

        officer_id_name = officer_sid.get("id_name")
        officer_id_value = officer_sid.get("value")
        officer_state = officer_sid.get("state")

        scraped_dt = parse_scraped_at(scraped_at)
        row = {
            "row_id": i,
            "record_id": record_id,
            "source_uid": source_uid,
            "complaint_key": complaint_key,
            "officer_id_name": officer_id_name,
            "officer_id_value": officer_id_value,
            "officer_state": officer_state,
            "url": url,
            "scraped_dt": scraped_dt,
        }

        input_rows.append(row)
        incoming_by_id[i] = fields
        civilian_by_id[i] = complainant

        if i < output:
            log.info(f"Incoming row {i}:")
            log.info(row)
    if not input_rows:
        return

    # --- 1) Prefetch existing + last citation date for (officers and employments)
    results = await tx.run(PREFETCH_CYPHER, rows=input_rows)
    prefetch = {}
    async for rec in results:
        record = rec["record"]
        row_id = int(record["row_id"])
        prefetch[row_id] = (
            record["complaint"],
            record["officer"],
            bool(record["exists"]),
            record["existing"],
            record["complainant"],
            record["last_ts"]
        )
        if row_id < output:
            log.info(f"Prefetched record for row {row_id}:")
            log.info(record)
    await results.consume()
    log.info("Prefetch complete.")
    log.info(f"Prefetched {len(prefetch)} complaint records.")

    # --- 2) Decide what to apply, and compute diffs only for fresh rows
    to_apply: list[dict[str, Any]] = []

    for r in input_rows:
        row_id = int(r["row_id"])
        incoming_data = incoming_by_id[row_id]
        incoming_civ = civilian_by_id[row_id]

        complaint_uid, officer_uid, exists, existing_map, complainant_map, last_ts = prefetch.get(
            row_id, (None, None, False, None, None, None))
        
        if complaint_uid is None:
            dropped_bad += 1
            continue

        # Freshness gate: skip if we already have a citation from
        # this source+url at or after this scraped time.
        if last_ts is not None and r["scraped_dt"] <= last_ts:
            dropped_expired += 1
            continue

        props = build_props_map(incoming_data, ALLEGATION_FIELDS)
        civ_props = build_props_map(incoming_civ, CIVILIAN_FIELDS)

        base_apply = {
            **r,
            "complaint_uid": complaint_uid,
            "officer_uid": officer_uid,
            "props": props,
            "civ_props": civ_props,
        }

        if not exists:
            # Creating: apply props + citation
            to_apply.append({
                **base_apply,
                "diff": None,
                "civ_diff": None,
            })
            continue

        # Existing: only write if there are meaningful diffs
        diff = detect_diff_dict(existing_map or {}, incoming_data)
        civ_diff = detect_diff_dict(complainant_map or {}, incoming_civ)
        if not diff and not civ_diff:
            dropped_expired += 1
            continue

        to_apply.append({
            **base_apply,
            "diff": diff.to_json() if diff else None,
            "civ_diff": civ_diff.to_json() if civ_diff else None,
        })

    if not to_apply:
        log.info("No allegation records to upsert.")
        return
    

    # --- 3) Apply in one write query (cli wraps this in adb.write_transaction)
    log.info(f"Upserting {len(to_apply)} allegation records...")
    if dropped_expired or dropped_bad:
        log.info(
            "Dropped records -" \
            " expired: {}, bad: {}".format(
                dropped_expired,
                dropped_bad
            ))
    merge_results = await tx.run(UPSERT_CYPHER, rows=to_apply)
    await merge_results.consume()
