from __future__ import annotations

import json
import logging
from typing import Any
from neo4j import AsyncManagedTransaction
from .base import register
from loader.utils.citations import detect_diff_dict, parse_scraped_at
from datetime import date, datetime


PREFETCH_CYPHER = """
UNWIND $rows AS row

MATCH (s:Source {uid: row.source_uid})

// Resolve the Complaint
OPTIONAL MATCH (c:Complaint {complaint_key: row.complaint_key})
OPTIONAL MATCH (c)-[:OCCURRED_IN]->(l:Location)

// Complaint freshness
CALL (c, s) {
  OPTIONAL MATCH (c)-[cit:UPDATED_BY]->(s)
  // WHERE cit.user_uid IS NULL
  RETURN max(cit.timestamp) AS last_ts
}

RETURN {
  row_id: row.row_id,
  exists: c IS NOT NULL,
  existing: CASE WHEN c IS NULL THEN NULL ELSE properties(c) END,
  existing_loc: CASE WHEN c IS NULL THEN NULL ELSE properties(l) END,
  last_ts: last_ts
} AS record
"""

UPSERT_CYPHER = """
UNWIND $rows AS row
MATCH (s:Source {uid: row.source_uid})
// Merge Complaint node
MERGE (c:Complaint {complaint_key: row.complaint_key})-[rel:HAS_SOURCE]->(s)
ON CREATE SET c.uid = replace(randomUUID(), "-", "")
SET 
  c += row.props,
  rel += row.source_rel_props


MERGE (c)-[cit:UPDATED_BY {
  timestamp: datetime(row.scraped_dt),
  url: coalesce(row.url, \"\")
}]->(s)
SET
  cit.user_uid = NULL,
  cit.diff = row.diff

// Location node
WITH s, c, row
CALL (s, c, row) {
  MERGE (c)-[:OCCURRED_IN]->(l:Location)
  SET l += row.loc_props

  MERGE (l)-[l_cit:UPDATED_BY {
    timestamp: datetime(row.scraped_dt),
    url: coalesce(row.url, \"\")
  }]->(s)
  SET
    l_cit.user_uid = NULL,
    l_cit.diff = row.loc_diff

  WITH l
  OPTIONAL MATCH (st:StateNode {abbreviation: l.state})

  WITH l, st
  WHERE st IS NOT NULL AND l.city IS NOT NULL
  OPTIONAL MATCH (city:CityNode {name: l.city})-[]-(:CountyNode)-[]-(st)
  WITH l, city
  FOREACH (_ IN CASE WHEN city IS NULL THEN [] ELSE [1] END |
    MERGE (l)-[:LOCATED_IN]->(city)
  )
}

RETURN count(*) AS applied
"""

COMPLAINT_FIELDS = (
    "record_id",
    "category",
    "incident_date",
    "received_date",
    "closed_date",
    "updated_date",
    "reason_for_contact",
    "outcome_of_contact",
    "civilian_witnesses",
    "attachments",
    "civilian_review_board_uid",
    "police_witnesses",
    "allegations",
    "investigations",
    "penalties",
)

SOURCE_DETAILS_FIELDS = (
    "record_type",
    "date_published",
    "court",
    "judge",
    "docket_number",
    "case_event_date",
    "publication_name",
    "publication_url",
    "author",
    "author_url",
    "author_email",
    "reporting_agency",
    "reporting_agency_url",
    "reporting_agency_email",
)

LOCATION_FIELDS = (
    "location_type",
    "location_description",
    "address",
    "city",
    "state",
    "zip",
    "administrative_area",
    "administrative_area_type",
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


def to_neo4j_date(value: Any) -> date | None:
    """
    Convert an incoming value into a Python date object suitable for Neo4j.

    Supported inputs:
    - datetime.date -> returned as-is
    - datetime.datetime -> converted with .date()
    - strings like:
        - YYYY-MM-DD
        - YYYY-MM-DD HH:MM:SS
        - YYYY-MM-DDTHH:MM:SS
        - ISO-like timestamps ending in Z
    - None or blank strings -> None
    """
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # First try the plain date format directly
        try:
            return date.fromisoformat(s)
        except ValueError:
            pass

        # Normalize common ISO datetime variants
        normalized = s.replace("Z", "+00:00")

        # Try parsing as a datetime, then take the date part
        try:
            return datetime.fromisoformat(normalized).date()
        except ValueError:
            pass

        # Fallback for common "YYYY-MM-DD HH:MM:SS" without timezone issues
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue

    raise ValueError(f"Could not parse date value: {value!r}")


def build_props_map(data: dict[str, Any], fields) -> dict[str, Any]:
    # only include non-null keys so SET a += props won't overwrite with nulls
    props: dict[str, Any] = {}
    for k in fields:
        # For dates, use Neo4j's date/datetime
        if k.endswith("_date"):
            v = data.get(k)
            if v is not None:
                props[k] = to_neo4j_date(v)
            continue
        v = data.get(k)
        if v is not None:
            props[k] = v
    return props


@register("complaint")
async def upsert_complaint_batch(
    tx: AsyncManagedTransaction,
    batch: list[dict[str, Any]],
    log: logging.LoggerAdapter,
    ) -> None:
    # Build input rows (one per JSONL object)
    log.info("Building complaint upsert batch...")
    input_rows: list[dict[str, Any]] = []
    incoming_by_id: dict[int, dict[str, Any]] = {}

    output = 0
    dropped_expired = dropped_bad = 0

    for i, item in enumerate(batch):
        fields = item.get("data") or {}

        # Complaint Identifiers
        record_id = fields.get("record_id")
        if not record_id:
            dropped_bad += 1
            continue

        # Source
        source_uid = item.get("source_uid")
        complaint_key = f"{source_uid}:{record_id}"
        url = item.get("url")
        scraped_at = item.get("scraped_at")
        if not (source_uid and url and scraped_at):
            dropped_bad += 1
            continue
        scraped_dt = parse_scraped_at(scraped_at)
        row = {
            "row_id": i,
            "complaint_key": complaint_key,
            "source_uid": source_uid,
            "url": url,
            "scraped_dt": scraped_dt,
        }

        input_rows.append(row)
        incoming_by_id[i] = fields

        if i < output:
            log.info(f"Incoming row {i}:")
            log.info(row["complaint_key"])
    if not input_rows:
        return

    # --- 1) Prefetch existing + last citation date for (officers and employments)
    results = await tx.run(PREFETCH_CYPHER, rows=input_rows)
    prefetch = {}
    async for rec in results:
        record = rec["record"]
        row_id = int(record["row_id"])
        prefetch[row_id] = (
            bool(record["exists"]),
            record["existing"],
            record["existing_loc"],
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

        exists, existing_map, existing_loc_map, last_ts = prefetch.get(
            row_id, (False, None, None, None))

        # Freshness gate: skip if we already have a citation from
        # this source+url at or after this scraped time.
        if last_ts is not None and r["scraped_dt"] <= last_ts:
            dropped_expired += 1
            continue

        props = build_props_map(incoming_data, COMPLAINT_FIELDS)
        loc_props = build_props_map(incoming_data.get("location", {}), LOCATION_FIELDS)
        source_rel_props = build_props_map(incoming_data.get("source_details", {}), SOURCE_DETAILS_FIELDS)

        base_apply = {
            **r,
            "props": props,
            "loc_props": loc_props,
            "source_rel_props": source_rel_props,
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
        loc_diff = detect_diff_dict(existing_loc_map or {}, incoming_data.get("location", {}))
        if not diff and not loc_diff:
            dropped_expired += 1
            continue

        to_apply.append({
            **base_apply,
            "diff": diff.to_json() if diff else None,
            "loc_diff": loc_diff.to_json() if loc_diff else None,
        })

    if not to_apply:
        log.info("No complaint records to upsert.")
        return
    

    # --- 3) Apply in one write query (cli wraps this in adb.write_transaction)
    log.info(f"Upserting {len(to_apply)} complaint records...")
    if dropped_expired or dropped_bad:
        log.info(
            "Dropped records -" \
            " expired: {}, bad: {}".format(
                dropped_expired,
                dropped_bad
            ))
    merge_results = await tx.run(UPSERT_CYPHER, rows=to_apply)
    await merge_results.consume()
