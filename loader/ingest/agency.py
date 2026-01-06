from __future__ import annotations

import json
import logging
from typing import Any
from neomodel import adb
from .base import register
from loader.utils.citations import detect_diff_dict, parse_scraped_at



PREFETCH_CYPHER = """
UNWIND $rows AS row
OPTIONAL MATCH (a:Agency {name: row.name, hq_state: row.hq_state})
MATCH (s:Source {uid: row.source_uid})
OPTIONAL MATCH (a)-[c:UPDATED_BY]->(s)
WHERE c.user_uid IS NULL            // ignore user-created updates
WITH row, a, max(c.timestamp) AS last_ts
RETURN
  row.row_id AS row_id,
  a IS NOT NULL AS exists,
  CASE WHEN a IS NULL THEN NULL ELSE properties(a) END AS existing,
  last_ts AS last_ts
"""

UPSERT_CYPHER = """
UNWIND $rows AS row
MATCH (s:Source {uid: row.source_uid})
MERGE (a:Agency {name: row.name, hq_state: row.hq_state})
ON CREATE SET a.uid = replace(randomUUID(), "-", "")
SET a += row.props

MERGE (a)-[cit:UPDATED_BY {
  timestamp: datetime(row.scraped_dt),
  url: coalesce(row.url, \"\")
}]->(s)
SET
  cit.user_uid = NULL,
  cit.diff = row.diff

// Location link
WITH a, row
OPTIONAL MATCH (st:StateNode {abbreviation: row.hq_state})

WITH a, row, st
WHERE st IS NOT NULL AND row.hq_city IS NOT NULL

OPTIONAL MATCH (c:CityNode {name: row.hq_city})-[]-(:CountyNode)-[]-(st)
WITH a, row, c

FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END |
  MERGE (a)-[:LOCATED_IN]->(c)
)

RETURN count(*) AS applied
"""

AGENCY_FIELDS = (
    "website_url",
    "hq_address",
    "hq_city",
    "hq_state",
    "hq_zip",
    "phone",
    "email",
    "description",
    "jurisdiction",
)


def build_props_map(agency_data: dict[str, Any]) -> dict[str, Any]:
    # only include non-null keys so SET a += props won't overwrite with nulls
    props: dict[str, Any] = {}
    for k in AGENCY_FIELDS:
        v = agency_data.get(k)
        if v is not None:
            props[k] = v
    return props


@register("agency")
async def upsert_agency_batch(batch: list[dict[str, Any]]) -> None:
    # Build input rows (one per JSONL object)
    input_rows: list[dict[str, Any]] = []
    incoming_by_id: dict[int, dict[str, Any]] = {}

    for i, item in enumerate(batch):
        d = item.get("data") or {}
        name = d.get("name")
        if not name:
            continue

        source_uid = item.get("source_uid")
        url = item.get("url")
        scraped_at = item.get("scraped_at")
        if not (source_uid and url and scraped_at):
            continue

        scraped_dt = parse_scraped_at(scraped_at)

        # Normalize/flatten if you want: address fields etc.
        # (Your JSON shows address nested; map it here if needed.)
        if d.get("hq_address") is None and isinstance(d.get("address"), dict):
            addr = d["address"] or {}
            d = {
                **d,
                "hq_address": addr.get("street"),
                "hq_city": d.get("hq_city") or addr.get("city"),
                "hq_state": d.get("hq_state") or addr.get("state"),
                "hq_zip": d.get("hq_zip") or addr.get("postal_code"),
            }

        row = {
            "row_id": i,
            "name": name,
            "source_uid": source_uid,
            "url": url,
            "scraped_dt": scraped_dt,
        }
        input_rows.append(row)
        incoming_by_id[i] = d

    if not input_rows:
        return

    # --- 1) Prefetch existing + last citation date for (agency, source, url)
    results, _meta = await adb.cypher_query(
        PREFETCH_CYPHER, {"rows": input_rows})

    # results rows come back as lists/tuples in neomodel; map by row_id
    # row shape: [row_id, exists, existing_map, last_ts]
    prefetch: dict[int, tuple[bool, dict[str, Any] | None, Any]] = {}
    for row_id, exists, existing, last_ts in results:
        prefetch[int(row_id)] = (bool(exists), existing, last_ts)

    # --- 2) Decide what to apply, and compute diffs only for fresh rows
    to_apply: list[dict[str, Any]] = []

    for r in input_rows:
        row_id = int(r["row_id"])
        incoming_data = incoming_by_id[row_id]

        exists, existing_map, last_ts = prefetch.get(row_id, (False, None, None))

        # Freshness gate: skip if we already have a citation from
        # this source+url at or after this scraped time.
        if last_ts is not None and r["scraped_dt"] <= last_ts:
            continue

        props = build_props_map(incoming_data)
        hq_state = incoming_data.get("hq_state")
        hq_city = incoming_data.get("hq_city")

        base_apply = {
            **r,
            "props": props,
            "hq_state": hq_state,
            "hq_city": hq_city,
        }

        if not exists:
            # Creating: apply props + citation (diff optional; can omit or store None)
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
        logging.info("No agency records to upsert.")  
        return

    # --- 3) Apply in one write query (cli wraps this in adb.write_transaction)
    logging.info(f"Upserting {len(to_apply)} agency records...")
    await adb.cypher_query(UPSERT_CYPHER, {"rows": to_apply})
