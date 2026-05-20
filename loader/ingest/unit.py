from __future__ import annotations

import logging
from typing import Any
from neo4j import AsyncManagedTransaction
from .base import register
from .change import latest_change_timestamp_cypher, merge_change_cypher
from loader.utils.citations import detect_diff_dict, parse_scraped_at


PREFETCH_CYPHER = f"""
UNWIND $rows AS row

// Resolve the Agency first
OPTIONAL MATCH (a_uid:Agency {{uid: row.a_label}})
OPTIONAL MATCH (a_key:Agency {{name: row.a_label, hq_state: row.a_hq_state}})
WITH row, coalesce (a_uid, a_key) AS a

// Now match Unit via Agency
OPTIONAL MATCH (u:Unit {{name: row.name}})-[:ESTABLISHED_BY]-(a)

MATCH (s:Source {{uid: row.source_uid}})
{latest_change_timestamp_cypher("u", "s", change_alias="unit_change", legacy_alias="unit_cit")}
WITH row, a, u, last_ts
RETURN {{
  row_id: row.row_id,
  agency_uid: a.uid,
  exists: u IS NOT NULL,
  existing: CASE WHEN u IS NULL THEN NULL ELSE properties(u) END,
  last_ts: last_ts
}} as record
"""

UPSERT_CYPHER = f"""
UNWIND $rows AS row
MATCH (s:Source {{uid: row.source_uid}})
MATCH (a:Agency {{uid: row.agency_uid}})
MERGE (a)<-[:ESTABLISHED_BY]-(u:Unit {{name: row.name}})
ON CREATE SET u.uid = replace(randomUUID(), "-", "")
SET u += row.props

{merge_change_cypher("u", "s", "unit_change")}

// Location link
WITH u, row
OPTIONAL MATCH (st:StateNode {{abbreviation: row.hq_state}})

WITH u, row, st
WHERE st IS NOT NULL AND row.hq_city IS NOT NULL

OPTIONAL MATCH (c:CityNode {{name: row.hq_city}})-[]-(:CountyNode)-[]-(st)
WITH u, row, c
FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END |
  MERGE (u)-[:LOCATED_IN]->(c)
)

RETURN count(*) AS applied

"""

UNIT_FIELDS = (
    "hq_state",
    "hq_address",
    "hq_city",
    "hq_zip",
    "phone",
    "email",
    "website_url",
    "description",
)


def build_props_map(unit_data: dict[str, Any]) -> dict[str, Any]:
    # only include non-null keys so SET a += props won't overwrite with nulls
    props: dict[str, Any] = {}
    for k in UNIT_FIELDS:
        v = unit_data.get(k)
        if v is not None:
            props[k] = v
    return props


@register("unit")
async def upsert_unit_batch(
    tx: AsyncManagedTransaction,
    batch: list[dict[str, Any]],
    log: logging.LoggerAdapter
    ) -> None:

    # Build input rows (one per JSONL object)
    input_rows: list[dict[str, Any]] = []
    incoming_by_id: dict[int, dict[str, Any]] = {}

    for i, item in enumerate(batch):
        d = item.get("data") or {}
        name = d.get("name")
        if not name:
            continue

        a_label = item.get("agency")
        a_hq_state = item.get("a_hq_state", d.get("hq_state", None))
        if not (a_label and a_hq_state):
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
            "a_label": a_label,
            "a_hq_state": a_hq_state,
            "url": url,
            "scraped_dt": scraped_dt,
        }
        input_rows.append(row)
        incoming_by_id[i] = d

    if not input_rows:
        return

    # --- 1) Prefetch existing + last citation date for (unit, source, url)
    results = await tx.run(PREFETCH_CYPHER, rows=input_rows)
    prefetch = {}
    async for rec in results:
        record = rec["record"]
        row_id = int(record["row_id"])
        prefetch[row_id] = (
            record["agency_uid"],
            bool(record["exists"]),
            record["existing"],
            record["last_ts"]
        )
    await results.consume()

    # --- 2) Decide what to apply, and compute diffs only for fresh rows
    to_apply: list[dict[str, Any]] = []

    for r in input_rows:
        row_id = int(r["row_id"])
        incoming_data = incoming_by_id[row_id]

        agency_uid, exists, existing_map, last_ts = prefetch.get(
            row_id, (None, False, None, None))
        
        # Skip if we couldn't resolve agency
        if agency_uid is None:
            continue

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
            "agency_uid": agency_uid,
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
        log.info("No unit records to upsert.")
        return

    # --- 3) Apply in one write query (cli wraps this in adb.write_transaction)
    log.info(f"Upserting {len(to_apply)} unit records...")
    merge_results = await tx.run(UPSERT_CYPHER, rows=to_apply)
    await merge_results.consume()
