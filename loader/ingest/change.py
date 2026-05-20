def latest_change_timestamp_cypher(
    node_alias: str,
    source_alias: str,
    result_alias: str = "last_ts",
    change_alias: str = "change",
    legacy_alias: str = "legacy_cit",
) -> str:
    return f"""
CALL ({node_alias}, {source_alias}) {{
  OPTIONAL MATCH ({node_alias})<-[:CHANGE_TO]-({change_alias}:Change)-[:ATTRIBUTED_TO]->({source_alias})
  WITH {node_alias}, {source_alias}, max({change_alias}.timestamp) AS change_ts
  OPTIONAL MATCH ({node_alias})-[{legacy_alias}:UPDATED_BY]->({source_alias})
  WITH change_ts, max({legacy_alias}.timestamp) AS legacy_ts
  RETURN CASE
    WHEN change_ts IS NULL THEN legacy_ts
    WHEN legacy_ts IS NULL THEN change_ts
    WHEN change_ts > legacy_ts THEN change_ts
    ELSE legacy_ts
  END AS {result_alias}
}}
"""


def merge_change_cypher(
    node_alias: str,
    source_alias: str,
    change_alias: str,
    row_alias: str = "row",
    diff_expr: str = "row.diff",
) -> str:
    return f"""
WITH *,
  elementId({node_alias}) + ":" +
  {row_alias}.source_uid + ":" +
  toString(datetime({row_alias}.scraped_dt)) + ":" +
  coalesce({row_alias}.url, "") AS {change_alias}_uid
MERGE ({change_alias}:Change {{uid: {change_alias}_uid}})
SET
  {change_alias}.timestamp = datetime({row_alias}.scraped_dt),
  {change_alias}.url = coalesce({row_alias}.url, ""),
  {change_alias}.diff = {diff_expr}
MERGE ({node_alias})<-[:CHANGE_TO]-({change_alias})
MERGE ({change_alias})-[:ATTRIBUTED_TO]->({source_alias})
"""
