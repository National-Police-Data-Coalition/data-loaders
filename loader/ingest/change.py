def latest_change_timestamp_cypher(
    node_alias: str,
    source_alias: str,
    result_alias: str = "last_ts",
    change_alias: str = "change",
    carry_aliases: tuple[str, ...] | None = None,
) -> str:
    if carry_aliases is None:
        carry_aliases = tuple(dict.fromkeys(("row", node_alias, source_alias)))
    carry = ", ".join(carry_aliases)
    return f"""
OPTIONAL MATCH ({node_alias})<-[:CHANGE_TO]-({change_alias}:Change)-[:ATTRIBUTED_TO]->({source_alias})
WHERE {node_alias} IS NOT NULL
WITH {carry}, max({change_alias}.timestamp) AS {result_alias}
"""


def merge_change_cypher(
    node_alias: str,
    source_alias: str,
    change_alias: str,
    row_alias: str = "row",
    diff_expr: str = "row.diff",
    change_uid_expr: str | None = None,
) -> str:
    if change_uid_expr is None:
        change_uid_expr = f"{row_alias}.change_uid"

    return f"""
MERGE ({change_alias}:Change {{uid: {change_uid_expr}}})
SET
  {change_alias}.timestamp = datetime({row_alias}.scraped_dt),
  {change_alias}.url = coalesce({row_alias}.url, ""),
  {change_alias}.diff = {diff_expr}
MERGE ({node_alias})<-[:CHANGE_TO]-({change_alias})
MERGE ({change_alias})-[:ATTRIBUTED_TO]->({source_alias})
"""
