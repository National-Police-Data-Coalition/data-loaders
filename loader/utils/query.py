
from typing import Any, Optional, List, Dict, Tuple
from neomodel import (
    db, StructuredNode
)

# A tiny, read-only, chainable relation view.
class RelQuery:
    """
    A tiny, read-only, chainable relation view.
    Usage:
        agency.units.filter(
           "u.name CONTAINS $q", q="SWAT").order_by("u.name").limit(5).all()
        agency.units.first()
        agency.units.exists()
        agency.units.one()  # raises if != 1
    """
    def __init__(
            self, owner: StructuredNode, base_cypher: str,
            return_alias: str, inflate_cls):
        self._owner = owner
        self._base = base_cypher.strip().rstrip(";")
        self._ret = return_alias
        self._inflate = inflate_cls
        self._where: List[str] = []
        self._params: Dict[str, Any] = {"owner_uid": owner.uid}
        self._order: Optional[str] = None
        self._limit: Optional[int] = None

    # ---- builders ----
    def filter(self, clause: str, /, **params):
        if clause:
            self._where.append(f"({clause})")
        if params:
            self._params.update(params)
        return self

    def params(self, **params):
        self._params.update(params)
        return self

    def order_by(self, clause: str):
        self._order = clause
        return self

    def limit(self, n: int):
        self._limit = n
        return self

    # ---- executors ----
    def _compose(self, count_only: bool = False) -> Tuple[str, Dict[str, Any]]:
        parts = [self._base]
        if self._where:
            parts.append("WHERE " + " AND ".join(self._where))
        if count_only:
            parts.append(f"RETURN count({self._ret}) AS c")
        else:
            parts.append(f"RETURN {self._ret} AS node")
            if self._order:
                parts.append(f"ORDER BY {self._order}")
            if self._limit is not None:
                parts.append(f"LIMIT {self._limit}")
        return " ".join(parts) + ";", self._params

    def all(self):
        cy, params = self._compose()
        rows, _ = db.cypher_query(cy, params, resolve_objects=True)
        # if resolve_objects=True is wired, rows come back as objects already
        if rows and not isinstance(rows[0][0], StructuredNode):
            # fallback inflate (in case resolve_objects isn't used)
            return [self._inflate.inflate(row[0]) for row in rows]
        return [row[0] for row in rows]

    def first(self):
        if self._limit is None:
            self.limit(1)
        res = self.all()
        return res[0] if res else None

    def one(self):
        # exactly one or raise
        res = self.limit(2).all()
        if len(res) != 1:
            raise ValueError(f"Expected exactly one result, got {len(res)}")
        return res[0]

    def one_or_none(self):
        # exactly one or none
        res = self.limit(2).all()
        if len(res) > 1:
            raise ValueError(f"Expected at most one result, got {len(res)}")
        return res[0] if res else None

    def exists(self) -> bool:
        cy, params = self._compose(count_only=True)
        rows, _ = db.cypher_query(cy, params)
        return bool(rows and rows[0][0] > 0)

    def count(self) -> int:
        """
        Count the number of nodes matching the query.
        Returns:
            int: The count of nodes.
        """
        cy, params = self._compose(count_only=True)
        rows, _ = db.cypher_query(cy, params)
        return rows[0][0] if rows else 0