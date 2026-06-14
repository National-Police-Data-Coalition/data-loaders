from __future__ import annotations

from datetime import datetime
import hashlib
import json
def deterministic_node_uid(*parts: object) -> str:
    payload = ["node:v1", *("" if part is None else str(part) for part in parts)]
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def det_change_uid(
    target_uid: str,
    source_uid: str,
    scraped_dt: datetime | str,
    url: str | None,
) -> str:
    payload = [
        "change:v1",
        target_uid,
        source_uid,
        scraped_dt.isoformat() if isinstance(scraped_dt, datetime) else str(scraped_dt),
        url or "",
    ]
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
