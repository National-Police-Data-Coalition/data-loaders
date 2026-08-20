from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone


def canonical_change_timestamp(scraped_dt: datetime | str) -> str:
    if isinstance(scraped_dt, datetime):
        dt = scraped_dt
    else:
        value = scraped_dt.strip()
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def canonical_change_url(url: str | None) -> str:
    return url or ""


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
        canonical_change_timestamp(scraped_dt),
        canonical_change_url(url),
    ]
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
