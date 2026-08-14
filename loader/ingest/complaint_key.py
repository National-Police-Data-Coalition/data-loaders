from __future__ import annotations

import hashlib


COMPLAINT_KEY_SEPARATOR = "\x1f"


def build_complaint_key(source_uid: str, record_id: str) -> str:
    payload = COMPLAINT_KEY_SEPARATOR.join(("complaint", source_uid, record_id))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
