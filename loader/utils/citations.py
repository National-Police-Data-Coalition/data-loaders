from datetime import datetime, timezone
from deepdiff import DeepDiff


IGNORE_FIELDS = {"uid", "element_id_property"}

def detect_diff_dict(existing: dict, incoming: dict) -> DeepDiff:
    incoming_mapped = {k: v for k, v in incoming.items() if v is not None}
    existing_mapped = {
        k: v for k, v in existing.items()
        if k not in IGNORE_FIELDS and v is not None
    }
    return DeepDiff(existing_mapped, incoming_mapped, ignore_order=True)

def parse_scraped_at(s: str) -> datetime:
    # incoming: "YYYY-MM-DD HH:MM:SS"
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)