"""Event Replayer — utility helpers for stream operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def snapshot_stream(data: Dict[str, Any]) -> Dict[str, Any]:
    """Stream snapshot — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "payload" not in result:
        raise ValueError(f"Stream must include 'payload'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["payload"]).encode()).hexdigest()[:12]
    return result


def record_streams(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Stream records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("record_streams: %d items after filter", len(out))
    return out[:limit]


def replay_stream(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "replayed_at" in updated and not isinstance(updated["replayed_at"], (int, float)):
        try:
            updated["replayed_at"] = float(updated["replayed_at"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_stream(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Stream invariants."""
    required = ["payload", "replayed_at", "stream_id"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_stream: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def trim_stream_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk trim."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
