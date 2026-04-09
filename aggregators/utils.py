"""Event Replayer — utils for event payloads."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class EventUtils:
    """Utils for Event Replayer event payloads."""

    _DATE_FIELDS = ("recorded_at", "replayed_at")

    @classmethod
    def loads(cls, raw: str) -> Dict[str, Any]:
        """Deserialise a JSON event payload."""
        data = json.loads(raw)
        return cls._coerce(data)

    @classmethod
    def dumps(cls, record: Dict[str, Any]) -> str:
        """Serialise a event record to JSON."""
        return json.dumps(record, default=str)

    @classmethod
    def _coerce(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Cast known date fields from ISO strings to datetime objects."""
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if k in cls._DATE_FIELDS and isinstance(v, str):
                try:
                    out[k] = datetime.fromisoformat(v)
                except ValueError:
                    out[k] = v
            else:
                out[k] = v
        return out


def parse_events(payload: str) -> List[Dict[str, Any]]:
    """Parse a JSON array of Event payloads."""
    raw = json.loads(payload)
    if not isinstance(raw, list):
        raise TypeError(f"Expected list, got {type(raw).__name__}")
    return [EventUtils._coerce(item) for item in raw]


def replay_event_to_str(
    record: Dict[str, Any], indent: Optional[int] = None
) -> str:
    """Convenience wrapper — serialise a Event to a JSON string."""
    if indent is None:
        return EventUtils.dumps(record)
    return json.dumps(record, indent=indent, default=str)
