"""Event Replayer — Snapshot service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class EventManager:
    """Business-logic service for Snapshot operations in Event Replayer."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("EventManager started")

    def replay(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the replay workflow for a new Snapshot."""
        if "cursor" not in payload:
            raise ValueError("Missing required field: cursor")
        record = self._repo.insert(
            payload["cursor"], payload.get("stream_id"),
            **{k: v for k, v in payload.items()
              if k not in ("cursor", "stream_id")}
        )
        if self._events:
            self._events.emit("snapshot.replayd", record)
        return record

    def trim(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Snapshot and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Snapshot {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("snapshot.trimd", updated)
        return updated

    def export(self, rec_id: str) -> None:
        """Remove a Snapshot and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Snapshot {rec_id!r} not found")
        if self._events:
            self._events.emit("snapshot.exportd", {"id": rec_id})

    def search(
        self,
        cursor: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search snapshots by *cursor* and/or *status*."""
        filters: Dict[str, Any] = {}
        if cursor is not None:
            filters["cursor"] = cursor
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search snapshots: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Snapshot counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
