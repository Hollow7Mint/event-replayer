"""Event Replayer — Cursor service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class EventService:
    """Business-logic service for Cursor operations in Event Replayer."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("EventService started")

    def export(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the export workflow for a new Cursor."""
        if "replayed_at" not in payload:
            raise ValueError("Missing required field: replayed_at")
        record = self._repo.insert(
            payload["replayed_at"], payload.get("cursor"),
            **{k: v for k, v in payload.items()
              if k not in ("replayed_at", "cursor")}
        )
        if self._events:
            self._events.emit("cursor.exportd", record)
        return record

    def snapshot(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Cursor and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Cursor {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("cursor.snapshotd", updated)
        return updated

    def seek(self, rec_id: str) -> None:
        """Remove a Cursor and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Cursor {rec_id!r} not found")
        if self._events:
            self._events.emit("cursor.seekd", {"id": rec_id})

    def search(
        self,
        replayed_at: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search cursors by *replayed_at* and/or *status*."""
        filters: Dict[str, Any] = {}
        if replayed_at is not None:
            filters["replayed_at"] = replayed_at
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search cursors: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Cursor counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
