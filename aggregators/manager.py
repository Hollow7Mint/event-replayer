"""Event Replayer — Stream manager layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class EventManager:
    """Stream manager for the Event Replayer application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._event_id = self._cfg.get("event_id", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def trim_stream(
        self, event_id: Any, cursor: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Stream record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "event_id": event_id,
            "cursor": cursor,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("trim_stream: created %s", saved["id"])
        return saved

    def get_stream(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Stream by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_stream: %s not found", record_id)
        return record

    def seek_stream(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Stream."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Stream {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def record_stream(self, record_id: str) -> bool:
        """Remove a Stream; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("record_stream: removed %s", record_id)
        return True

    def list_streams(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Stream records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_streams: %d results", len(results))
        return results

    def iter_streams(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Stream records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_streams(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
