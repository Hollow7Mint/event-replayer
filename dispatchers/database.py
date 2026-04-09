"""Event Replayer — Replay database layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class EventDatabase:
    """Replay database for the Event Replayer application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._stream_id = self._cfg.get("stream_id", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def replay_replay(
        self, stream_id: Any, cursor: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Replay record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "stream_id": stream_id,
            "cursor": cursor,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("replay_replay: created %s", saved["id"])
        return saved

    def get_replay(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Replay by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_replay: %s not found", record_id)
        return record

    def seek_replay(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Replay."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Replay {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def record_replay(self, record_id: str) -> bool:
        """Remove a Replay; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("record_replay: removed %s", record_id)
        return True

    def list_replays(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Replay records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_replays: %d results", len(results))
        return results

    def iter_replays(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Replay records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_replays(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
