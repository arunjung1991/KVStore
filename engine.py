# engine.py
from typing import Optional
from index import BPlusTreeIndex  # <-- now importing the B+ tree
from storage import AppendOnlyLog

class KVEngine:
    """Coordinates the in-memory index and the append-only log."""

    def __init__(self, db_path: str = "data.db") -> None:
        """Initialize the engine and rebuild state by replaying the log."""
        self._index = BPlusTreeIndex()
        self._log = AppendOnlyLog(db_path)
        self._load_from_log()

    def _load_from_log(self) -> None:
        """Rebuild in-memory state by replaying SET records from disk.

        Last-write-wins is achieved by applying entries in append order so
        later writes overwrite earlier ones for the same key.
        """
        for key, value in self._log.replay() or []:
            self._index.set(key, value)

    def set(self, key: str, value: str) -> None:
        """Persist a key-value pair and update the in-memory state."""
        self._log.append_set(key, value)
        self._index.set(key, value)

    def get(self, key: str) -> Optional[str]:
        """Return the latest value for a key, or None if missing."""
        return self._index.get(key)
