
from typing import Optional
from index import LinearKVIndex
from storage import AppendOnlyLog

class KVEngine:
    """Coordinates the in-memory index and the append-only log.
    On startup, replays the log to rebuild the in-memory state.
    """
    def __init__(self, db_path: str = "data.db") -> None:
        self._index = LinearKVIndex()
        self._log = AppendOnlyLog(db_path)
        self._load_from_log()

    def _load_from_log(self) -> None:
        for key, value in self._log.replay() or []:
            self._index.set(key, value)

    def set(self, key: str, value: str) -> None:
        # Persist first so a crash after this call still records the write.
        self._log.append_set(key, value)
        # Then update the in-memory index.
        self._index.set(key, value)

    def get(self, key: str) -> Optional[str]:
        return self._index.get(key)
