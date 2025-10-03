# storage.py
import os
from typing import Iterator, Tuple

class AppendOnlyLog:
    """Append-only file-backed log with immediate persistence."""

    def __init__(self, path: str = "data.db") -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def replay(self) -> Iterator[Tuple[str, str]]:
        """Yield (key, value) pairs in write order by scanning the log."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8", newline="") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                parts = line.split(" ", 2)
                if len(parts) != 3:
                    continue
                cmd, key, value = parts
                if cmd != "SET":
                    continue
                yield (key, value)

    def append_set(self, key: str, value: str) -> None:
        """Append a SET record and fsync immediately."""
        line = f"SET {key} {value}\n"
        with open(self.path, "a", encoding="utf-8", newline="") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
