
import os
from typing import Iterator, Tuple, Optional

class AppendOnlyLog:
    """Append-only file-backed log with immediate persistence (fsync on each write).
    Log line format: 'SET <key> <value>\n'
    """
    def __init__(self, path: str = "data.db") -> None:
        self.path = path
        # Ensure the directory exists if a path includes dirs
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def replay(self) -> Iterator[Tuple[str, str]]:
        """Yield (key, value) pairs from the log by scanning it from start to end.
        Ignores malformed lines and non-SET commands.
        """
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8", newline="") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                # Expected: SET <key> <value>
                # We split into max 3 parts to preserve spaces in value if present.
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    continue
                cmd, key, value = parts[0], parts[1], parts[2]
                if cmd != "SET":
                    continue
                yield (key, value)

    def append_set(self, key: str, value: str) -> None:
        """Append a SET entry and fsync immediately to ensure durability."""
        # Very lightweight text format; value is written as-is after a space.
        # Newlines in value are not supported by design for this assignment.
        line = f"SET {key} {value}\n"
        with open(self.path, "a", encoding="utf-8", newline="") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
