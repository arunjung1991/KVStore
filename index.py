
from typing import List, Tuple, Optional

class LinearKVIndex:
    """A minimal in-memory index using a linear list.
    No built-in dict/map types are used.
    Stores (key, value) pairs in append order. Last write wins.
    """
    def __init__(self) -> None:
        self._pairs: List[Tuple[str, str]] = []

    def set(self, key: str, value: str) -> None:
        self._pairs.append((key, value))

    def get(self, key: str) -> Optional[str]:
        # Scan from the end to enforce last-write-wins.
        for k, v in reversed(self._pairs):
            if k == key:
                return v
        return None

    def clear(self) -> None:
        self._pairs = []

    def __len__(self) -> int:
        return len(self._pairs)
