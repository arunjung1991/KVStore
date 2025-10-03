# index.py
from typing import Optional, List, Tuple

FNV64_OFFSET = 1469598103934665603
FNV64_PRIME  = 1099511628211

def _fnv1a_64(s: str) -> int:
    """Return a stable 64-bit FNV-1a hash of a UTF-8 string."""
    h = FNV64_OFFSET
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * FNV64_PRIME) & 0xFFFFFFFFFFFFFFFF  # keep 64-bit
    return h

class LinearKVIndex:
    """Open-addressing hash table for key→value with linear probing.

    - No built-in dict/map; uses plain Python lists as arrays.
    - Last-write-wins: `set()` overwrites existing key's value.
    - Amortized O(1) for set/get under load factor ≤ 0.7.
    """

    def __init__(self, initial_capacity: int = 8) -> None:
        """
        Args:
            initial_capacity: Minimum table size; rounded up to a power of two.
        """
        cap = 1
        while cap < max(8, initial_capacity):
            cap <<= 1
        self._cap: int = cap
        self._size: int = 0
        self._keys:   List[Optional[str]] = [None] * self._cap
        self._values: List[Optional[str]] = [None] * self._cap

    # -------------------- public API --------------------

    def set(self, key: str, value: str) -> None:
        """Insert or overwrite a key's value (last-write-wins).

        Args:
            key: Non-empty string key (no whitespace recommended).
            value: Value string.
        """
        if (self._size + 1) * 10 >= self._cap * 7:  # load factor > 0.7
            self._resize(self._cap * 2)
        self._insert_or_update(key, value)

    def get(self, key: str) -> Optional[str]:
        """Return the latest value or None if missing.

        Args:
            key: Key to look up.
        Returns:
            The value string if present, else None.
        """
        idx = self._find_slot(key)
        if idx is None:
            return None
        # idx points to the slot containing key
        return self._values[idx]

    def clear(self) -> None:
        """Clear all entries (does not shrink capacity)."""
        self._keys  = [None] * self._cap
        self._values = [None] * self._cap
        self._size = 0

    def __len__(self) -> int:
        """Number of stored keys."""
        return self._size

    # -------------------- internal helpers --------------------

    def _insert_or_update(self, key: str, value: str) -> None:
        mask = self._cap - 1
        idx = _fnv1a_64(key) & mask
        first_free = None

        while True:
            k = self._keys[idx]
            if k is None:
                # empty slot
                if first_free is None:
                    first_free = idx
                self._keys[first_free] = key
                self._values[first_free] = value
                self._size += 1
                return
            if k == key:
                # overwrite existing
                self._values[idx] = value
                return
            idx = (idx + 1) & mask  # linear probe

    def _find_slot(self, key: str) -> Optional[int]:
        """Find the slot index containing `key`, or None if absent."""
        mask = self._cap - 1
        idx = _fnv1a_64(key) & mask
        probes = 0
        while probes < self._cap:
            k = self._keys[idx]
            if k is None:
                return None
            if k == key:
                return idx
            idx = (idx + 1) & mask
            probes += 1
        return None

    def _resize(self, new_cap: int) -> None:
        """Rehash all entries into a table of size `new_cap` (power of two)."""
        cap = 1
        while cap < new_cap:
            cap <<= 1
        old_keys, old_vals = self._keys, self._values
        self._cap = cap
        self._keys  = [None] * self._cap
        self._values = [None] * self._cap
        self._size = 0
        mask = self._cap - 1
        for k, v in zip(old_keys, old_vals):
            if k is not None:
                # insert without triggering further resizes
                idx = _fnv1a_64(k) & mask
                while self._keys[idx] is not None:
                    idx = (idx + 1) & mask
                self._keys[idx] = k
                self._values[idx] = v
                self._size += 1
