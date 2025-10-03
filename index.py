# # index.py
# from typing import Optional, List, Tuple

# FNV64_OFFSET = 1469598103934665603
# FNV64_PRIME  = 1099511628211

# def _fnv1a_64(s: str) -> int:
#     """Return a stable 64-bit FNV-1a hash of a UTF-8 string."""
#     h = FNV64_OFFSET
#     for b in s.encode("utf-8"):
#         h ^= b
#         h = (h * FNV64_PRIME) & 0xFFFFFFFFFFFFFFFF  # keep 64-bit
#     return h

# class LinearKVIndex:
#     """Open-addressing hash table for key→value with linear probing.

#     - No built-in dict/map; uses plain Python lists as arrays.
#     - Last-write-wins: `set()` overwrites existing key's value.
#     - Amortized O(1) for set/get under load factor ≤ 0.7.
#     """

#     def __init__(self, initial_capacity: int = 8) -> None:
#         """
#         Args:
#             initial_capacity: Minimum table size; rounded up to a power of two.
#         """
#         cap = 1
#         while cap < max(8, initial_capacity):
#             cap <<= 1
#         self._cap: int = cap
#         self._size: int = 0
#         self._keys:   List[Optional[str]] = [None] * self._cap
#         self._values: List[Optional[str]] = [None] * self._cap

#     # -------------------- public API --------------------

#     def set(self, key: str, value: str) -> None:
#         """Insert or overwrite a key's value (last-write-wins).

#         Args:
#             key: Non-empty string key (no whitespace recommended).
#             value: Value string.
#         """
#         if (self._size + 1) * 10 >= self._cap * 7:  # load factor > 0.7
#             self._resize(self._cap * 2)
#         self._insert_or_update(key, value)

#     def get(self, key: str) -> Optional[str]:
#         """Return the latest value or None if missing.

#         Args:
#             key: Key to look up.
#         Returns:
#             The value string if present, else None.
#         """
#         idx = self._find_slot(key)
#         if idx is None:
#             return None
#         # idx points to the slot containing key
#         return self._values[idx]

#     def clear(self) -> None:
#         """Clear all entries (does not shrink capacity)."""
#         self._keys  = [None] * self._cap
#         self._values = [None] * self._cap
#         self._size = 0

#     def __len__(self) -> int:
#         """Number of stored keys."""
#         return self._size

#     # -------------------- internal helpers --------------------

#     def _insert_or_update(self, key: str, value: str) -> None:
#         mask = self._cap - 1
#         idx = _fnv1a_64(key) & mask
#         first_free = None

#         while True:
#             k = self._keys[idx]
#             if k is None:
#                 # empty slot
#                 if first_free is None:
#                     first_free = idx
#                 self._keys[first_free] = key
#                 self._values[first_free] = value
#                 self._size += 1
#                 return
#             if k == key:
#                 # overwrite existing
#                 self._values[idx] = value
#                 return
#             idx = (idx + 1) & mask  # linear probe

#     def _find_slot(self, key: str) -> Optional[int]:
#         """Find the slot index containing `key`, or None if absent."""
#         mask = self._cap - 1
#         idx = _fnv1a_64(key) & mask
#         probes = 0
#         while probes < self._cap:
#             k = self._keys[idx]
#             if k is None:
#                 return None
#             if k == key:
#                 return idx
#             idx = (idx + 1) & mask
#             probes += 1
#         return None

#     def _resize(self, new_cap: int) -> None:
#         """Rehash all entries into a table of size `new_cap` (power of two)."""
#         cap = 1
#         while cap < new_cap:
#             cap <<= 1
#         old_keys, old_vals = self._keys, self._values
#         self._cap = cap
#         self._keys  = [None] * self._cap
#         self._values = [None] * self._cap
#         self._size = 0
#         mask = self._cap - 1
#         for k, v in zip(old_keys, old_vals):
#             if k is not None:
#                 # insert without triggering further resizes
#                 idx = _fnv1a_64(k) & mask
#                 while self._keys[idx] is not None:
#                     idx = (idx + 1) & mask
#                 self._keys[idx] = k
#                 self._values[idx] = v
#                 self._size += 1

# index.py
"""
Custom open-addressing hash table (linear probing) for key→value storage.
- No built-in dict/map (uses plain Python lists as arrays)
- Power-of-two capacity so modulo becomes bitmasking
- Last-write-wins on repeated SETs
"""

from typing import Optional, List, Tuple

# -------------------- hashing (FNV-1a 64-bit) --------------------
FNV64_OFFSET = 1469598103934665603
FNV64_PRIME  = 1099511628211

def _fnv1a_64(s: str) -> int:
    """Stable 64-bit FNV-1a hash of a UTF-8 string.

    Args:
        s: Input string.

    Returns:
        64-bit unsigned integer hash.
    """
    h = FNV64_OFFSET
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * FNV64_PRIME) & 0xFFFFFFFFFFFFFFFF  # keep to 64 bits
    return h

# -------------------- tuning constants (no magic numbers) --------------------
MIN_CAPACITY: int = 8          # Smallest table size (rounded to power of two)
GROWTH_FACTOR: int = 2         # Resize multiplier when growing
# Load factor threshold: (size + 1) / cap >= 0.7 → trigger resize
LOAD_FACTOR_NUM: int = 7       # 0.7 == 7 / 10 (use integer math, no floats)
LOAD_FACTOR_DEN: int = 10
MAX_PROBE_MULTIPLIER: int = 1  # Linear probe cap == capacity * this multiplier

class LinearKVIndex:
    """Open-addressing hash table with linear probing (no built-in dict/map).

    Complexity:
        - set(): Amortized O(1) average until load factor ~ 0.7
        - get(): Amortized O(1) average
    Invariants:
        - Capacity is a power of two → we use bitmasking for wraparound
        - Last write wins: set() overwrites an existing key’s value
    """

    def __init__(self, initial_capacity: int = MIN_CAPACITY) -> None:
        """Create a table with at least `initial_capacity`.

        Args:
            initial_capacity: Requested starting capacity; rounded up to a power of two.
        """
        cap = 1
        target = max(MIN_CAPACITY, initial_capacity)
        while cap < target:
            cap <<= 1
        self._cap: int = cap
        self._size: int = 0
        self._keys:   List[Optional[str]] = [None] * self._cap
        self._values: List[Optional[str]] = [None] * self._cap

    # -------------------- public API --------------------

    def set(self, key: str, value: str) -> None:
        """Insert or overwrite key’s value (last-write-wins).

        Args:
            key: Non-empty key string.
            value: Value string (overwrites any previous value for this key).
        """
        # Resize proactively if adding one more would exceed the load threshold.
        if (self._size + 1) * LOAD_FACTOR_DEN >= self._cap * LOAD_FACTOR_NUM:
            self._resize(self._cap * GROWTH_FACTOR)
        self._insert_or_update(key, value)

    def get(self, key: str) -> Optional[str]:
        """Return latest value for key or None if missing.

        Args:
            key: Key to look up.

        Returns:
            The value string if present; otherwise None.
        """
        idx = self._find_slot(key)
        return None if idx is None else self._values[idx]

    def clear(self) -> None:
        """Remove all entries (capacity unchanged)."""
        self._keys  = [None] * self._cap
        self._values = [None] * self._cap
        self._size = 0

    def __len__(self) -> int:
        """Number of stored keys."""
        return self._size

    # -------------------- internal helpers --------------------

    def _insert_or_update(self, key: str, value: str) -> None:
        """Insert new key or overwrite existing value using linear probing."""
        mask = self._cap - 1  # power-of-two capacity
        idx = _fnv1a_64(key) & mask
        while True:
            k = self._keys[idx]
            if k is None:
                # Empty slot → insert
                self._keys[idx] = key
                self._values[idx] = value
                self._size += 1
                return
            if k == key:
                # Found existing → overwrite
                self._values[idx] = value
                return
            idx = (idx + 1) & mask  # next probe

    def _find_slot(self, key: str) -> Optional[int]:
        """Find index of `key` in table, or None if absent.

        Uses linear probing up to `capacity * MAX_PROBE_MULTIPLIER` steps.
        """
        mask = self._cap - 1
        idx = _fnv1a_64(key) & mask
        probes = 0
        limit = self._cap * MAX_PROBE_MULTIPLIER
        while probes < limit:
            k = self._keys[idx]
            if k is None:
                return None
            if k == key:
                return idx
            idx = (idx + 1) & mask
            probes += 1
        # Pathological case (e.g., completely full); treat as not found.
        return None

    def _resize(self, new_cap: int) -> None:
        """Rehash all entries into a table of size `new_cap` (rounded to power of two)."""
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
                idx = _fnv1a_64(k) & mask
                while self._keys[idx] is not None:
                    idx = (idx + 1) & mask
                self._keys[idx] = k
                self._values[idx] = v
                self._size += 1
