# index.py
"""
B+ Tree index for key→value storage (strings). Supports:
  - set(key, value): insert or overwrite (last-write-wins)
  - get(key): Optional[str]
  - clear(): reset the tree
  - len(index): number of keys

Design:
  - Classic B+ tree (all values in leaves, internal nodes only route)
  - ORDER controls max children per internal node (fanout)
  - Leaves have up to (ORDER-1) keys; internals up to (ORDER-1) keys and ORDER children
  - No built-in dict/map; only lists and bisect
"""

from __future__ import annotations
from typing import List, Optional
from bisect import bisect_left

# -------------------- Tuning constants (no magic numbers) --------------------
ORDER: int = 16                    # Max children per internal node (fanout)
MAX_KEYS: int = ORDER - 1          # Max keys per node
MIN_KEYS_LEAF: int = (MAX_KEYS + 1) // 2   # half-full rule
MIN_KEYS_INTERNAL: int = (MAX_KEYS) // 2   # internal min keys after split

# -------------------- Node classes --------------------
class _Node:
    """Base class. `keys` are sorted. `parent` set for upward navigation."""
    def __init__(self) -> None:
        self.parent: Optional[_Internal] = None
        self.keys: List[str] = []

    def is_leaf(self) -> bool:
        return isinstance(self, _Leaf)

class _Leaf(_Node):
    """Leaf holds (key, value) pairs and links to next leaf."""
    def __init__(self) -> None:
        super().__init__()
        self.values: List[str] = []
        self.next: Optional[_Leaf] = None  # for range scans if needed

class _Internal(_Node):
    """Internal holds separator keys and child pointers (len(children) = len(keys)+1)."""
    def __init__(self) -> None:
        super().__init__()
        self.children: List[_Node] = []

# -------------------- Public Index --------------------
class BPlusTreeIndex:
    """B+ tree index with SET/GET, last-write-wins, no dict/map."""

    def __init__(self) -> None:
        self._root: _Node = _Leaf()
        self._size: int = 0

    # -------------------- Public API --------------------
    def set(self, key: str, value: str) -> None:
        """Insert or overwrite key’s value (last-write-wins)."""
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            # overwrite
            leaf.values[i] = value
            return
        # insert
        leaf.keys.insert(i, key)
        leaf.values.insert(i, value)
        self._size += 1
        if len(leaf.keys) > MAX_KEYS:
            self._split_leaf(leaf)

    def get(self, key: str) -> Optional[str]:
        """Return latest value for key, or None if missing."""
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            return leaf.values[i]
        return None

    def clear(self) -> None:
        """Remove all entries (capacity resets to a single empty leaf)."""
        self._root = _Leaf()
        self._size = 0

    def __len__(self) -> int:
        """Number of stored keys."""
        return self._size

    # -------------------- Internal helpers --------------------
    def _find_leaf(self, key: str) -> _Leaf:
        """Descend from root to the leaf that would contain `key`."""
        node = self._root
        while not node.is_leaf():
            internal: _Internal = node  # type: ignore[assignment]
            # first child with key > target; go left of it
            i = bisect_left(internal.keys, key)
            node = internal.children[i]
        return node  # type: ignore[return-value]

    def _split_leaf(self, leaf: _Leaf) -> None:
        """Split a full leaf into (leaf, new_leaf) and push up separator."""
        mid = (len(leaf.keys) + 1) // 2
        new_leaf = _Leaf()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        # link leaves
        new_leaf.next = leaf.next
        leaf.next = new_leaf

        sep_key = new_leaf.keys[0]

        parent = leaf.parent
        if parent is None:
            # create new root
            new_root = _Internal()
            new_root.keys = [sep_key]
            new_root.children = [leaf, new_leaf]
            leaf.parent = new_root
            new_leaf.parent = new_root
            self._root = new_root
        else:
            self._insert_into_parent(parent, leaf, sep_key, new_leaf)

    def _insert_into_parent(self, parent: _Internal, left: _Node, key: str, right: _Node) -> None:
        """Insert (key, right) into parent to the right of `left` child; split if needed."""
        # find left index
        idx = 0
        while idx < len(parent.children) and parent.children[idx] is not left:
            idx += 1

        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, right)
        right.parent = parent

        if len(parent.keys) > MAX_KEYS:
            self._split_internal(parent)

    def _split_internal(self, internal: _Internal) -> None:
        """Split a full internal node; promote the middle key to parent."""
        mid = len(internal.keys) // 2
        promote = internal.keys[mid]

        left = _Internal()
        left.keys = internal.keys[:mid]
        left.children = internal.children[:mid + 1]
        for ch in left.children:
            ch.parent = left

        right = _Internal()
        right.keys = internal.keys[mid + 1:]
        right.children = internal.children[mid + 1:]
        for ch in right.children:
            ch.parent = right

        parent = internal.parent
        if parent is None:
            # new root
            new_root = _Internal()
            new_root.keys = [promote]
            new_root.children = [left, right]
            left.parent = new_root
            right.parent = new_root
            self._root = new_root
        else:
            self._insert_into_parent(parent, internal, promote, right)
            # replace parent's pointer to `internal` with `left`
            # (we inserted (promote, right) to the right of old position)
            # Find and swap the old child reference
            for i, ch in enumerate(parent.children):
                if ch is internal:
                    parent.children[i] = left
                    break
            left.parent = parent

