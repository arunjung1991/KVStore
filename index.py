# index.py
"""
B+ Tree index for key→value storage (strings). Supports:
  - set(key, value): insert or overwrite (last-write-wins)
  - get(key): Optional[str]
  - clear(): reset the tree
  - len(index): number of keys
  - "k" in index: membership test
  - iterators: items(), keys(), values(), iter_range(start, end), iter_prefix(prefix)

Design:
  - Classic B+ tree (all values live in leaves; internal nodes only route)
  - ORDER controls max children per internal node (fanout)
  - Leaves have up to (ORDER-1) keys; internals have up to (ORDER-1) keys and ORDER children
  - No built-in dict/map; only lists and bisect

Commenting approach:
  - Short “why” comments at complex steps (descent decisions, split boundaries, separator choice)
  - ASCII mini-diagrams in split functions
  - Validation explains the invariants being checked
"""

from __future__ import annotations
from typing import List, Optional, Iterable, Iterator, Tuple
from bisect import bisect_left


# -------------------- Node classes --------------------
class _Node:
    """Base node: keeps sorted separator `keys` and `parent` for upward navigation."""
    __slots__ = ("parent", "keys")

    def __init__(self) -> None:
        self.parent: Optional[_Internal] = None
        self.keys: List[str] = []

    def is_leaf(self) -> bool:
        return isinstance(self, _Leaf)


class _Leaf(_Node):
    """Leaf stores user data: parallel `keys` and `values`, and a `next` pointer for range scans."""
    __slots__ = ("values", "next", "parent", "keys")

    def __init__(self) -> None:
        super().__init__()
        self.values: List[str] = []
        self.next: Optional[_Leaf] = None  # singly-linked list over leaves


class _Internal(_Node):
    """Internal node routes lookups: len(children) == len(keys) + 1."""
    __slots__ = ("children", "parent", "keys")

    def __init__(self) -> None:
        super().__init__()
        self.children: List[_Node] = []


# -------------------- Public Index --------------------
class BPlusTreeIndex:
    """B+ tree over string keys/values.

    Invariants:
      - Internal nodes: sorted keys; len(children) == len(keys) + 1
      - Leaves: `keys` and `values` are sorted-aligned; leaves linked via .next
      - Root is a _Leaf when empty; becomes _Internal once the first split occurs

    Complexity (typical):
      - set/get: O(log_f N) node hops + O(ORDER) in-node list ops
    """

    # Set True during testing to assert invariants after each write
    _ENABLE_VALIDATE_AFTER_WRITE = False

    def __init__(self, order: int = 16) -> None:
        assert order >= 4, "order must be >= 4"
        self.ORDER: int = order
        self.MAX_KEYS: int = self.ORDER - 1
        # Min counts follow classic “half-full” rules (root may violate).
        self.MIN_KEYS_LEAF: int = (self.MAX_KEYS + 1) // 2
        self.MIN_KEYS_INTERNAL: int = (self.MAX_KEYS) // 2
        self._root: _Node = _Leaf()
        self._size: int = 0

    # -------------------- Public API --------------------
    def set(self, key: str, value: str) -> None:
        """Insert or overwrite key’s value (last-write-wins)."""
        # 1) Find the target leaf that *would* contain `key` if present.
        leaf = self._find_leaf(key)

        # 2) Locate insertion/overwrite point inside the leaf using binary search.
        i = bisect_left(leaf.keys, key)

        # 3) Overwrite in-place if key already exists (no structure change).
        if i < len(leaf.keys) and leaf.keys[i] == key:
            leaf.values[i] = value
            if self._ENABLE_VALIDATE_AFTER_WRITE:
                self._validate()
            return

        # 4) Insert new key/value at position i, preserving sorted order.
        leaf.keys.insert(i, key)
        leaf.values.insert(i, value)
        self._size += 1

        # 5) If we overflow capacity, split the leaf and propagate separator up.
        if len(leaf.keys) > self.MAX_KEYS:
            self._split_leaf(leaf)

        if self._ENABLE_VALIDATE_AFTER_WRITE:
            self._validate()

    def get(self, key: str) -> Optional[str]:
        """Return latest value for key, or None if missing."""
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            return leaf.values[i]
        return None

    def clear(self) -> None:
        """Remove all entries (reset to a single empty leaf)."""
        self._root = _Leaf()
        self._size = 0

    def __len__(self) -> int:
        """Number of stored keys."""
        return self._size

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    # -------- Iterators (ordered by key via leaf chain) --------
    def items(self) -> Iterator[Tuple[str, str]]:
        """Yield (key, value) in ascending key order (via leaf linked list)."""
        node = self._leftmost_leaf()
        while node is not None:
            for k, v in zip(node.keys, node.values):
                yield k, v
            node = node.next

    def keys(self) -> Iterator[str]:
        for k, _ in self.items():
            yield k

    def values(self) -> Iterator[str]:
        for _, v in self.items():
            yield v

    def iter_range(self, start: str, end: Optional[str] = None) -> Iterator[Tuple[str, str]]:
        """Yield (k, v) for start ≤ k < end (if end is given)."""
        # Start from the leaf that would contain `start`, then sweep right via .next.
        leaf = self._find_leaf(start)
        i = bisect_left(leaf.keys, start)
        node, idx = leaf, i
        while node:
            while idx < len(node.keys):
                k = node.keys[idx]
                if end is not None and k >= end:
                    return
                yield k, node.values[idx]
                idx += 1
            node, idx = node.next, 0

    def iter_prefix(self, prefix: str) -> Iterator[Tuple[str, str]]:
        """Yield (k, v) for keys starting with `prefix` (string domain)."""
        if prefix == "":
            yield from self.items()
            return
        hi = self._prefix_hi(prefix)
        yield from self.iter_range(prefix, hi)

    def bulk_load(self, items: Iterable[Tuple[str, str]]) -> None:
        """Naive bulk load (O(N log N)): sequential inserts; input need not be sorted."""
        for k, v in items:
            self.set(k, v)

    def stats(self) -> dict:
        """Return simple stats (height, node counts, avg leaf fill)."""
        height, internal_cnt, leaf_cnt, total_leaf_keys = self._collect_stats()
        avg_fill = (total_leaf_keys / (leaf_cnt * self.MAX_KEYS)) if leaf_cnt else 0.0
        return {
            "order": self.ORDER,
            "height": height,
            "internal_nodes": internal_cnt,
            "leaf_nodes": leaf_cnt,
            "size": self._size,
            "avg_leaf_fill_ratio": avg_fill,
        }

    # -------------------- Internal helpers --------------------
    def _leftmost_leaf(self) -> _Leaf:
        """Follow child[0] until a leaf is reached (used for full scans)."""
        node = self._root
        while not node.is_leaf():
            node = _as_internal(node).children[0]
        return node  # type: ignore[return-value]

    def _find_leaf(self, key: str) -> _Leaf:
        """Descend root→leaf using binary searches in internal nodes.

        We use bisect_left so equal separators route to the *left* child. This is
        the standard choice that also aligns with overwrite semantics.
        """
        node = self._root
        while not node.is_leaf():
            internal = _as_internal(node)
            # Example: keys = [k0, k1, k2]; children = [c0, c1, c2, c3]
            # i = first index where keys[i] >= key
            #   key < k0  -> i=0 -> c0
            #   k0<=key<k1-> i=1 -> c1
            #   k1<=key<k2-> i=2 -> c2
            #   key >= k2 -> i=3 -> c3
            i = bisect_left(internal.keys, key)
            node = internal.children[i]
        return node  # type: ignore[return-value]

    def _split_leaf(self, leaf: _Leaf) -> None:
        """Split a full leaf into (leaf, new_leaf) and push the first key of new_leaf up.

        Why the first key of the right sibling?
          In a B+ tree, internal separators are *routing keys*, not stored values.
          Choosing new_leaf.keys[0] ensures all keys left <= sep < all keys right.

        Diagram (K=keys, V=values; '|' indicates split):

            before:  leaf: [k0, k1, k2, k3, k4]  (overflow)
            split @ mid -> left: [k0, k1 |]  right: [| k2, k3, k4]
            sep_key = right.keys[0] = k2  (promoted up to parent)

        Linked list:
            left.next -> right -> old_left.next
        """
        # Mid biased upward so right sibling is non-empty (classic choice).
        mid = (len(leaf.keys) + 1) // 2

        # Create right sibling with upper half.
        new_leaf = _Leaf()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]

        # Truncate left to lower half.
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        # Maintain leaf chain for range scans.
        new_leaf.next = leaf.next
        leaf.next = new_leaf

        # Separator pushed to parent = first key in the right sibling.
        sep_key = new_leaf.keys[0]

        parent = leaf.parent
        if parent is None:
            # Height increases: build a fresh root separating left and right.
            new_root = _Internal()
            new_root.keys = [sep_key]
            new_root.children = [leaf, new_leaf]
            leaf.parent = new_root
            new_leaf.parent = new_root
            self._root = new_root
            return

        # Insert (sep_key, new_leaf) immediately to the right of `leaf` in `parent`.
        self._insert_into_parent(parent, leaf, sep_key, new_leaf)

    def _insert_into_parent(self, parent: _Internal, left: _Node, key: str, right: _Node) -> None:
        """Insert a separator `key` and right-child `right` to the right of `left` in `parent`.

        Parent layout before (keys shown between children):
            [ c0  k0  c1  k1  c2  k2  c3 ]

        If `left` is c1 and key is k1', we insert at position of c1:
            keys.insert(pos, k1') and children.insert(pos+1, right)
        """
        pos = self._find_child_index(parent, left)
        parent.keys.insert(pos, key)
        parent.children.insert(pos + 1, right)
        right.parent = parent

        # If parent overflows, split and bubble one key up.
        if len(parent.keys) > self.MAX_KEYS:
            self._split_internal(parent)

    def _split_internal(self, internal: _Internal) -> None:
        """Split a full internal node and promote the middle key to the parent.

        Internal split differs from leaf split: the middle key is *promoted*
        (removed from both children), preserving routing correctness.

        before:
            keys:   [k0, k1, k2, k3, k4]   (mid=2, promote=k2)
            child:  [c0, c1, c2, c3, c4, c5]

        after:
            left.keys   = [k0, k1]
            left.child  = [c0, c1, c2]
            promote     =  k2   (goes to parent; not kept in children)
            right.keys  = [k3, k4]
            right.child = [c3, c4, c5]
        """
        mid = len(internal.keys) // 2
        promote = internal.keys[mid]

        left = _Internal()
        left.keys = internal.keys[:mid]
        left.children = internal.children[: mid + 1]
        for ch in left.children:
            ch.parent = left

        right = _Internal()
        right.keys = internal.keys[mid + 1 :]
        right.children = internal.children[mid + 1 :]
        for ch in right.children:
            ch.parent = right

        parent = internal.parent
        if parent is None:
            # Height increases: new root separates left/right by `promote`.
            new_root = _Internal()
            new_root.keys = [promote]
            new_root.children = [left, right]
            left.parent = new_root
            right.parent = new_root
            self._root = new_root
            return

        # Replace old `internal` with `left` in parent, then insert (promote, right).
        pos = self._replace_child(parent, internal, left)
        parent.keys.insert(pos, promote)
        parent.children.insert(pos + 1, right)
        right.parent = parent

        if len(parent.keys) > self.MAX_KEYS:
            self._split_internal(parent)

    # -------------------- Validation & Stats (debug helpers) --------------------
    def _validate(self) -> None:
        """Validate B+ tree invariants; raise AssertionError if violated.

        Checks:
          - Keys are sorted at every node.
          - Internal: len(children) == len(keys) + 1 and parent links consistent.
          - Separator keys sit between child key ranges (routing correctness).
          - Leaf chain is non-decreasing across `.next`.
        """
        def dfs(node: _Node) -> List[str]:
            if node.is_leaf():
                leaf: _Leaf = node  # type: ignore
                assert leaf.keys == sorted(leaf.keys)
                assert len(leaf.keys) == len(leaf.values)
                # Return [min_key, max_key] range for routing checks upstream.
                return [leaf.keys[0], leaf.keys[-1]] if leaf.keys else []
            internal: _Internal = node  # type: ignore
            assert len(internal.children) == len(internal.keys) + 1
            assert internal.keys == sorted(internal.keys)
            ranges = []
            for ch in internal.children:
                assert ch.parent is internal
                rng = dfs(ch)
                if rng:
                    ranges.append(rng)
            # Check that each separator `keys[i]` lies between child ranges i and i+1.
            for i, k in enumerate(internal.keys):
                left_max = ranges[i][1] if ranges[i] else None
                right_min = ranges[i + 1][0] if ranges[i + 1] else None
                if left_max is not None:
                    assert left_max <= k
                if right_min is not None:
                    assert k <= right_min
            return [ranges[0][0], ranges[-1][1]] if ranges else []

        dfs(self._root)

        # Verify leaf chain ordering via `.next` pointers.
        if isinstance(self._root, _Internal):
            n: _Node = self._root
            while not n.is_leaf():
                n = _as_internal(n).children[0]
            seen: List[str] = []
            while n is not None:
                leaf: _Leaf = n  # type: ignore
                seen.extend(leaf.keys)
                if leaf.next is not None:
                    assert (not leaf.keys) or leaf.keys[-1] <= leaf.next.keys[0]
                n = leaf.next
            assert seen == sorted(seen)

    def _collect_stats(self) -> Tuple[int, int, int, int]:
        """Return (height, internal_nodes, leaf_nodes, total_leaf_keys)."""
        if isinstance(self._root, _Leaf):
            return (1, 0, 1, len(self._root.keys))
        # Height = edges along the left spine + 1
        h = 1
        n: _Node = self._root
        while not n.is_leaf():
            h += 1
            n = _as_internal(n).children[0]
        # Count nodes with a stack (order doesn't matter)
        internal_cnt = 0
        leaf_cnt = 0
        total_leaf_keys = 0
        q: List[_Node] = [self._root]
        while q:
            cur = q.pop()
            if cur.is_leaf():
                leaf_cnt += 1
                leaf: _Leaf = cur  # type: ignore
                total_leaf_keys += len(leaf.keys)
            else:
                internal_cnt += 1
                q.extend(_as_internal(cur).children)
        return (h, internal_cnt, leaf_cnt, total_leaf_keys)

    # -------------------- Small utilities --------------------
    @staticmethod
    def _prefix_hi(prefix: str) -> str:
        """Exclusive upper bound for all strings starting with `prefix`.

        Choosing `prefix + '\uffff'` (a high BMP char) exceeds any string
        beginning with prefix under typical Unicode ordering.
        """
        return prefix + "\uffff"

    @staticmethod
    def _find_child_index(parent: _Internal, child: _Node) -> int:
        """Return index i such that parent.children[i] is `child` (identity match)."""
        for i, ch in enumerate(parent.children):
            if ch is child:
                return i
        raise RuntimeError("child not found in parent")

    @staticmethod
    def _replace_child(parent: _Internal, old: _Node, new: _Node) -> int:
        """Replace `old` by `new` in `parent.children`; return index where replaced."""
        for i, ch in enumerate(parent.children):
            if ch is old:
                parent.children[i] = new
                new.parent = parent
                return i
        raise RuntimeError("old child not found in parent")


# -------------------- Tiny helper for clean casts --------------------
def _as_internal(n: _Node) -> _Internal:
    """Assert/cast `_Node` to `_Internal` (internal use only)."""
    assert isinstance(n, _Internal), "expected _Internal"
    return n  # type: ignore[return-value]
