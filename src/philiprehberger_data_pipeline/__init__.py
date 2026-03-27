"""Composable data transformation pipeline with lazy evaluation."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Generic, Iterable, Iterator, TypeVar

__all__ = ["Pipeline"]

T = TypeVar("T")
U = TypeVar("U")


class Pipeline(Generic[T]):
    """A composable, lazy data transformation pipeline.

    Operations are chained and only executed when a terminal method
    (collect, first, count, to_csv, to_json, etc.) is called.
    """

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._source = data
        self._operations: list[Callable[[Iterable], Iterable]] = []

    @classmethod
    def define(cls) -> Pipeline:
        """Create a reusable pipeline definition without data."""
        return cls(None)

    def run(self, data: Iterable[T]) -> list[T]:
        """Apply this pipeline definition to data and collect results."""
        p = Pipeline(data)
        p._operations = list(self._operations)
        return p.collect()

    def _chain(self, op: Callable[[Iterable], Iterable]) -> Pipeline[T]:
        new = Pipeline(self._source)
        new._operations = list(self._operations)
        new._operations.append(op)
        return new

    def _execute(self) -> Iterable[T]:
        if self._source is None:
            raise ValueError("No data source. Use run(data) for reusable pipelines.")
        result: Iterable = self._source
        for op in self._operations:
            result = op(result)
        return result

    # --- Transformation operations ---

    def filter(self, predicate: Callable[[T], bool]) -> Pipeline[T]:
        """Keep only items where predicate returns True."""
        return self._chain(lambda data: (item for item in data if predicate(item)))

    def map(self, fn: Callable[[T], Any]) -> Pipeline:
        """Transform each item."""
        return self._chain(lambda data: (fn(item) for item in data))

    def flat_map(self, fn: Callable[[T], Iterable]) -> Pipeline:
        """Transform each item into an iterable and flatten."""
        def _flat(data: Iterable) -> Iterable:
            for item in data:
                yield from fn(item)
        return self._chain(_flat)

    def flatten(self) -> Pipeline:
        """Flatten one level of nesting."""
        def _flat(data: Iterable) -> Iterable:
            for item in data:
                if isinstance(item, (list, tuple, set, frozenset)):
                    yield from item
                else:
                    yield item
        return self._chain(_flat)

    def sort_by(self, key: str | Callable, reverse: bool = False) -> Pipeline[T]:
        """Sort items. Key can be a dict key string or a callable."""
        if isinstance(key, str):
            k = key
            key_fn: Callable = lambda item: item[k] if isinstance(item, dict) else getattr(item, k)
        else:
            key_fn = key
        return self._chain(lambda data: sorted(data, key=key_fn, reverse=reverse))

    def unique_by(self, key: str | Callable) -> Pipeline[T]:
        """Remove duplicates by key."""
        if isinstance(key, str):
            k = key
            key_fn: Callable = lambda item: item[k] if isinstance(item, dict) else getattr(item, k)
        else:
            key_fn = key

        def _unique(data: Iterable) -> Iterable:
            seen: set = set()
            for item in data:
                val = key_fn(item)
                if val not in seen:
                    seen.add(val)
                    yield item
        return self._chain(_unique)

    def take(self, n: int) -> Pipeline[T]:
        """Take the first n items."""
        if n < 0:
            raise ValueError("take count must be non-negative")

        def _take(data: Iterable) -> Iterable:
            for i, item in enumerate(data):
                if i >= n:
                    break
                yield item
        return self._chain(_take)

    def skip(self, n: int) -> Pipeline[T]:
        """Skip the first n items."""
        if n < 0:
            raise ValueError("skip count must be non-negative")

        def _skip(data: Iterable) -> Iterable:
            for i, item in enumerate(data):
                if i >= n:
                    yield item
        return self._chain(_skip)

    def each(self, fn: Callable[[T], None]) -> Pipeline[T]:
        """Execute a side effect for each item without changing the data."""
        def _each(data: Iterable) -> Iterable:
            for item in data:
                fn(item)
                yield item
        return self._chain(_each)

    def window(self, size: int, step: int = 1) -> Pipeline[list[T]]:
        """Sliding window that groups items into overlapping sublists.

        Args:
            size: Window size (number of items per group).
            step: Number of items to advance between windows.

        Example:
            Pipeline([1,2,3,4,5]).window(3, 1).collect()
            # [[1,2,3], [2,3,4], [3,4,5]]
        """
        if size <= 0:
            raise ValueError("window size must be positive")
        if step <= 0:
            raise ValueError("window step must be positive")

        def _window(data: Iterable) -> Iterable:
            buf: list = []
            for item in data:
                buf.append(item)
                if len(buf) == size:
                    yield list(buf)
                    buf = buf[step:]
        return self._chain(_window)

    def deduplicate(self) -> Pipeline[T]:
        """Remove duplicate items, keeping first occurrence and preserving order.

        Uses a set for O(1) lookups when items are hashable; falls back to
        list-based comparison for unhashable items.
        """
        def _deduplicate(data: Iterable) -> Iterable:
            seen_set: set = set()
            seen_list: list = []
            use_set = True
            for item in data:
                if use_set:
                    try:
                        if item not in seen_set:
                            seen_set.add(item)
                            yield item
                    except TypeError:
                        # Item is unhashable — switch to list-based
                        use_set = False
                        seen_list = list(seen_set)
                        if item not in seen_list:
                            seen_list.append(item)
                            yield item
                else:
                    if item not in seen_list:
                        seen_list.append(item)
                        yield item
        return self._chain(_deduplicate)

    def chunk(self, size: int) -> Pipeline[list[T]]:
        """Split into chunks of the given size."""
        if size <= 0:
            raise ValueError("chunk size must be positive")

        def _chunk(data: Iterable) -> Iterable:
            batch: list = []
            for item in data:
                batch.append(item)
                if len(batch) >= size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        return self._chain(_chunk)

    # --- Terminal operations ---

    def collect(self) -> list[T]:
        """Execute the pipeline and return results as a list."""
        return list(self._execute())

    def first(self, default: T | None = None) -> T | None:
        """Return the first item or default."""
        for item in self._execute():
            return item
        return default

    def count(self) -> int:
        """Count the number of items."""
        return sum(1 for _ in self._execute())

    def group_by(self, key: str | Callable) -> dict[Any, list[T]]:
        """Group items by key and return a dict."""
        if isinstance(key, str):
            k = key
            key_fn: Callable = lambda item: item[k] if isinstance(item, dict) else getattr(item, k)
        else:
            key_fn = key

        groups: dict[Any, list[T]] = defaultdict(list)
        for item in self._execute():
            groups[key_fn(item)].append(item)
        return dict(groups)

    def reduce(self, fn: Callable[[Any, T], Any], initial: Any = None) -> Any:
        """Reduce the pipeline to a single value."""
        result = initial
        for item in self._execute():
            if result is None:
                result = item
            else:
                result = fn(result, item)
        return result

    def sum(self, key: str | Callable | None = None) -> float:
        """Sum numeric values. Optionally extract with key."""
        key_fn = _make_key_fn(key)
        return sum(key_fn(item) for item in self._execute())

    def avg(self, key: str | Callable | None = None) -> float:
        """Average numeric values."""
        key_fn = _make_key_fn(key)
        total = 0.0
        n = 0
        for item in self._execute():
            total += key_fn(item)
            n += 1
        return total / n if n > 0 else 0.0

    def min(self, key: str | Callable | None = None) -> Any:
        """Find minimum value."""
        key_fn = _make_key_fn(key)
        return builtins_min(self._execute(), key=key_fn, default=None)

    def max(self, key: str | Callable | None = None) -> Any:
        """Find maximum value."""
        key_fn = _make_key_fn(key)
        return builtins_max(self._execute(), key=key_fn, default=None)

    # --- Export ---

    def to_json(self, path: str | Path, indent: int = 2) -> None:
        """Write results as JSON."""
        data = self.collect()
        Path(path).write_text(json.dumps(data, indent=indent, default=str))

    def to_csv(
        self,
        path: str | Path,
        headers: list[str] | None = None,
    ) -> None:
        """Write results as CSV. Items should be dicts."""
        data = self.collect()
        if not data:
            Path(path).write_text("")
            return

        if headers is None and isinstance(data[0], dict):
            headers = list(data[0].keys())

        with open(path, "w", newline="") as f:
            if headers:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for row in data:
                    writer.writerow(row if isinstance(row, dict) else {"value": row})
            else:
                writer = csv.writer(f)
                for row in data:
                    writer.writerow(row if isinstance(row, (list, tuple)) else [row])


# Use builtins to avoid shadowing
import builtins as _builtins
builtins_min = _builtins.min
builtins_max = _builtins.max


def _make_key_fn(key: str | Callable | None) -> Callable:
    if key is None:
        return lambda x: x
    if isinstance(key, str):
        k = key
        return lambda item: item[k] if isinstance(item, dict) else getattr(item, k)
    return key
