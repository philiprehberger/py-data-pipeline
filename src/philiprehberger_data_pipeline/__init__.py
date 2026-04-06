"""Composable data transformation pipeline with lazy evaluation."""

from __future__ import annotations

import csv
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Generic, Iterable, TypeVar

__all__ = ["Pipeline", "retry"]

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

    def tap(self, fn: Callable[[T], None]) -> Pipeline[T]:
        """Execute a side-effect function for each item without altering data.

        Unlike ``each``, ``tap`` is skipped during dry-run mode, making it
        suitable for logging, metrics, or other side effects that should not
        run in a dry run.
        """
        def _tap(data: Iterable) -> Iterable:
            for item in data:
                fn(item)
                yield item
        _tap._is_tap = True  # type: ignore[attr-defined]
        return self._chain(_tap)

    def branch(self, *branches: Callable[[Pipeline[T]], list]) -> Pipeline:
        """Split the pipeline into parallel branches and merge results.

        Each branch receives a new ``Pipeline`` wrapping the current data
        snapshot and must return a list of results.  All branch results are
        concatenated in order.

        Example::

            Pipeline([1, 2, 3]).branch(
                lambda p: p.map(lambda x: x * 2).collect(),
                lambda p: p.filter(lambda x: x > 1).collect(),
            ).collect()
            # [2, 4, 6, 2, 3]
        """
        if not branches:
            raise ValueError("branch requires at least one branch function")

        def _branch(data: Iterable) -> Iterable:
            snapshot = list(data)
            for branch_fn in branches:
                yield from branch_fn(Pipeline(snapshot))
        return self._chain(_branch)

    def enumerate(self, start: int = 0) -> Pipeline[tuple[int, T]]:
        """Pair each item with its index."""
        def _enumerate(data: Iterable) -> Iterable:
            for i, item in builtins_enumerate(data, start):
                yield (i, item)
        return self._chain(_enumerate)

    def zip_with(self, other: Iterable) -> Pipeline[tuple]:
        """Pair items from this pipeline with items from another iterable."""
        def _zip(data: Iterable) -> Iterable:
            yield from zip(data, other)
        return self._chain(_zip)

    def take_while(self, predicate: Callable[[T], bool]) -> Pipeline[T]:
        """Take items while predicate returns True, stop at first False."""
        def _take_while(data: Iterable) -> Iterable:
            for item in data:
                if predicate(item):
                    yield item
                else:
                    break
        return self._chain(_take_while)

    def skip_while(self, predicate: Callable[[T], bool]) -> Pipeline[T]:
        """Skip items while predicate returns True, then yield the rest."""
        def _skip_while(data: Iterable) -> Iterable:
            skipping = True
            for item in data:
                if skipping and predicate(item):
                    continue
                skipping = False
                yield item
        return self._chain(_skip_while)

    def dry_run(self, data: Iterable[T] | None = None) -> list[dict[str, Any]]:
        """Execute the pipeline logging each step's input/output without side effects.

        ``tap`` steps are skipped.  Returns a list of dicts describing each
        step's behaviour::

            [{"step": 0, "name": "filter", "input": [...], "output": [...]}, ...]

        If *data* is provided it is used as the source; otherwise the
        pipeline's bound data is used.
        """
        source: Iterable[T]
        if data is not None:
            source = data
        elif self._source is not None:
            source = self._source
        else:
            raise ValueError("No data source. Pass data to dry_run() or use a bound pipeline.")

        log: list[dict[str, Any]] = []
        current = list(source)

        for i, op in enumerate(self._operations):
            # Detect tap operations — skip them in dry-run mode
            if getattr(op, "_is_tap", False):
                log.append({
                    "step": i,
                    "name": "tap",
                    "skipped": True,
                })
                continue

            output = list(op(iter(current)))
            name = _infer_op_name(op)
            log.append({
                "step": i,
                "name": name,
                "input": list(current),
                "output": list(output),
            })
            current = output

        return log

    def __add__(self, other: Pipeline) -> Pipeline:
        """Compose two pipelines: ``pipeline_a + pipeline_b``.

        Returns a new pipeline whose operations are the concatenation of
        both pipelines' operations, using the left pipeline's data source.
        """
        if not isinstance(other, Pipeline):
            return NotImplemented
        new = Pipeline(self._source)
        new._operations = list(self._operations) + list(other._operations)
        return new

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
builtins_enumerate = _builtins.enumerate


def _make_key_fn(key: str | Callable | None) -> Callable:
    if key is None:
        return lambda x: x
    if isinstance(key, str):
        k = key
        return lambda item: item[k] if isinstance(item, dict) else getattr(item, k)
    return key


def _infer_op_name(op: Callable) -> str:
    """Best-effort name extraction from a pipeline operation closure."""
    qual = getattr(op, "__qualname__", "") or ""
    # Closures generated by _chain typically look like
    # "Pipeline.filter.<locals>.<lambda>" or "Pipeline.chunk.<locals>._chunk"
    parts = qual.split(".")
    for part in parts:
        if part in (
            "filter", "map", "flat_map", "flatten", "sort_by", "unique_by",
            "take", "skip", "each", "window", "deduplicate", "chunk",
            "tap", "branch", "enumerate", "take_while", "skip_while",
        ):
            return part
    # Fall back to the function's __name__
    name = getattr(op, "__name__", "unknown")
    if name == "<lambda>":
        return "lambda"
    return name


def retry(
    fn: Callable[[T], Any],
    attempts: int = 3,
    delay: float = 0.0,
    on_error: Callable[[Exception, int], None] | None = None,
) -> Callable[[T], Any]:
    """Wrap a step function with retry logic for use in ``.map()`` or ``.each()``.

    Args:
        fn: The function to wrap.
        attempts: Maximum number of tries (must be >= 1).
        delay: Seconds to wait between retries.
        on_error: Optional callback receiving ``(exception, attempt_number)``.

    Returns:
        A wrapped function with the same signature as *fn*.

    Example::

        Pipeline(urls).map(retry(fetch_url, attempts=3, delay=1.0)).collect()
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def wrapper(item: T) -> Any:
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return fn(item)
            except Exception as exc:
                last_exc = exc
                if on_error is not None:
                    on_error(exc, attempt)
                if attempt < attempts and delay > 0:
                    time.sleep(delay)
        raise last_exc  # type: ignore[misc]

    wrapper.__name__ = getattr(fn, "__name__", "retry_wrapper")
    wrapper.__qualname__ = getattr(fn, "__qualname__", "retry_wrapper")
    return wrapper
