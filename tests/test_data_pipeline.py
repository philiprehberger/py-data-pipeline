import json
import pytest
from pathlib import Path
from philiprehberger_data_pipeline import Pipeline, retry


def test_filter():
    result = Pipeline([1, 2, 3, 4, 5]).filter(lambda x: x > 3).collect()
    assert result == [4, 5]


def test_map():
    result = Pipeline([1, 2, 3]).map(lambda x: x * 2).collect()
    assert result == [2, 4, 6]


def test_flat_map():
    result = Pipeline([1, 2]).flat_map(lambda x: [x, x * 10]).collect()
    assert result == [1, 10, 2, 20]


def test_flatten():
    result = Pipeline([[1, 2], [3], 4]).flatten().collect()
    assert result == [1, 2, 3, 4]


def test_sort_by_callable():
    result = Pipeline([3, 1, 2]).sort_by(key=lambda x: x).collect()
    assert result == [1, 2, 3]


def test_sort_by_dict_key():
    data = [{"n": 3}, {"n": 1}, {"n": 2}]
    result = Pipeline(data).sort_by("n").collect()
    assert [d["n"] for d in result] == [1, 2, 3]


def test_sort_by_reverse():
    result = Pipeline([1, 2, 3]).sort_by(key=lambda x: x, reverse=True).collect()
    assert result == [3, 2, 1]


def test_unique_by():
    data = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}]
    result = Pipeline(data).unique_by("id").collect()
    assert len(result) == 2
    assert result[0]["v"] == "a"


def test_take():
    result = Pipeline([1, 2, 3, 4, 5]).take(3).collect()
    assert result == [1, 2, 3]


def test_skip():
    result = Pipeline([1, 2, 3, 4, 5]).skip(2).collect()
    assert result == [3, 4, 5]


def test_each():
    side = []
    result = Pipeline([1, 2, 3]).each(lambda x: side.append(x)).collect()
    assert result == [1, 2, 3]
    assert side == [1, 2, 3]


def test_chunk():
    result = Pipeline([1, 2, 3, 4, 5]).chunk(2).collect()
    assert result == [[1, 2], [3, 4], [5]]


def test_first():
    assert Pipeline([10, 20]).first() == 10


def test_first_empty():
    assert Pipeline([]).first(default=99) == 99


def test_count():
    assert Pipeline([1, 2, 3]).count() == 3


def test_group_by():
    data = [{"t": "a", "v": 1}, {"t": "b", "v": 2}, {"t": "a", "v": 3}]
    groups = Pipeline(data).group_by("t")
    assert len(groups["a"]) == 2
    assert len(groups["b"]) == 1


def test_reduce():
    result = Pipeline([1, 2, 3]).reduce(lambda a, b: a + b)
    assert result == 6


def test_sum():
    assert Pipeline([1, 2, 3]).sum() == 6


def test_sum_with_key():
    data = [{"v": 10}, {"v": 20}]
    assert Pipeline(data).sum("v") == 30


def test_avg():
    assert Pipeline([2, 4, 6]).avg() == 4.0


def test_min():
    assert Pipeline([3, 1, 2]).min() == 1


def test_max():
    assert Pipeline([3, 1, 2]).max() == 3


def test_chaining():
    result = (
        Pipeline([1, 2, 3, 4, 5, 6])
        .filter(lambda x: x % 2 == 0)
        .map(lambda x: x * 10)
        .collect()
    )
    assert result == [20, 40, 60]


def test_define_and_run():
    pipe = Pipeline.define().filter(lambda x: x > 2).map(lambda x: x * 2)
    assert pipe.run([1, 2, 3, 4]) == [6, 8]


def test_no_data_raises():
    pipe = Pipeline.define().filter(lambda x: x > 0)
    with pytest.raises(ValueError):
        pipe.collect()


def test_to_json(tmp_path):
    out = tmp_path / "out.json"
    Pipeline([{"a": 1}, {"a": 2}]).to_json(out)
    data = json.loads(out.read_text())
    assert len(data) == 2


def test_to_csv(tmp_path):
    out = tmp_path / "out.csv"
    Pipeline([{"name": "Alice", "age": 30}]).to_csv(out)
    text = out.read_text()
    assert "name" in text
    assert "Alice" in text


def test_empty_pipeline():
    assert Pipeline([]).collect() == []
    assert Pipeline([]).count() == 0


# --- Validation ---


def test_chunk_invalid_size():
    with pytest.raises(ValueError, match="positive"):
        Pipeline([1]).chunk(0)


def test_chunk_negative_size():
    with pytest.raises(ValueError, match="positive"):
        Pipeline([1]).chunk(-1)


def test_take_negative():
    with pytest.raises(ValueError, match="non-negative"):
        Pipeline([1]).take(-1)


def test_skip_negative():
    with pytest.raises(ValueError, match="non-negative"):
        Pipeline([1]).skip(-1)


# --- Edge cases ---


def test_take_zero():
    result = Pipeline([1, 2, 3]).take(0).collect()
    assert result == []


def test_chunk_size_one():
    result = Pipeline([1, 2]).chunk(1).collect()
    assert result == [[1], [2]]


def test_avg_empty():
    result = Pipeline([]).avg()
    assert result == 0.0


def test_min_empty():
    result = Pipeline([]).min()
    assert result is None


def test_max_empty():
    result = Pipeline([]).max()
    assert result is None


def test_sum_with_key_callable():
    data = [{"amount": 10}, {"amount": 20}]
    result = Pipeline(data).sum(lambda x: x["amount"])
    assert result == 30


def test_to_csv_empty(tmp_path):
    path = tmp_path / "out.csv"
    Pipeline([]).to_csv(path)
    assert path.read_text() == ""


def test_chained_with_take():
    result = (
        Pipeline(range(10))
        .filter(lambda x: x % 2 == 0)
        .map(lambda x: x * 10)
        .take(3)
        .collect()
    )
    assert result == [0, 20, 40]


# --- tap ---


def test_tap_does_not_alter_data():
    side = []
    result = Pipeline([1, 2, 3]).tap(lambda x: side.append(x * 10)).collect()
    assert result == [1, 2, 3]
    assert side == [10, 20, 30]


def test_tap_called_in_order():
    log = []
    result = (
        Pipeline([1, 2, 3])
        .tap(lambda x: log.append(("before", x)))
        .map(lambda x: x + 10)
        .tap(lambda x: log.append(("after", x)))
        .collect()
    )
    assert result == [11, 12, 13]
    assert ("before", 1) in log
    assert ("after", 11) in log


# --- branch ---


def test_branch_basic():
    result = (
        Pipeline([1, 2, 3])
        .branch(
            lambda p: p.map(lambda x: x * 2).collect(),
            lambda p: p.filter(lambda x: x > 1).collect(),
        )
        .collect()
    )
    assert result == [2, 4, 6, 2, 3]


def test_branch_single():
    result = (
        Pipeline([10, 20])
        .branch(lambda p: p.map(lambda x: x + 1).collect())
        .collect()
    )
    assert result == [11, 21]


def test_branch_empty_raises():
    with pytest.raises(ValueError, match="at least one"):
        Pipeline([1]).branch()


def test_branch_preserves_source():
    """Each branch receives the same snapshot of data."""
    results = []

    def branch_a(p):
        items = p.collect()
        results.append(items)
        return items

    def branch_b(p):
        items = p.collect()
        results.append(items)
        return items

    Pipeline([1, 2]).branch(branch_a, branch_b).collect()
    assert results[0] == [1, 2]
    assert results[1] == [1, 2]


# --- retry ---


def test_retry_succeeds_first_try():
    result = retry(lambda x: x * 2, attempts=3)(5)
    assert result == 10


def test_retry_succeeds_after_failures():
    call_count = 0

    def flaky(x):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("not yet")
        return x

    wrapped = retry(flaky, attempts=3)
    assert wrapped(42) == 42
    assert call_count == 3


def test_retry_exhausts_attempts():
    def always_fail(x):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        retry(always_fail, attempts=2)(1)


def test_retry_on_error_callback():
    errors = []

    def fail_once(x):
        if not errors:
            raise ValueError("oops")
        return x

    def on_err(exc, attempt):
        errors.append((str(exc), attempt))

    wrapped = retry(fail_once, attempts=3, on_error=on_err)
    result = wrapped(99)
    assert result == 99
    assert len(errors) == 1
    assert errors[0] == ("oops", 1)


def test_retry_invalid_attempts():
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        retry(lambda x: x, attempts=0)


def test_retry_in_pipeline():
    call_counts = {}

    def flaky_double(x):
        call_counts[x] = call_counts.get(x, 0) + 1
        if call_counts[x] < 2:
            raise ValueError("retry me")
        return x * 2

    result = Pipeline([1, 2, 3]).map(retry(flaky_double, attempts=3)).collect()
    assert result == [2, 4, 6]


# --- pipeline composition (__add__) ---


def test_add_pipelines():
    a = Pipeline.define().filter(lambda x: x > 1)
    b = Pipeline.define().map(lambda x: x * 10)
    combined = a + b
    assert combined.run([1, 2, 3]) == [20, 30]


def test_add_preserves_originals():
    a = Pipeline.define().filter(lambda x: x > 1)
    b = Pipeline.define().map(lambda x: x * 10)
    _ = a + b
    # Originals should be unchanged
    assert a.run([1, 2, 3]) == [2, 3]
    assert b.run([1, 2, 3]) == [10, 20, 30]


def test_add_with_data():
    p1 = Pipeline([1, 2, 3, 4]).filter(lambda x: x % 2 == 0)
    p2 = Pipeline.define().map(lambda x: x * 100)
    combined = p1 + p2
    assert combined.collect() == [200, 400]


def test_add_empty_pipelines():
    a = Pipeline.define()
    b = Pipeline.define()
    combined = a + b
    assert combined.run([1, 2]) == [1, 2]


def test_add_returns_not_implemented_for_non_pipeline():
    p = Pipeline.define()
    assert p.__add__(42) is NotImplemented


# --- dry_run ---


def test_dry_run_basic():
    log = (
        Pipeline([1, 2, 3, 4])
        .filter(lambda x: x > 2)
        .map(lambda x: x * 10)
        .dry_run()
    )
    assert len(log) == 2
    assert log[0]["step"] == 0
    assert log[0]["input"] == [1, 2, 3, 4]
    assert log[0]["output"] == [3, 4]
    assert log[1]["step"] == 1
    assert log[1]["input"] == [3, 4]
    assert log[1]["output"] == [30, 40]


def test_dry_run_skips_tap():
    side = []
    log = (
        Pipeline([1, 2])
        .tap(lambda x: side.append(x))
        .map(lambda x: x + 1)
        .dry_run()
    )
    # tap should be skipped, side effects not executed
    assert side == []
    tap_entries = [e for e in log if e.get("skipped")]
    assert len(tap_entries) == 1
    assert tap_entries[0]["name"] == "tap"


def test_dry_run_with_data_argument():
    pipe = Pipeline.define().filter(lambda x: x > 5)
    log = pipe.dry_run([1, 5, 10])
    assert len(log) == 1
    assert log[0]["output"] == [10]


def test_dry_run_no_data_raises():
    pipe = Pipeline.define().map(lambda x: x)
    with pytest.raises(ValueError, match="No data source"):
        pipe.dry_run()


def test_dry_run_empty_pipeline():
    log = Pipeline([1, 2, 3]).dry_run()
    assert log == []


def test_enumerate():
    result = Pipeline([10, 20, 30]).enumerate().collect()
    assert result == [(0, 10), (1, 20), (2, 30)]


def test_enumerate_start():
    result = Pipeline([10, 20]).enumerate(start=5).collect()
    assert result == [(5, 10), (6, 20)]


def test_zip_with():
    result = Pipeline([1, 2, 3]).zip_with(["a", "b", "c"]).collect()
    assert result == [(1, "a"), (2, "b"), (3, "c")]


def test_zip_with_unequal_lengths():
    result = Pipeline([1, 2]).zip_with(["a", "b", "c"]).collect()
    assert result == [(1, "a"), (2, "b")]


def test_take_while():
    result = Pipeline([1, 2, 3, 4, 5]).take_while(lambda x: x < 4).collect()
    assert result == [1, 2, 3]


def test_take_while_all_true():
    result = Pipeline([1, 2, 3]).take_while(lambda x: x < 10).collect()
    assert result == [1, 2, 3]


def test_skip_while():
    result = Pipeline([1, 2, 3, 4, 5]).skip_while(lambda x: x < 3).collect()
    assert result == [3, 4, 5]


def test_skip_while_all_true():
    result = Pipeline([1, 2, 3]).skip_while(lambda x: x < 10).collect()
    assert result == []
