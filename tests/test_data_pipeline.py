import json
import pytest
from pathlib import Path
from philiprehberger_data_pipeline import Pipeline


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
