# philiprehberger-data-pipeline

[![Tests](https://github.com/philiprehberger/py-data-pipeline/actions/workflows/publish.yml/badge.svg)](https://github.com/philiprehberger/py-data-pipeline/actions/workflows/publish.yml)
[![PyPI version](https://img.shields.io/pypi/v/philiprehberger-data-pipeline.svg)](https://pypi.org/project/philiprehberger-data-pipeline/)
[![Last updated](https://img.shields.io/github/last-commit/philiprehberger/py-data-pipeline)](https://github.com/philiprehberger/py-data-pipeline/commits/main)

![philiprehberger-data-pipeline](https://raw.githubusercontent.com/philiprehberger/py-data-pipeline/main/package-card.webp)

Composable data transformation pipeline with lazy evaluation.

## Installation

```bash
pip install philiprehberger-data-pipeline
```

## Usage

```python
from philiprehberger_data_pipeline import Pipeline

data = [
    {"name": " Alice ", "email": "alice@example.com", "status": "active", "age": 30},
    {"name": "Bob", "email": "bob@example.com", "status": "inactive", "age": 25},
    {"name": "Alice", "email": "alice@example.com", "status": "active", "age": 30},
]

result = (
    Pipeline(data)
    .filter(lambda r: r["status"] == "active")
    .map(lambda r: {**r, "name": r["name"].strip()})
    .unique_by("email")
    .sort_by("name")
    .collect()
)
```

### Reusable Pipelines

```python
from philiprehberger_data_pipeline import Pipeline

clean_users = (
    Pipeline.define()
    .filter(lambda r: r.get("email"))
    .map(lambda r: {**r, "email": r["email"].lower()})
    .unique_by("email")
)

active = clean_users.run(active_users)
archived = clean_users.run(archived_users)
```

### Tap (Side Effects)

```python
from philiprehberger_data_pipeline import Pipeline

result = (
    Pipeline([1, 2, 3])
    .tap(lambda x: print(f"Processing: {x}"))
    .map(lambda x: x * 2)
    .collect()
)
# Prints each item without altering the data
```

### Branch (Parallel Splits)

```python
from philiprehberger_data_pipeline import Pipeline

result = (
    Pipeline([1, 2, 3])
    .branch(
        lambda p: p.map(lambda x: x * 2).collect(),
        lambda p: p.filter(lambda x: x > 1).collect(),
    )
    .collect()
)
# [2, 4, 6, 2, 3]
```

### Retry Wrapper

```python
from philiprehberger_data_pipeline import Pipeline, retry

def fetch_url(url):
    # might fail transiently
    return requests.get(url).text

result = Pipeline(urls).map(retry(fetch_url, attempts=3, delay=1.0)).collect()
```

### Pipeline Composition

```python
from philiprehberger_data_pipeline import Pipeline

clean = Pipeline.define().filter(lambda x: x > 0).map(lambda x: x * 2)
limit = Pipeline.define().take(3)

combined = clean + limit
combined.run([−1, 5, 0, 3, 7, 2])
# [10, 6, 14]
```

### Dry Run

```python
from philiprehberger_data_pipeline import Pipeline

log = (
    Pipeline([1, 2, 3, 4])
    .filter(lambda x: x > 2)
    .map(lambda x: x * 10)
    .dry_run()
)
# [{"step": 0, "name": "filter", "input": [1,2,3,4], "output": [3,4]},
#  {"step": 1, "name": "map", "input": [3,4], "output": [30,40]}]
```

### Sliding Window

```python
from philiprehberger_data_pipeline import Pipeline

Pipeline([1, 2, 3, 4, 5]).window(3, 1).collect()
# [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
```

### Aggregations

```python
from philiprehberger_data_pipeline import Pipeline

p = Pipeline(sales_data)
total = p.sum("amount")
average = p.avg("amount")
grouped = p.group_by("category")
```

### Peek and count_by

`peek(n)` materializes the first `n` items for quick inspection without committing to a `collect()`. `count_by(key)` returns a per-key occurrence count, similar to `group_by` but without the per-value lists.

```python
from philiprehberger_data_pipeline import Pipeline

Pipeline([10, 20, 30, 40, 50, 60]).filter(lambda x: x > 15).peek(3)
# [20, 30, 40]

Pipeline([
    {"category": "fruit"},
    {"category": "veg"},
    {"category": "fruit"},
]).count_by("category")
# {"fruit": 2, "veg": 1}
```

### Export

```python
from philiprehberger_data_pipeline import Pipeline

Pipeline(data).filter(lambda x: x["active"]).to_csv("output.csv")
Pipeline(data).filter(lambda x: x["active"]).to_json("output.json")
```

## API

| Function / Class | Description |
|------------------|-------------|
| `Pipeline(data)` | Composable, lazy data transformation pipeline |
| `.filter(fn)` | Keep items where fn returns True |
| `.map(fn)` | Transform each item |
| `.flat_map(fn)` | Transform and flatten |
| `.flatten()` | Flatten one level of nesting |
| `.sort_by(key)` | Sort by key (string or callable) |
| `.unique_by(key)` | Remove duplicates by key |
| `.take(n)` | Take first n items |
| `.skip(n)` | Skip first n items |
| `.chunk(size)` | Split into chunks |
| `.each(fn)` | Execute side effect for each item |
| `.tap(fn)` | Side effect without altering data, skipped in dry run |
| `.window(size, step)` | Sliding window grouping |
| `.deduplicate()` | Remove duplicate items preserving order |
| `.branch(*fns)` | Split into parallel branches and merge results |
| `.dry_run(data)` | Log each step's input/output without side effects |
| `pipeline_a + pipeline_b` | Compose two pipelines into one |
| `.collect()` | Execute and return list |
| `.first()` | Return first item |
| `.count()` | Count items |
| `.sum(key)` | Sum values |
| `.avg(key)` | Average values |
| `.min(key)` | Find minimum value |
| `.max(key)` | Find maximum value |
| `.reduce(fn, initial)` | Reduce to single value |
| `.group_by(key)` | Group into dict |
| `.count_by(key)` | Count occurrences per key value |
| `.peek(n)` | Return the first `n` items as a list (debugging) |
| `.to_csv(path)` | Export as CSV |
| `.to_json(path)` | Export as JSON |
| `.enumerate(start)` | Pair each item with its index |
| `.zip_with(other)` | Pair items with another iterable |
| `.take_while(fn)` | Take items while predicate is True |
| `.skip_while(fn)` | Skip items while predicate is True |
| `retry(fn, attempts, delay, on_error)` | Wrap a step function with configurable retry logic |

## Development

```bash
pip install -e .
python -m pytest tests/ -v
```

## Support

If you find this project useful:

⭐ [Star the repo](https://github.com/philiprehberger/py-data-pipeline)

🐛 [Report issues](https://github.com/philiprehberger/py-data-pipeline/issues?q=is%3Aissue+is%3Aopen+label%3Abug)

💡 [Suggest features](https://github.com/philiprehberger/py-data-pipeline/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement)

❤️ [Sponsor development](https://github.com/sponsors/philiprehberger)

🌐 [All Open Source Projects](https://philiprehberger.com/open-source-packages)

💻 [GitHub Profile](https://github.com/philiprehberger)

🔗 [LinkedIn Profile](https://www.linkedin.com/in/philiprehberger)

## License

[MIT](LICENSE)
