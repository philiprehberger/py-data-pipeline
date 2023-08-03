# philiprehberger-data-pipeline

[![Tests](https://github.com/philiprehberger/py-data-pipeline/actions/workflows/publish.yml/badge.svg)](https://github.com/philiprehberger/py-data-pipeline/actions/workflows/publish.yml)
[![PyPI version](https://img.shields.io/pypi/v/philiprehberger-data-pipeline.svg)](https://pypi.org/project/philiprehberger-data-pipeline/)
[![Last updated](https://img.shields.io/github/last-commit/philiprehberger/py-data-pipeline)](https://github.com/philiprehberger/py-data-pipeline/commits/main)

Composable data transformation pipeline with lazy evaluation.

## Installation

```bash
pip install philiprehberger-data-pipeline
```

## Usage

### Basic Pipeline

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
clean_users = (
    Pipeline.define()
    .filter(lambda r: r.get("email"))
    .map(lambda r: {**r, "email": r["email"].lower()})
    .unique_by("email")
)

active = clean_users.run(active_users)
archived = clean_users.run(archived_users)
```

### Sliding Window

```python
data = [1, 2, 3, 4, 5]

# Window of size 3, step 1
Pipeline(data).window(3, 1).collect()
# [[1, 2, 3], [2, 3, 4], [3, 4, 5]]

# Window of size 3, step 2
Pipeline(data).window(3, 2).collect()
# [[1, 2, 3], [3, 4, 5]]
```

### Deduplication

```python
Pipeline([1, 2, 3, 2, 1, 4]).deduplicate().collect()
# [1, 2, 3, 4]

# Works with unhashable items too
Pipeline([{"a": 1}, {"a": 1}, {"b": 2}]).deduplicate().collect()
# [{"a": 1}, {"b": 2}]
```

### Aggregations

```python
p = Pipeline(sales_data)
total = p.sum("amount")
average = p.avg("amount")
grouped = p.group_by("category")
```

### Export

```python
Pipeline(data).filter(...).to_csv("output.csv")
Pipeline(data).filter(...).to_json("output.json")
```

## API

| Function / Class | Description |
|------------------|-------------|
| `Pipeline(data)` | Composable, lazy data transformation pipeline with chainable operations and terminal methods |
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
| `.window(size, step)` | Sliding window grouping |
| `.deduplicate()` | Remove duplicate items preserving order |
| `.collect()` | Execute and return list |
| `.first()` | Return first item |
| `.count()` | Count items |
| `.sum(key)` | Sum values |
| `.avg(key)` | Average values |
| `.min(key)` | Find minimum value |
| `.max(key)` | Find maximum value |
| `.reduce(fn, initial)` | Reduce to single value |
| `.group_by(key)` | Group into dict |
| `.to_csv(path)` | Export as CSV |
| `.to_json(path)` | Export as JSON |

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
