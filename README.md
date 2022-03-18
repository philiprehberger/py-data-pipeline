# philiprehberger-data-pipeline

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
clean_users = (
    Pipeline.define()
    .filter(lambda r: r.get("email"))
    .map(lambda r: {**r, "email": r["email"].lower()})
    .unique_by("email")
)

active = clean_users.run(active_users)
archived = clean_users.run(archived_users)
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

## Operations

| Transform | Description |
|-----------|-------------|
| `.filter(fn)` | Keep items where fn returns True |
| `.map(fn)` | Transform each item |
| `.flat_map(fn)` | Transform and flatten |
| `.sort_by(key)` | Sort by key (string or callable) |
| `.unique_by(key)` | Remove duplicates by key |
| `.take(n)` | Take first n items |
| `.skip(n)` | Skip first n items |
| `.chunk(size)` | Split into chunks |
| `.flatten()` | Flatten one level of nesting |

| Terminal | Description |
|----------|-------------|
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


## API

| Function / Class | Description |
|------------------|-------------|
| `Pipeline(data)` | Composable, lazy data transformation pipeline with chainable operations and terminal methods |

## Development

```bash
pip install -e .
python -m pytest tests/ -v
```

## License

MIT
