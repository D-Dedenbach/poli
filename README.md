# Danish Politician Votes

ETL pipeline using **dlt** to fetch Danish Parliament voting data from OData API and load into DuckDB, ready for dbt transformations.

## Quick Start

```bash
# Install dependencies
uv sync

# Fetch all data
python -m src.ingest_dlt

# Query results
python -c "
import duckdb
conn = duckdb.connect('data/data.duckdb')
print(conn.execute('SELECT COUNT(*) FROM raw.actors').fetchall())
"
```

## What's Included

- **src/sources.py** — dlt resource definitions for OData endpoints
- **src/ingest_dlt.py** — dlt pipeline runner  
- **data/data.duckdb** — DuckDB database with raw data

## Data Available

- `raw.actors` — Politicians, committees, ministries (~18k records)
- `raw.actor_types` — Actor type definitions
- `raw.actor_actor` — Relationships between actors
- `raw.actor_actor_roles` — Role definitions

## Next Steps

1. Add dbt transformations in `dbt_project/`
2. Build API layer with FastAPI
3. Deploy to production (database URL changes, code stays same)

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design and roadmap.
