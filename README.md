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
print(conn.execute('SELECT COUNT(*) FROM raw_actor.actors').fetchall())
"
```

## What's Included

- **src/sources.py** — dlt resource definitions for OData endpoints
- **src/ingest_dlt.py** — dlt pipeline runner  
- **data/data.duckdb** — Single DuckDB database with multiple schemas (one per pipeline)

## Data Available

All data is in `data/data.duckdb` with schemas per pipeline:

**raw_actor schema** (Aktør pipeline):
- `raw_actor.actors` — Politicians, committees, ministries (~18k records)
- `raw_actor.actor_types` — Actor type definitions
- `raw_actor.actor_actor` — Relationships between actors
- `raw_actor.actor_actor_roles` — Role definitions

Future schemas will be added as pipelines are implemented:
- `raw_sag` — Cases and case metadata (when implemented)
- `raw_afstemning` — Votes and voting records (when implemented)

## Next Steps

- Draw data model in draw.io 
- Review wait policy on dlt pipelines, should be larger than currently is 
- Divide sag, møde and afstemning into pipelines and add to dlt
- Add dbt transformations in `dbt_project/`
- Build API layer with FastAPI
- Deploy to production (database URL changes, code stays same)

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design and roadmap.
