# Danish Politician Votes — Architecture

## Overview

This project uses **dlt** (data load tool) for efficient, low-code ETL from the Danish Parliament OData API to DuckDB, with dbt for transformations.

**Why dlt?**
- ✅ **No boilerplate** — Single resource definition per endpoint
- ✅ **Auto-schema inference** — Fields detected automatically from API responses
- ✅ **Built-in pagination** — dlt handles offset/limit logic
- ✅ **dbt integration** — Transforms data after load
- ✅ **Simple to scale** — Add endpoints with 10 lines of code

## Stack

- **dlt** — ETL orchestration (replaces Pydantic + ORM boilerplate)
- **DuckDB** — Embedded OLAP database (perfect for dbt)
- **dbt** — SQL transformations on raw data

## Project Structure

```
danish-politician-votes/
├── sources.py           # dlt resource definitions
├── ingest_dlt.py        # dlt pipeline runner
├── dbt_project/         # (future) dbt transformations
├── db/config.py         # Minimal config
└── pyproject.toml
```

## Quick Start

### 1. Install dependencies
```bash
uv pip install dlt[duckdb] dbt-duckdb
```

### 2. Fetch all data
```bash
python ingest_dlt.py
```

Or fetch specific endpoints:
```bash
python ingest_dlt.py --resources actors actor_types
```

This will:
- Create `ft_dk_pipeline.duckdb`
- Automatically infer schema from OData responses
- Create `raw` dataset with tables: `actors`, `actor_types`, `actor_actor`, `actor_actor_roles`

### 3. Query the data
```python
import duckdb

conn = duckdb.connect("ft_dk_pipeline.duckdb")
result = conn.execute("SELECT COUNT(*) FROM raw.actors").fetchall()
print(f"Total actors: {result[0][0]}")
```

## Data Flow

```
OData API
   ↓
sources.py (dlt resources)
   ↓
ingest_dlt.py (dlt pipeline)
   ↓
DuckDB (ft_dk_pipeline.duckdb)
   ↓
dbt (future: transformations)
   ↓
Analytics / API
```

## How dlt Simplifies Things

**Before (old approach):**
- Pydantic model for validation
- ORM model for persistence  
- Conversion function
- Manual schema creation
- ~100 lines of code per endpoint

**After (dlt approach):**
```python
@dlt.resource(name="actors")
def get_actors():
    skip = 0
    while True:
        response = requests.get("...", params={...})
        items = response.json().get("value", [])
        if not items:
            break
        for item in items:
            yield item  # dlt infers schema from this
        skip += ...
```

That's it! dlt:
- Infers `actors` table schema from yielded dicts
- Handles pagination
- Detects data types
- Evolves schema if API changes
- Creates indexes
- Handles updates/deletes

## Environment Variables

```bash
# dlt will create this database (defaults to project root)
DLT_DATA_PATH="./"
```

## Next: Add dbt Transformations

When ready:

```bash
mkdir dbt_project
cd dbt_project
dbt init --adapter duckdb
```

Create `dbt_project/models/staging/stg_actors.sql`:

```sql
{{ config(materialized='view') }}

SELECT 
    id,
    navn,
    typeid,
    startdato,
    slutdato,
    _dlt_load_id,
    _dlt_id
FROM {{ source('raw', 'actors') }}
WHERE navn IS NOT NULL
```

Run:

```bash
dbt run
dbt test
dbt docs gen
```

## Future: Add API Layer

When ready to expose data:

```python
from fastapi import FastAPI
import duckdb

app = FastAPI()

@app.get("/actors/{id}")
async def get_actor(id: int):
    conn = duckdb.connect("ft_dk_pipeline.duckdb")
    result = conn.execute(
        "SELECT * FROM raw.actors WHERE id = ?",
        [id]
    ).fetchall()
    ...
```

## Key Principles

✅ **Minimal boilerplate** — dlt handles schema, validation, pagination  
✅ **Type inference** — No need to define every field upfront  
✅ **Incremental loading** — dlt tracks state automatically  
✅ **dbt-ready** — Raw tables feed directly into dbt workflows  
✅ **Scalable** — Add endpoints in minutes, not hours  

## Troubleshooting

**Database file location:**
```
ft_dk_pipeline.duckdb  # Created in project root
```

**View dlt metadata:**
```bash
ls -la .dlt/pipelines/ft_dk_pipeline/  # Pipeline state
```

**Clear and reload:**
```bash
rm -rf ft_dk_pipeline.duckdb .dlt/
python ingest_dlt.py
```

## References

- [dlt Docs](https://dlthub.com/docs)
- [dlt + dbt Integration](https://dlthub.com/docs/general-usage/resource#dbt-workflow)
- [DuckDB Python](https://duckdb.org/docs/api/python/overview)

---

## Quick Start

### 1. Install dependencies
```bash
uv pip install duckdb dbt-duckdb sqlalchemy duckdb-engine alembic
```

### 2. Run ingest to fetch and save actors
```bash
python ingest.py
```

For testing (limit to 100 actors):
```bash
python ingest.py --limit 100
```

This will:
- Create `actors.duckdb` if it doesn't exist
- Fetch all actors from OData API
- Validate with Pydantic
- Convert to ORM models
- Save to DuckDB database

### 3. (Optional) Query the database
```python
from db.config import SessionLocal
from db.orm_models import ActorORM

session = SessionLocal()
actors = session.query(ActorORM).limit(5).all()
for actor in actors:
    print(actor.navn, actor.typeid)
```

---

## File Structure

```
danish-politician-votes/
├── client.py              # OData API client (fetch)
├── models.py              # Pydantic models (validation)
├── ingest.py              # ETL script (fetch → DB)
├── db/
│   ├── __init__.py
│   ├── config.py          # DB engine, DDL initialization
│   ├── orm_models.py      # SQLAlchemy ORM models
│   └── repository.py      # Abstract repository + SQLAlchemy impl
├── pyproject.toml
└── README.md (this file)
```

---

## Environment Variables

Control database backend with `DATABASE_URL`:

```bash
# DuckDB (default)
DATABASE_URL="duckdb:///./actors.duckdb"

# PostgreSQL (for hosting)
DATABASE_URL="postgresql://user:pass@localhost/actors_db"

# Enable SQL logging
SQL_ECHO="true"
```

---

## Future: Set Up dbt for Transformations

When ready to add data transformations:

```bash
# Initialize dbt project
dbt init dbt_project
cd dbt_project

# Configure DuckDB adapter in profiles.yml
# Then run:
dbt run        # Execute transformations
dbt test       # Run data quality tests
dbt docs gen   # Generate documentation
```

Your dbt models will sit atop your raw data (actors table) and create `processed` tables:
```sql
-- models/staging/stg_actors.sql
SELECT 
    id,
    navn,
    typeid,
    startdato,
    slutdato
FROM {{ source('raw', 'actors') }}
WHERE navn IS NOT NULL
```

---

## Future: Add API Layer (FastAPI)

When you're ready to expose data to clients:

```python
# api.py (new)
from fastapi import FastAPI, Depends
from db.config import get_db
from db.orm_models import ActorORM

app = FastAPI()

@app.get("/actors/{id}")
def get_actor(id: int, db = Depends(get_db)):
    actor = db.query(ActorORM).filter(ActorORM.id == id).first()
    if not actor:
        raise HTTPException(status_code=404)
    return actor
```

Then `uvicorn api:app --reload` and deploy!

---

## Future: Database Migrations (Alembic)

To evolve schema over time without losing data:

```bash
# Initialize (one-time)
alembic init alembic

# Create a migration
alembic revision --autogenerate -m "Add vote_history table"

# Apply migrations
alembic upgrade head
```

Note: For DuckDB, you may prefer to manage schema in dbt instead.

---

## Next Steps

1. **Run ingest.py** to populate your DuckDB with raw data
2. **Set up dbt** for data transformations when ready
3. **Add more OData endpoints** (ActorType, ActorActor, votes, etc.)
4. **Implement FastAPI endpoints** when ready for client access
5. **Migrate to PostgreSQL** when deploying: just change `DATABASE_URL`

---

## Key Design Principles

✅ **Repository Pattern** → Swap DB backends without changing code  
✅ **Separate Pydantic/ORM models** → API validation ≠ persistence  
✅ **Environment-based config** → Dev/staging/prod with one code base  
✅ **dbt-ready** → Transformations layer built in from the start  
✅ **Type hints everywhere** → IDE autocomplete, mypy validation  

---

Questions? Check `db/repository.py` for the abstract interface and `ingest.py` for an example ingest workflow.
