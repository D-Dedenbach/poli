# Danish Politician Votes

ETL pipeline fetching Danish Parliament voting data from OData API into DuckDB, with dbt transformations.
A webapp displaying the results.

## Run the webapp locally
If you want to see how the webpage looks like at the moment, run 2 processes: The API and the webapp itself.

```bash
# Note the port spec - reflex and uvicorn use same port by default
uv run uvicorn backend.run_api:app --port 5000 --reload

uv run reflex run
```
Then you will be able to see the webapp at http://localhost:3000/ and API endpoints as specified in run_api with the base url http://localhost:5000/. For instance try http://localhost:5000/polls/latest

## Run the data ingestion

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

The suggestion is clearly to access duckdb via duckdb CLI or a database client like dbgate.

## DLT Pipelines

The `dlt_pipelines` module provides structured ETL pipelines for ingesting Danish Parliament (Folketinget) data from the [OData API](https://oda.ft.dk/api) into DuckDB.

### Running Pipelines

Execute pipelines using the `ingest_dlt.py` module:

```bash
# Install dependencies (if not already installed)
uv sync

# Run a specific source (e.g., actors)
uv run python -m dlt_pipelines.ingest_dlt actors

# Run with specific resources only
uv run python -m dlt_pipelines.ingest_dlt actors --resources actors actor_types

# List all available sources
uv run python -m dlt_pipelines.ingest_dlt --list

# Run all sources
uv run python -m dlt_pipelines.ingest_dlt all

# Run with different log level
uv run python -m dlt_pipelines.ingest_dlt votes --log-level DEBUG
```

### Available Sources and Resources

The following sources are available, each fetching data from different endpoints of the Folketinget OData API:

| Source | Description | Available Resources |
|--------|-------------|---------------------|
| **actors** | Danish parliament actors (persons, parties, committees, ministries) | `actors`, `actor_types`, `actor_actor`, `actor_actor_roles` |
| **votes** | Parliamentary voting data and records | `votes`, `member_votes`, `member_vote_types`, `vote_types` |
| **cases** | Case/legislative proposal data | `case`, `case_status`, `case_type`, `case_category`, `case_step`, `case_step_type`, `case_step_status` |
| **meeting** | Parliamentary meetings (plenum and committees) | `meeting`, `meeting_status`, `meeting_type` |
| **relations** | Parliamentary periods and actor relationship roles | `periods`, `role_definitions` |

Each source uses incremental loading (filtering by `opdateringsdato` since 2020-01-01) and the `merge` write disposition with primary keys to avoid duplicates.

### Command Line Arguments

| Argument | Description | Required | Default |
|----------|-------------|----------|---------|
| `source` | Name of the source to run. Options: `actors`, `votes`, `cases`, `meeting`, `relations`, `all`, or `--list` | No* | None |
| `--resources` | Space-separated list of specific resources to load from the source (loads all if not specified) | No | None |
| `--list` | List all available sources with descriptions | No | False |
| `--log-level` | Logging verbosity level | No | INFO |

*Either `source` or `--list` is required. When using `--list`, the `source` argument is ignored.

**Note:** The `--resources` argument only works with a single source and cannot be used with the `all` option.

### Data Output

All data is loaded into the DuckDB database at `data/data.duckdb` in the `raw` dataset/schema. Tables are named according to the pattern `{source_name}_{resource_name}` (e.g., `raw.actors`, `raw.votes`, `raw.case`).

## Data

All data is in `data/data.duckdb`. Currently includes:
- `raw_actor.actors` — Politicians, committees, ministries (~18k records)
- `raw_actor.actor_types` — Actor type definitions
- `raw_actor.actor_actor` — Relationships between actors
- `raw_actor.actor_actor_roles` — Role definitions


## Run dbt
The dbt project is in the dbt folder, not at project root. Hence to run the dbt models, navigate to /dbt and run
```bash
# all models
uv run dbt build

# specific models
uv run dbt build -s app_poll_outcome
```
The dbt models are categorized into 3 zones:
* staging: Raw data loaded into a dbt model. Only light transformations such as casting data types. Prefix stg_
* intermediary: Transformation layer, for intermediary models as the layer name states. Prefix int_
* app: Models feeding into the API for the webapp. Prefix app_

## Next

See [ARCHITECTURE.md](ARCHITECTURE.md) for design and roadmap.
