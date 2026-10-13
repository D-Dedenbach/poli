- Always use uv run to run python commands. All python packages are to be installed via uv in .venv
- Never delete the database (currently data/data.duckdb)

# Repository Agent Rules

The project constitution (`.specify/memory/constitution.md`) takes precedence over this file.

## SDD Workflow Rules
1. Never start writing application code until `specs/<feature>/spec.md` and `specs/<feature>/plan.md` are drafted and approved.
2. Read the spec requirements carefully before proposing implementation steps.
3. Clearly document architecture decisions in plan.md. Note shortly why the decision was made, what the upsides and downsides of it are

## System Verification
- API tests: `uv run pytest tests/api`
- Ingestion tests: `uv run pytest tests/pipeline`
- Data transformation tests: `uv run dbt test` (from `dbt/`)
