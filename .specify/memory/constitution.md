<!--
Sync Impact Report (2026-10-13)
Version change: 1.2.4 → 1.3.0 (MINOR: new and materially changed rules)
Modified principles: I (concrete rules), II (scope incl. PDFs), III (defines "raw"), IV (planned-test review at task creation, all tests pass at task completion), V (where docs live)
Modified sections: artifact names aligned with Spec Kit (spec.md, plan.md, tasks.md); DB path fixed to data/data.duckdb; commands use `uv run`; versioning rules added
Added: data protection and provenance rules, tech stack additions (Python, uv, pydantic)
Follow-up: .opencode/commands/speckit.{tasks,implement}.md and tasks-template.md customized to enforce Principle IV
-->

# Danish Politician Votes Constitution

## Core Principles

### I. Data-Centric Architecture
Data integrity and accuracy take priority over every other concern. Pipelines MUST be:
- **Reproducible**: dependencies are locked (`uv.lock`) and the database can be rebuilt from source data and code.
- **Idempotent**: re-running a load never creates duplicates or corrupts existing data.
- **Traceable**: every record can be traced to its source (OData entity, or PDF file and page).

The ELT flow (dlt → DuckDB → dbt) is the foundation of the application.

### II. Incremental Data Loading
Ingestion MUST update existing data without full reloads. OData sources MUST use dlt's built-in incremental loading. PDF ingestion MUST skip documents that have already been processed.

### III. Transformation Separation
Raw data is the data as loaded by dlt, plus the original PDF files. It MUST NOT be modified after loading. All transformations, including cleaning and business logic, happen in the dbt layer. This keeps the data auditable and allows reprocessing when logic changes.

### IV. Test-First Development
Tests MUST be planned before implementation and MUST all pass before a task is complete. Test types:
- Unit tests for plain Python code (not dlt/dbt)
- API tests: `uv run pytest tests/api`
- Ingestion tests: `uv run pytest tests/pipeline`
- Data transformation tests: `uv run dbt test` (run from `dbt/`)

**At task creation** (`/speckit.tasks`):
- Every task in `tasks.md` MUST list at least one automated test, giving its file path, test name, and what it validates. Example: "`tests/pipeline/test_actors.py::test_parse_actor` verifies that an OData response is parsed into the pydantic model, mandatory fields are filled when present, and an error is raised when a mandatory field is missing."
- A task with no testable output (pure documentation or configuration) MAY instead state `Tests: N/A` with a reason.
- Before `tasks.md` is written, the full task-to-test overview MUST be presented to the user and explicitly approved. The approval date is recorded in `tasks.md`.

**At task completion** (`/speckit.implement`):
- Tests are written first and MUST fail before the implementation exists.
- A task MUST NOT be marked done until every test listed for it passes.

### V. Documentation First
Documentation MUST be updated in the same change as the code it describes:
- **Data models and transformations**: documented in dbt `schema.yml`/model docs.
- **API endpoints**: documented in the FastAPI OpenAPI metadata.
- **`ARCHITECTURE.md`**: contains a mermaid diagram of the high-level architecture and is updated with every feature.
- **`plan.md`**: every feature plan MUST include a mermaid diagram of the feature's low-level architecture, which the user MUST explicitly accept before the plan is finalized. When modifying existing components, the diagram MUST distinguish pre-existing from new architecture.
- **Architecture decisions** in `plan.md` MUST state why the decision was made, plus its upsides and downsides.

## Technology Stack

- **Python 3.12** managed with **uv** (`uv run`; packages installed via `uv` into `.venv`)
- **dlt** for loading data from the Danish Parliament OData API
- **Custom Python** for PDF download and parsing, with **pydantic** models for parsed data
- **DuckDB** with **dbt** for storage and transformations
- **FastAPI** for the API layer
- **Reflex** for the frontend web application

Adding or replacing a technology requires a constitution amendment.

## Database Rules

- The database is `data/data.duckdb`. It MUST NEVER be deleted or overwritten.
- Before writing `plan.md` or pipeline code (dlt/dbt), inspect the database (read-only) to verify table schemas, column names, data types, and sample records.
- `plan.md` (or the `data-model.md` it references) MUST document source tables and column mappings before `tasks.md` is generated.

## Development Workflow

Features follow the Spec Kit workflow (`/speckit.specify` → `/speckit.plan` → `/speckit.tasks` → `/speckit.implement`). Application code MUST NOT be written until `spec.md` and `plan.md` are drafted and approved.

Before a feature is complete, all three test suites in Principle IV MUST pass.

## Governance

This Constitution takes precedence over `AGENTS.md` and all other project guidance. All PRs and reviews MUST verify compliance.

Amendments require a documented change, maintainer approval, and a migration plan if existing workflows are affected. Versioning follows semantic versioning:
- **MAJOR**: a principle is removed or redefined incompatibly
- **MINOR**: a principle or section is added or materially changed
- **PATCH**: wording or clarification only

Prefer the simplest solution. Added complexity MUST be justified in `plan.md`.

**Version**: 1.3.0 | **Ratified**: 2026-10-01 | **Last Amended**: 2026-10-13
