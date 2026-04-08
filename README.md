# RAWG Game Release Pipeline

Local-first data engineering portfolio project that ingests live video game release data from the RAWG API, stores snapshots in DuckDB, transforms them with dbt, and publishes a stakeholder-friendly Markdown + CSV report. The repository keeps Airflow in the stack, but optimizes the main developer experience around a simple local CLI run.

## Why This Project Exists

This repo is designed to show an end-to-end data engineering workflow in a form that is easy for hiring managers to scan:

- live API ingestion with retry handling and snapshot metadata
- raw-to-mart transformations in dbt
- local orchestration with both a direct CLI path and an Airflow DAG
- visible output artifacts instead of only internal tables
- fixture-backed automated tests so CI does not depend on secrets

## Architecture

`RAWG API -> Python ingestion -> DuckDB raw schema -> dbt base/intermediate/marts -> Markdown + CSV reports`

Core runtime pieces:

- `scripts/run_local_pipeline.py`: primary one-command local runner
- `scripts/ingest_rawg.py`: raw ingestion only
- `src/game_release_pipeline/`: shared ingestion, dbt, quality-check, and reporting logic
- `analytics/dbt/`: layered dbt project
- `orchestration/airflow/dags/pipeline.py`: optional Airflow DAG using the same shared Python code

## Repository Layout

- `src/game_release_pipeline/` contains the reusable Python application code for ingestion, orchestration, storage, and reporting.
- `analytics/dbt/` contains the dbt project that turns raw API snapshots into reporting marts.
- `orchestration/airflow/` contains the Airflow DAG and Airflow-only runtime helpers.
- `scripts/` contains thin local wrappers for the shared package.
- `tests/` contains fixture-backed verification.
- `docs/` and `analysis/` contain secondary setup notes and ad hoc exploration material.

## Quickstart

1. Install `uv`.
2. Copy the env template and add your RAWG API key.
3. Sync dependencies.
4. Run the full pipeline.

```bash
cp .env.example .env
uv sync
uv run --env-file .env python scripts/run_local_pipeline.py
```

Primary output artifacts:

- `output/reports/game_release_calendar_<date>.csv`
- `output/reports/game_release_digest_<date>.md`

For a no-key smoke run using fixture data:

```bash
uv run python scripts/run_local_pipeline.py --fixtures-dir tests/fixtures/rawg_pages --as-of-date 2026-04-08
```

## Optional Airflow Run

Airflow is included as an orchestration showcase, not the required local entrypoint.

```bash
cp default.env .env
# fill in absolute paths and RAWG_API_KEY
uv run --env-file .env airflow standalone
```

Then open `http://localhost:8080`, enable `rawg_game_release_pipeline`, and trigger a run.

## Sample Output Shape

The generated Markdown digest includes:

- snapshot date and RAWG attribution
- headline KPIs for recent and upcoming titles
- top upcoming releases
- highest-rated recent releases
- monthly platform trend table
- platform and genre mix summaries

The CSV export contains one row per game from `analytics.marts_games__release_calendar`.

## Testing

Automated tests are fixture-backed. CI uses recorded JSON pages under `tests/fixtures/rawg_pages/` so it can validate ingestion, dbt modeling, and report generation without hitting the live API.

Run locally:

```bash
uv run python -m unittest discover -s tests -v
```

## Tradeoffs

- The snapshot is intentionally bounded so the project stays fast to run locally.
- Airflow remains optional to avoid making local setup heavier than the portfolio signal justifies.
- The report emphasizes clarity over exhaustive game coverage or a complex warehouse design.

## Attribution

Data is sourced from the [RAWG Video Games Database](https://rawg.io/apidocs). If you publish generated outputs, keep the RAWG attribution/backlink in place.

Additional setup and design notes live in [docs/setup.md](/Users/petershatwell/Documents/coding-sandbox/data-challenge-copy/docs/setup.md) and [docs/portfolio-notes.md](/Users/petershatwell/Documents/coding-sandbox/data-challenge-copy/docs/portfolio-notes.md).
