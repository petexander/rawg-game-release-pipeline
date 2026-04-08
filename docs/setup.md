# Setup

This project is built for local runs with DuckDB and `uv`. You do not need Docker, a cloud warehouse, or any external scheduler to get the main pipeline working.

## Prerequisites

- `uv`
- a free RAWG API key for live runs

## Local Pipeline

From the repository root:

```bash
cp .env.example .env
uv sync
uv run --env-file .env python scripts/run_local_pipeline.py
```

That command will:

- fetch a bounded recent + upcoming RAWG snapshot
- write raw tables into `DUCKDB_PATH`
- build dbt base, intermediate, and marts layers
- run mart quality checks
- export Markdown and CSV reports to `output/reports/`

### Reproducible Runs

Use `--as-of-date` to pin the logical run date:

```bash
uv run --env-file .env python scripts/run_local_pipeline.py --as-of-date 2026-04-08
```

### Fixture-Backed Smoke Run

Use the recorded fixtures when you want a deterministic local or CI run without a live API key:

```bash
uv run python scripts/run_local_pipeline.py --fixtures-dir tests/fixtures/rawg_pages --as-of-date 2026-04-08
```

## Ingestion Only

If you want to inspect the raw schema without building dbt:

```bash
uv run --env-file .env python scripts/ingest_rawg.py
```

## dbt Commands

The Python runner sets `DBT_PROFILES_DIR` automatically, but you can run dbt manually from `analytics/dbt/`:

```bash
cd analytics/dbt
export DBT_PROFILES_DIR=$(pwd)
export DUCKDB_PATH=$(pwd)/../../game_release.duckdb

uv run python -m dbt.cli.main debug
uv run python -m dbt.cli.main run --select path:models/base
uv run python -m dbt.cli.main run --select path:models/intermediate
uv run python -m dbt.cli.main run --select path:models/marts
uv run python -m dbt.cli.main test --select marts_games__release_calendar
```

## Airflow

Airflow is optional and uses the same shared orchestration code as the CLI runner.

1. Copy `default.env` to `.env`.
2. Replace the placeholder paths with absolute paths to this repository.
3. Set `RAWG_API_KEY`.
4. Start Airflow:

```bash
uv run --env-file .env airflow standalone
```

Useful commands:

```bash
uv run --env-file .env airflow dags list
uv run --env-file .env airflow dags show rawg_game_release_pipeline
uv run --env-file .env airflow dags trigger rawg_game_release_pipeline
uv run --env-file .env airflow tasks test rawg_game_release_pipeline run_base 2026-04-08
```

## Troubleshooting

- Missing `RAWG_API_KEY`: use `.env.example` or run against fixtures instead.
- Empty marts: confirm ingestion succeeded by querying `raw.ingestion_runs`.
- DuckDB lock issues: close other write-mode processes using the same `.duckdb` file.
