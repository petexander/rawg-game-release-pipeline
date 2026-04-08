# Setup

This project is designed to run locally with `uv`, DuckDB, and dbt. The default path does not require Docker, a cloud warehouse, or Airflow.

## Prerequisites

- `uv`
- RAWG API key only if you want live ingestion instead of the fixture-backed demo path

## Recommended First Run

Use the recorded RAWG fixtures for a deterministic end-to-end run:

```bash
uv sync
uv run game-release-pipeline run --fixtures-dir tests/fixtures/rawg_pages --as-of-date 2026-04-08
```

That command will:

- load a point-in-time snapshot into DuckDB
- run dbt base, intermediate, and mart models
- run dbt tests plus explicit mart checks
- export Markdown and CSV reports to `output/reports/`

## Live API Run

```bash
cp .env.example .env
# set RAWG_API_KEY
uv run --env-file .env game-release-pipeline run
```

Useful variants:

```bash
uv run --env-file .env game-release-pipeline run --as-of-date 2026-04-08
uv run --env-file .env game-release-pipeline ingest --as-of-date 2026-04-08
```

## Environment Variables

The local CLI uses `.env.example` as the template for live runs:

```bash
RAWG_API_KEY=your_rawg_api_key
RAWG_BASE_URL=https://api.rawg.io/api
DUCKDB_PATH=./game_release.duckdb
RAWG_PAGE_SIZE=40
RAWG_MAX_PAGES_PER_SEGMENT=3
RAWG_REQUEST_TIMEOUT_SECONDS=30
RAWG_REQUEST_RETRY_ATTEMPTS=3
RAWG_REQUEST_RETRY_BACKOFF_SECONDS=1.0
```

## Running dbt Directly

The package CLI configures dbt automatically, but manual dbt commands are available if you want to inspect the transformation layers:

```bash
cd analytics/dbt
export DBT_PROFILES_DIR=$(pwd)
export DUCKDB_PATH=$(pwd)/../../game_release.duckdb

uv run dbt debug
uv run dbt run --select path:models/base
uv run dbt run --select path:models/intermediate
uv run dbt run --select path:models/marts
uv run dbt test --select marts_games__release_calendar marts_games__top_titles
```

## Optional Airflow

Airflow is present as a secondary showcase path. It is not required for the main local run.

Install the optional dependency set:

```bash
uv sync --extra airflow
```

Then configure Airflow-specific environment variables in your shell:

```bash
export AIRFLOW_HOME=$(pwd)/orchestration/airflow
export PYTHONPATH=$(pwd)/orchestration/airflow
```

Start Airflow:

```bash
uv run airflow standalone
```

Useful Airflow commands:

```bash
uv run airflow dags list
uv run airflow dags show rawg_game_release_pipeline
uv run airflow dags trigger rawg_game_release_pipeline
uv run airflow tasks test rawg_game_release_pipeline run_base 2026-04-08
```

## Testing

```bash
uv run python -m unittest discover -s tests -v
```

## Troubleshooting

- Missing `RAWG_API_KEY`: use `.env.example` for live runs or use the fixture-backed command instead.
- Empty marts: inspect `raw.ingestion_runs` and confirm the ingestion step succeeded.
- DuckDB lock issues: close other write-mode processes using the same `.duckdb` file.
