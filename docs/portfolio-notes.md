# Portfolio Notes

## Design Goals

- Keep the repo runnable on a laptop with one primary command.
- Show a clear separation between ingestion, transformation, orchestration, and reporting.
- Preserve enough operational realism to be credible without turning the project into infrastructure overhead.

## Repository Layout

- `src/game_release_pipeline/` is the reusable application layer.
- `analytics/dbt/` is the transformation layer.
- `orchestration/airflow/` is the optional orchestration runtime.
- `scripts/` is for thin wrappers only.
- `analysis/sql/` is for exploratory queries, not runtime code.

## Deliberate Scope Choices

- The pipeline uses the RAWG `games` list endpoint only for v1.
- Raw nested arrays stay as JSON in `raw.rawg_games_snapshot`, then dbt handles flattening into bridge models.
- Airflow is optional because it is valuable as a portfolio signal but too heavy to force as the only local entrypoint.
- CI runs against fixture pages so automated verification is deterministic and secret-free.

## Future Extensions

- Incremental historical marts across many snapshot dates
- richer dimensional models for publishers, developers, and stores
- a lightweight dashboard over the exported marts
- containerized local runtime for fully reproducible onboarding
