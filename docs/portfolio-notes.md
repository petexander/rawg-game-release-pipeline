# Portfolio Notes

## Project Intent

This repository is designed to show an end-to-end data engineering workflow in a form that is fast for a reviewer to understand:

- one public CLI for the default local run
- a clear split between ingestion, modeling, orchestration, and reporting
- visible final artifacts instead of only warehouse tables
- deterministic tests that do not require secrets

## Deliberate Tradeoffs

- The warehouse keeps a bounded one-year recent and one-year upcoming snapshot instead of attempting full historical backfill.
- The generated report focuses on the last 90 days and next 90 days, because those windows are easier to interpret than full-month aggregates over sparse data.
- DuckDB is the default runtime because it keeps the project portable and fast to review on a laptop.
- Airflow remains in the repo as an optional orchestration signal, but the main developer experience is intentionally lighter weight.

## Modeling Decisions

- Nested RAWG arrays stay raw as JSON in the ingestion table and are flattened in dbt models.
- The marts layer centers on a release calendar table with one row per title in the latest snapshot.
- A separate ranked-titles mart supports the report's curated near-term highlight sections.

## What I Would Extend Next

- Historical snapshots across multiple run dates.
- Richer dimensions for publishers, developers, and storefronts.
- A dashboard or notebook layer on top of the exported marts.
- Packaging the Airflow path so it requires less manual environment setup.
