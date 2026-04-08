"""Airflow DAG for the RAWG game release portfolio pipeline."""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

from airflow.decorators import dag, task


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = PROJECT_ROOT / "src"
for path in (SOURCE_ROOT, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from game_release_pipeline import (  # noqa: E402
    PipelineSettings,
    check_mart_quality,
    export_reports,
    ingest_rawg_snapshot,
    run_model_layer,
)

DEFAULT_ARGS = {
    "owner": "portfolio",
    "retries": 1,
}


def get_settings() -> PipelineSettings:
    """Load runtime settings from environment variables."""

    return PipelineSettings.from_env(require_api_key=False)


@dag(
    dag_id="rawg_game_release_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def rawg_game_release_pipeline():
    """Orchestrate live RAWG ingestion, dbt transforms, quality checks, and report export."""

    @task
    def ingest_rawg_snapshot_task(**context):
        """Load the latest RAWG snapshot into DuckDB raw tables."""

        summary = ingest_rawg_snapshot(get_settings(), date.fromisoformat(context["ds"]))
        return {
            "run_id": summary.run_id,
            "snapshot_date": summary.snapshot_date,
            "rows_loaded": summary.rows_loaded,
            "pages_fetched": summary.pages_fetched,
        }

    @task
    def run_base():
        """Build base dbt models over the latest raw snapshot."""

        run_model_layer(get_settings(), "base")

    @task
    def run_intermediate():
        """Build intermediate dbt models for release cohorts and trend rollups."""

        run_model_layer(get_settings(), "intermediate")

    @task
    def run_marts():
        """Build final analytics marts for release reporting."""

        run_model_layer(get_settings(), "marts")

    @task
    def check_mart_quality_task():
        """Run dbt tests and explicit quality checks over the final mart."""

        check_mart_quality(get_settings())

    @task
    def export_reports_task(**context):
        """Export the final CSV and Markdown artifacts for the DAG run date."""

        artifacts = export_reports(get_settings(), date.fromisoformat(context["ds"]))
        return {
            "csv_path": str(artifacts.csv_path),
            "markdown_path": str(artifacts.markdown_path),
            "rows_exported": artifacts.rows_exported,
        }

    # Define task dependencies
    raw_snapshot = ingest_rawg_snapshot_task()
    base = run_base()
    intermediate = run_intermediate()
    marts = run_marts()
    quality = check_mart_quality_task()
    report = export_reports_task()

    # Set up the DAG flow
    raw_snapshot >> base >> intermediate >> marts >> quality >> report


rawg_game_release_pipeline()
