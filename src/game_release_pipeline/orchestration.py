"""Shared orchestration used by both CLI scripts and Airflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .dbt_runner import run_dbt_layer, run_dbt_tests
from .rawg import FixtureRawgClient, RawgClient, build_snapshot_window
from .reporting import ReportArtifacts, export_reports as export_report_artifacts
from .settings import PipelineSettings
from .storage import IngestionSummary, get_connection, persist_snapshot


@dataclass(frozen=True)
class PipelineRunResult:
    """End-to-end pipeline outputs."""

    ingestion: IngestionSummary
    reports: ReportArtifacts


def ingest_rawg_snapshot(
    settings: PipelineSettings,
    snapshot_date: date,
    fixtures_dir: Path | str | None = None,
) -> IngestionSummary:
    """Ingest one RAWG snapshot into the raw DuckDB schema."""

    window = build_snapshot_window(snapshot_date)
    if fixtures_dir is not None:
        client = FixtureRawgClient(fixtures_dir)
    else:
        client = RawgClient(
            api_key=settings.rawg_api_key or "",
            base_url=settings.rawg_base_url,
            page_size=settings.rawg_page_size,
            retry_attempts=settings.request_retry_attempts,
            retry_backoff_seconds=settings.request_retry_backoff_seconds,
            timeout_seconds=settings.request_timeout_seconds,
        )

    fetched_snapshot = client.fetch_snapshot(window, settings.rawg_max_pages_per_segment)
    if not fetched_snapshot.records:
        raise ValueError("No RAWG titles were fetched for the requested window.")
    return persist_snapshot(settings.duckdb_path, fetched_snapshot)


def run_model_layer(settings: PipelineSettings, layer_name: str) -> None:
    """Run a named dbt layer."""

    run_dbt_layer(settings, layer_name)


def check_mart_quality(settings: PipelineSettings) -> None:
    """Run dbt tests and a few explicit row-count checks."""

    run_dbt_tests(
        settings,
        [
            "marts_games__release_calendar",
            "marts_games__top_titles",
            "base_rawg__game_platforms",
        ],
    )

    with get_connection(settings.duckdb_path, read_only=True) as conn:
        mart_row_count = conn.execute(
            "SELECT COUNT(*) FROM analytics.marts_games__release_calendar"
        ).fetchone()[0]
        if mart_row_count == 0:
            raise ValueError("analytics.marts_games__release_calendar is empty.")

        null_key_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM analytics.marts_games__release_calendar
            WHERE game_id IS NULL
                OR game_name IS NULL
                OR release_bucket IS NULL
            """
        ).fetchone()[0]
        if null_key_count > 0:
            raise ValueError("Key mart columns contain nulls.")


def export_reports(settings: PipelineSettings, snapshot_date: date) -> ReportArtifacts:
    """Export the final report artifacts."""

    return export_report_artifacts(settings, snapshot_date)


def run_full_pipeline(
    settings: PipelineSettings,
    snapshot_date: date,
    fixtures_dir: Path | str | None = None,
) -> PipelineRunResult:
    """Run ingestion, dbt layers, data checks, and report export."""

    ingestion = ingest_rawg_snapshot(settings, snapshot_date, fixtures_dir=fixtures_dir)
    for layer_name in ("base", "intermediate", "marts"):
        run_model_layer(settings, layer_name)
    check_mart_quality(settings)
    reports = export_reports(settings, snapshot_date)
    return PipelineRunResult(ingestion=ingestion, reports=reports)
