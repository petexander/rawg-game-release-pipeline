"""Command-line interface for the RAWG game release pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
from typing import Sequence

from .orchestration import ingest_rawg_snapshot, run_full_pipeline
from .rawg import parse_as_of_date
from .settings import PipelineSettings


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""

    parser = argparse.ArgumentParser(
        prog="game-release-pipeline",
        description="Run the RAWG game release pipeline locally.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_name, help_text in (
        ("run", "Run ingestion, dbt models, quality checks, and report export."),
        ("ingest", "Load a RAWG snapshot into DuckDB without running dbt or reporting."),
    ):
        command_parser = subparsers.add_parser(command_name, help=help_text)
        command_parser.add_argument(
            "--as-of-date",
            help="Snapshot date in YYYY-MM-DD format.",
        )
        command_parser.add_argument(
            "--fixtures-dir",
            help="Optional fixture directory for deterministic local runs.",
        )

    subparsers.add_parser(
        "duckdb-ui",
        help="Open the local DuckDB UI against the configured database file.",
    )

    return parser


def _resolve_fixtures_dir(fixtures_dir: str | None) -> Path | None:
    return Path(fixtures_dir) if fixtures_dir else None


def _open_duckdb_ui(settings: PipelineSettings) -> int:
    """Launch the DuckDB UI for the configured database."""

    duckdb_binary = shutil.which("duckdb")
    if duckdb_binary is None:
        raise RuntimeError(
            "DuckDB CLI executable not found on PATH. "
            "Run this command through `uv run` so the project's DuckDB dependency is available."
        )

    subprocess.run([duckdb_binary, "-ui", str(settings.duckdb_path)], check=True)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI with an optional argv override."""

    args = build_parser().parse_args(argv)

    if args.command == "duckdb-ui":
        settings = PipelineSettings.from_env(require_api_key=False)
        return _open_duckdb_ui(settings)

    fixtures_dir = _resolve_fixtures_dir(args.fixtures_dir)
    settings = PipelineSettings.from_env(require_api_key=fixtures_dir is None)
    snapshot_date = parse_as_of_date(args.as_of_date)

    if args.command == "run":
        result = run_full_pipeline(
            settings,
            snapshot_date,
            fixtures_dir=fixtures_dir,
        )
        print(
            "Pipeline completed",
            f"snapshot_date={result.ingestion.snapshot_date}",
            f"rows_loaded={result.ingestion.rows_loaded}",
            f"csv={result.reports.csv_path}",
            f"markdown={result.reports.markdown_path}",
        )
        return 0

    summary = ingest_rawg_snapshot(
        settings,
        snapshot_date,
        fixtures_dir=fixtures_dir,
    )
    print(
        "Loaded RAWG snapshot",
        f"run_id={summary.run_id}",
        f"snapshot_date={summary.snapshot_date}",
        f"rows_loaded={summary.rows_loaded}",
        f"pages_fetched={summary.pages_fetched}",
    )
    return 0
