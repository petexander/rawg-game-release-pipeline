#!/usr/bin/env python3
"""Run the full local pipeline: ingest, dbt, checks, and report export."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
for path in (SOURCE_ROOT, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from game_release_pipeline.orchestration import run_full_pipeline
from game_release_pipeline.rawg import parse_as_of_date
from game_release_pipeline.settings import PipelineSettings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of-date", help="Snapshot date in YYYY-MM-DD format.")
    parser.add_argument(
        "--fixtures-dir",
        help="Optional fixture directory for fixture-backed smoke runs.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = PipelineSettings.from_env(require_api_key=not bool(args.fixtures_dir))
    result = run_full_pipeline(
        settings,
        parse_as_of_date(args.as_of_date),
        fixtures_dir=Path(args.fixtures_dir) if args.fixtures_dir else None,
    )
    print(
        "Pipeline completed",
        f"snapshot_date={result.ingestion.snapshot_date}",
        f"rows_loaded={result.ingestion.rows_loaded}",
        f"csv={result.reports.csv_path}",
        f"markdown={result.reports.markdown_path}",
    )


if __name__ == "__main__":
    main()
