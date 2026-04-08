#!/usr/bin/env python3
"""CLI entrypoint for loading RAWG snapshot data into DuckDB."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
for path in (SOURCE_ROOT, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from game_release_pipeline.orchestration import ingest_rawg_snapshot
from game_release_pipeline.rawg import parse_as_of_date
from game_release_pipeline.settings import PipelineSettings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of-date", help="Snapshot date in YYYY-MM-DD format.")
    parser.add_argument(
        "--fixtures-dir",
        help="Optional fixture directory for local smoke runs and tests.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = PipelineSettings.from_env(require_api_key=not bool(args.fixtures_dir))
    summary = ingest_rawg_snapshot(
        settings,
        parse_as_of_date(args.as_of_date),
        fixtures_dir=Path(args.fixtures_dir) if args.fixtures_dir else None,
    )
    print(
        "Loaded RAWG snapshot",
        f"run_id={summary.run_id}",
        f"snapshot_date={summary.snapshot_date}",
        f"rows_loaded={summary.rows_loaded}",
        f"pages_fetched={summary.pages_fetched}",
    )


if __name__ == "__main__":
    main()
