from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import unittest

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from game_release_pipeline.orchestration import run_full_pipeline
from game_release_pipeline.settings import PipelineSettings

FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures" / "rawg_pages"


class PipelineSmokeTests(unittest.TestCase):
    def test_run_full_pipeline_with_fixtures(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            settings = PipelineSettings(
                rawg_api_key=None,
                rawg_base_url="https://api.rawg.io/api",
                duckdb_path=temp_root / "game_release.duckdb",
                output_dir=temp_root / "output" / "reports",
                dbt_project_dir=PROJECT_ROOT / "analytics" / "dbt",
                dbt_profiles_dir=PROJECT_ROOT / "analytics" / "dbt",
                rawg_page_size=40,
                rawg_max_pages_per_segment=3,
                request_timeout_seconds=30,
                request_retry_attempts=3,
                request_retry_backoff_seconds=1.0,
            )

            result = run_full_pipeline(settings, snapshot_date=date(2026, 4, 8), fixtures_dir=FIXTURES_DIR)

            self.assertTrue(result.reports.csv_path.exists())
            self.assertTrue(result.reports.markdown_path.exists())
            self.assertGreater(result.ingestion.rows_loaded, 0)

            exported_calendar = pd.read_csv(result.reports.csv_path)
            self.assertIn("window_start_date", exported_calendar.columns)
            self.assertIn("window_end_date", exported_calendar.columns)

            with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
                mart_row_count = conn.execute(
                    "SELECT COUNT(*) FROM analytics.marts_games__release_calendar"
                ).fetchone()[0]
                self.assertGreater(mart_row_count, 0)


if __name__ == "__main__":
    unittest.main()
