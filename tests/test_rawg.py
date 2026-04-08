from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from game_release_pipeline.rawg import FixtureRawgClient, build_snapshot_window


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "rawg_pages"


class RawgClientTests(unittest.TestCase):
    def test_build_snapshot_window(self) -> None:
        window = build_snapshot_window(date(2026, 4, 8))
        self.assertEqual(window.recent_start.isoformat(), "2025-04-08")
        self.assertEqual(window.recent_end.isoformat(), "2026-04-08")
        self.assertEqual(window.upcoming_start.isoformat(), "2026-04-09")
        self.assertEqual(window.upcoming_end.isoformat(), "2027-04-08")

    def test_fixture_client_reads_multiple_pages(self) -> None:
        client = FixtureRawgClient(FIXTURES_DIR)
        snapshot = client.fetch_snapshot(build_snapshot_window(date(2026, 4, 8)), max_pages_per_segment=3)
        self.assertEqual(snapshot.pages_fetched, 4)
        self.assertEqual(snapshot.segments_fetched, 2)
        self.assertEqual(len(snapshot.records), 8)
        self.assertEqual(snapshot.records[0].segment, "recent")
        self.assertEqual(snapshot.records[-1].segment, "upcoming")


if __name__ == "__main__":
    unittest.main()
