from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from game_release_pipeline.reporting import _render_markdown_table, render_release_digest


class ReportingTests(unittest.TestCase):
    def test_render_markdown_table_handles_nullable_integer_columns(self) -> None:
        dataframe = pd.DataFrame(
            {
                "title": ["Skyline Echo", "Paper Citadel"],
                "added_score": pd.Series([980, None], dtype="Int32"),
                "metacritic": pd.Series([81, None], dtype="Int32"),
            }
        )

        rendered = _render_markdown_table(dataframe)

        self.assertIn("Skyline Echo", rendered)
        self.assertIn("980", rendered)
        self.assertIn("Paper Citadel", rendered)

    def test_render_release_digest_contains_expected_sections(self) -> None:
        release_calendar = pd.DataFrame(
            [
                {
                    "release_bucket": "recent",
                    "rating": 4.7,
                    "metacritic": 88,
                    "primary_platform": "PC",
                    "genre_names": "Action, RPG",
                },
                {
                    "release_bucket": "upcoming",
                    "rating": 4.2,
                    "metacritic": None,
                    "primary_platform": "PlayStation 5",
                    "genre_names": "Adventure",
                },
            ]
        )
        monthly_trends = pd.DataFrame(
            [
                {
                    "release_month": "2026-05-01",
                    "primary_platform": "PC",
                    "total_titles": 2,
                    "upcoming_titles": 2,
                    "recent_titles": 0,
                    "avg_recent_rating": None,
                    "avg_metacritic": None,
                }
            ]
        )
        top_titles = pd.DataFrame(
            [
                {
                    "title_group": "upcoming_most_anticipated",
                    "rank_in_group": 1,
                    "game_name": "Skyline Echo",
                    "released": "2026-05-14",
                    "primary_platform": "PC",
                    "genre_names": "Adventure",
                    "rating": 4.2,
                    "metacritic": None,
                    "added": 980,
                },
                {
                    "title_group": "recent_highest_rated",
                    "rank_in_group": 1,
                    "game_name": "Orbit Breaker",
                    "released": "2026-03-02",
                    "primary_platform": "PC",
                    "genre_names": "Action, RPG",
                    "rating": 4.7,
                    "metacritic": 88,
                    "added": 1240,
                },
            ]
        )

        digest = render_release_digest(
            snapshot_date=date(2026, 4, 8),
            release_calendar=release_calendar,
            monthly_trends=monthly_trends,
            top_titles=top_titles,
        )

        self.assertIn("# RAWG Game Release Digest", digest)
        self.assertIn("Top Upcoming Releases", digest)
        self.assertIn("Skyline Echo", digest)
        self.assertIn("Orbit Breaker", digest)


if __name__ == "__main__":
    unittest.main()
