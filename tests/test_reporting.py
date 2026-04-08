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

from game_release_pipeline.reporting import (
    _positive_mean,
    _render_markdown_table,
    render_release_digest,
)


class ReportingTests(unittest.TestCase):
    def test_positive_mean_excludes_zero_ratings(self) -> None:
        series = pd.Series([0.0, 4.0, 5.0], dtype="Float64")

        self.assertEqual(_positive_mean(series), 4.5)

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

    def test_render_markdown_table_formats_integer_like_floats_as_integers(self) -> None:
        dataframe = pd.DataFrame(
            {
                "month": ["2026-05-01"],
                "total_titles": [2.0],
                "upcoming_titles": [2.0],
                "avg_metacritic": [81.5],
            }
        )

        rendered = _render_markdown_table(dataframe)

        self.assertIn("2", rendered)
        self.assertNotIn("2.0", rendered)
        self.assertIn("81.5", rendered)

    def test_render_markdown_table_formats_date_like_values_without_midnight_timestamp(self) -> None:
        dataframe = pd.DataFrame(
            {
                "release_date": [pd.Timestamp("2026-05-14 00:00:00")],
                "release_month": [pd.Timestamp("2026-05-01 00:00:00")],
            }
        )

        rendered = _render_markdown_table(dataframe)

        self.assertIn("2026-05-14", rendered)
        self.assertIn("2026-05-01", rendered)
        self.assertNotIn("T00:00:00", rendered)

    def test_render_release_digest_contains_expected_sections(self) -> None:
        release_calendar = pd.DataFrame(
            [
                {
                    "window_start_date": pd.Timestamp("2025-04-08"),
                    "window_end_date": pd.Timestamp("2027-04-08"),
                    "release_bucket": "recent",
                    "game_name": "Orbit Breaker",
                    "released": pd.Timestamp("2026-03-02"),
                    "days_from_snapshot": -37,
                    "rating": 4.7,
                    "metacritic": 88,
                    "primary_platform": "PC",
                    "genre_names": "Action, RPG",
                    "added": 1240,
                },
                {
                    "window_start_date": pd.Timestamp("2025-04-08"),
                    "window_end_date": pd.Timestamp("2027-04-08"),
                    "release_bucket": "upcoming",
                    "game_name": "Skyline Echo",
                    "released": pd.Timestamp("2026-05-14"),
                    "days_from_snapshot": 36,
                    "rating": 4.2,
                    "metacritic": 81,
                    "primary_platform": "PlayStation 5",
                    "genre_names": "Adventure",
                    "added": 980,
                },
            ]
        )
        top_titles = pd.DataFrame(
            [
                {
                    "title_group": "upcoming_next_90_most_added",
                    "rank_in_group": 1,
                    "game_name": "Skyline Echo",
                    "released": "2026-05-14",
                    "days_from_snapshot": 36,
                    "primary_platform": "PC",
                    "genre_names": "Adventure",
                    "rating": 4.2,
                    "metacritic": 81,
                    "added": 980,
                },
                {
                    "title_group": "recent_last_90_highest_rated",
                    "rank_in_group": 1,
                    "game_name": "Orbit Breaker",
                    "released": "2026-03-02",
                    "days_from_snapshot": -37,
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
            top_titles=top_titles,
        )

        self.assertIn("# RAWG Game Release Digest", digest)
        self.assertIn("## Coverage", digest)
        self.assertIn("Recent release window", digest)
        self.assertIn("Upcoming Releases in Next 90 Days", digest)
        self.assertIn("Skyline Echo", digest)
        self.assertIn("Orbit Breaker", digest)
        self.assertIn("RAWG added", digest)
        self.assertIn("Release Window Summary", digest)
        self.assertNotIn("Monthly Release Trends", digest)
        self.assertIn("Rated titles in last 90 days", digest)
        self.assertNotIn("Metacritic", digest)

    def test_render_release_digest_handles_windows_with_only_null_metacritic(self) -> None:
        release_calendar = pd.DataFrame(
            [
                {
                    "window_start_date": pd.Timestamp("2025-04-08"),
                    "window_end_date": pd.Timestamp("2027-04-08"),
                    "release_bucket": "upcoming",
                    "game_name": "Skyline Echo",
                    "released": pd.Timestamp("2026-05-14"),
                    "days_from_snapshot": 36,
                    "rating": 4.2,
                    "metacritic": pd.NA,
                    "primary_platform": "PC",
                    "genre_names": "Adventure",
                    "added": 980,
                }
            ]
        )
        top_titles = pd.DataFrame(
            [
                {
                    "title_group": "upcoming_next_90_most_added",
                    "rank_in_group": 1,
                    "game_name": "Skyline Echo",
                    "released": "2026-05-14",
                    "days_from_snapshot": 36,
                    "primary_platform": "PC",
                    "genre_names": "Adventure",
                    "rating": 4.2,
                    "metacritic": pd.NA,
                    "added": 980,
                }
            ]
        )

        digest = render_release_digest(
            snapshot_date=date(2026, 4, 8),
            release_calendar=release_calendar,
            top_titles=top_titles,
        )

        self.assertIn("Next 90 days", digest)
        self.assertNotIn("Metacritic", digest)
        self.assertNotIn("| metacritic |", digest)


if __name__ == "__main__":
    unittest.main()
