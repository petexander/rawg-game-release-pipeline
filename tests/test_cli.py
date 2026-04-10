from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from game_release_pipeline.cli import build_parser, main


class CliTests(unittest.TestCase):
    def test_build_parser_supports_run_subcommand(self) -> None:
        args = build_parser().parse_args(
            ["run", "--fixtures-dir", "tests/fixtures/rawg_pages", "--as-of-date", "2026-04-08"]
        )

        self.assertEqual(args.command, "run")
        self.assertEqual(args.fixtures_dir, "tests/fixtures/rawg_pages")
        self.assertEqual(args.as_of_date, "2026-04-08")

    def test_build_parser_supports_duckdb_ui_subcommand(self) -> None:
        args = build_parser().parse_args(["duckdb-ui"])

        self.assertEqual(args.command, "duckdb-ui")

    @patch("game_release_pipeline.cli.run_full_pipeline")
    @patch("game_release_pipeline.cli.PipelineSettings.from_env")
    def test_main_run_uses_fixture_mode_without_api_key(
        self,
        mock_from_env,
        mock_run_full_pipeline,
    ) -> None:
        mock_from_env.return_value = object()
        mock_run_full_pipeline.return_value = SimpleNamespace(
            ingestion=SimpleNamespace(snapshot_date="2026-04-08", rows_loaded=8),
            reports=SimpleNamespace(
                csv_path=Path("/tmp/game_release_calendar_2026-04-08.csv"),
                markdown_path=Path("/tmp/game_release_digest_2026-04-08.md"),
            ),
        )

        with patch("builtins.print") as mock_print:
            exit_code = main(
                ["run", "--fixtures-dir", "tests/fixtures/rawg_pages", "--as-of-date", "2026-04-08"]
            )

        self.assertEqual(exit_code, 0)
        mock_from_env.assert_called_once_with(require_api_key=False)
        mock_run_full_pipeline.assert_called_once()
        self.assertEqual(
            mock_run_full_pipeline.call_args.kwargs["fixtures_dir"],
            Path("tests/fixtures/rawg_pages"),
        )
        mock_print.assert_called_once()

    @patch("game_release_pipeline.cli.ingest_rawg_snapshot")
    @patch("game_release_pipeline.cli.PipelineSettings.from_env")
    def test_main_ingest_requires_api_key_for_live_runs(
        self,
        mock_from_env,
        mock_ingest_rawg_snapshot,
    ) -> None:
        mock_from_env.return_value = object()
        mock_ingest_rawg_snapshot.return_value = SimpleNamespace(
            run_id="run-123",
            snapshot_date="2026-04-08",
            rows_loaded=8,
            pages_fetched=4,
        )

        with patch("builtins.print") as mock_print:
            exit_code = main(["ingest", "--as-of-date", "2026-04-08"])

        self.assertEqual(exit_code, 0)
        mock_from_env.assert_called_once_with(require_api_key=True)
        mock_ingest_rawg_snapshot.assert_called_once()
        mock_print.assert_called_once()

    @patch("game_release_pipeline.cli.subprocess.run")
    @patch("game_release_pipeline.cli.shutil.which")
    @patch("game_release_pipeline.cli.PipelineSettings.from_env")
    def test_main_duckdb_ui_opens_configured_database(
        self,
        mock_from_env,
        mock_which,
        mock_subprocess_run,
    ) -> None:
        mock_from_env.return_value = SimpleNamespace(duckdb_path=Path("/tmp/game_release.duckdb"))
        mock_which.return_value = "/usr/local/bin/duckdb"

        exit_code = main(["duckdb-ui"])

        self.assertEqual(exit_code, 0)
        mock_from_env.assert_called_once_with(require_api_key=False)
        mock_subprocess_run.assert_called_once_with(
            ["/usr/local/bin/duckdb", "-ui", "/tmp/game_release.duckdb"],
            check=True,
        )
