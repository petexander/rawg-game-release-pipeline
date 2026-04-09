from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import unittest
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import game_release_pipeline.settings as settings_module


class SettingsTests(unittest.TestCase):
    def test_load_project_env_defaults_reads_project_dotenv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            env_file = project_root / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "RAWG_API_KEY=test-key",
                        "DUCKDB_PATH=./custom.duckdb",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.dict(settings_module.os.environ, {}, clear=True):
                settings_module._load_project_env_defaults(env_file)

                self.assertEqual(settings_module.os.environ["RAWG_API_KEY"], "test-key")
                self.assertEqual(settings_module.os.environ["DUCKDB_PATH"], "./custom.duckdb")


if __name__ == "__main__":
    unittest.main()
