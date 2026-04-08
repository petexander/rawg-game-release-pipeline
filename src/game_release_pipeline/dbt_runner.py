"""dbt subprocess helpers."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from collections.abc import Sequence

from .settings import PipelineSettings


def _dbt_env(settings: PipelineSettings) -> dict[str, str]:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(settings.dbt_profiles_dir)
    env["DUCKDB_PATH"] = str(settings.duckdb_path)
    return env


def run_dbt_command(settings: PipelineSettings, args: Sequence[str]) -> None:
    """Run a dbt command inside the project directory."""

    dbt_executable = Path(os.environ.get("DBT_EXECUTABLE", Path(sys.executable).with_name("dbt")))
    subprocess.run(
        [str(dbt_executable), *args],
        cwd=settings.dbt_project_dir,
        env=_dbt_env(settings),
        check=True,
    )


def run_dbt_layer(settings: PipelineSettings, layer_name: str) -> None:
    """Run one dbt model layer by path."""

    run_dbt_command(settings, ["run", "--select", f"path:models/{layer_name}"])


def run_dbt_tests(settings: PipelineSettings, select_models: Sequence[str]) -> None:
    """Run targeted dbt tests."""

    run_dbt_command(settings, ["test", "--select", *select_models])
