"""Runtime configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return float(value)


@dataclass(frozen=True)
class PipelineSettings:
    """All filesystem and API settings required by the pipeline."""

    rawg_api_key: str | None
    rawg_base_url: str
    duckdb_path: Path
    output_dir: Path
    dbt_project_dir: Path
    dbt_profiles_dir: Path
    rawg_page_size: int
    rawg_max_pages_per_segment: int
    request_timeout_seconds: int
    request_retry_attempts: int
    request_retry_backoff_seconds: float

    @classmethod
    def from_env(cls, require_api_key: bool = False) -> "PipelineSettings":
        rawg_api_key = os.getenv("RAWG_API_KEY")
        if require_api_key and not rawg_api_key:
            raise ValueError(
                "RAWG_API_KEY is required for live ingestion. "
                "Set it in your shell or pass --fixtures-dir for a local smoke run."
            )

        duckdb_path = Path(os.getenv("DUCKDB_PATH", PROJECT_ROOT / "game_release.duckdb")).expanduser()
        if not duckdb_path.is_absolute():
            duckdb_path = (PROJECT_ROOT / duckdb_path).resolve()

        return cls(
            rawg_api_key=rawg_api_key,
            rawg_base_url=os.getenv("RAWG_BASE_URL", "https://api.rawg.io/api"),
            duckdb_path=duckdb_path,
            output_dir=PROJECT_ROOT / "output" / "reports",
            dbt_project_dir=PROJECT_ROOT / "analytics" / "dbt",
            dbt_profiles_dir=PROJECT_ROOT / "analytics" / "dbt",
            rawg_page_size=_env_int("RAWG_PAGE_SIZE", 40),
            rawg_max_pages_per_segment=_env_int("RAWG_MAX_PAGES_PER_SEGMENT", 3),
            request_timeout_seconds=_env_int("RAWG_REQUEST_TIMEOUT_SECONDS", 30),
            request_retry_attempts=_env_int("RAWG_REQUEST_RETRY_ATTEMPTS", 3),
            request_retry_backoff_seconds=_env_float("RAWG_REQUEST_RETRY_BACKOFF_SECONDS", 1.0),
        )
