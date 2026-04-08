"""RAWG API clients and snapshot record shaping."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


RAWG_HEADERS = {"User-Agent": "rawg-game-release-pipeline/0.1"}


@dataclass(frozen=True)
class SnapshotWindow:
    """A bounded snapshot window around an as-of date."""

    as_of_date: date
    recent_start: date
    recent_end: date
    upcoming_start: date
    upcoming_end: date

    @property
    def window_start_date(self) -> date:
        return self.recent_start

    @property
    def window_end_date(self) -> date:
        return self.upcoming_end


@dataclass(frozen=True)
class RawgGameRecord:
    """A raw record ready to persist into DuckDB."""

    snapshot_date: date
    window_start_date: date
    window_end_date: date
    run_id: str
    segment: str
    page_number: int
    game_id: int
    slug: str | None
    name: str
    released: date | None
    tba: bool
    updated_at: str | None
    rating: float | None
    ratings_count: int | None
    metacritic: int | None
    added: int | None
    platforms_json: str
    genres_json: str
    stores_json: str
    esrb_rating_json: str | None
    background_image_url: str | None


@dataclass(frozen=True)
class FetchedSnapshot:
    """A full ingestion payload for one logical run."""

    run_id: str
    snapshot_date: date
    window_start_date: date
    window_end_date: date
    records: list[RawgGameRecord]
    pages_fetched: int
    segments_fetched: int


def build_snapshot_window(as_of_date: date) -> SnapshotWindow:
    """Build the recent and upcoming windows used by the pipeline."""

    return SnapshotWindow(
        as_of_date=as_of_date,
        recent_start=as_of_date - timedelta(days=365),
        recent_end=as_of_date,
        upcoming_start=as_of_date + timedelta(days=1),
        upcoming_end=as_of_date + timedelta(days=365),
    )


def parse_iso_date(value: str | None) -> date | None:
    """Parse an ISO date if present."""

    if not value:
        return None
    return date.fromisoformat(value)


def parse_as_of_date(value: str | None) -> date:
    """Resolve an optional CLI date argument."""

    if value:
        return date.fromisoformat(value)
    return date.today()


def _json_dump(value: Any, default: Any) -> str:
    return json.dumps(value if value is not None else default, sort_keys=True)


def _map_game_record(
    game: dict[str, Any],
    *,
    window: SnapshotWindow,
    run_id: str,
    segment: str,
    page_number: int,
) -> RawgGameRecord:
    return RawgGameRecord(
        snapshot_date=window.as_of_date,
        window_start_date=window.window_start_date,
        window_end_date=window.window_end_date,
        run_id=run_id,
        segment=segment,
        page_number=page_number,
        game_id=int(game["id"]),
        slug=game.get("slug"),
        name=game.get("name", "Unknown title"),
        released=parse_iso_date(game.get("released")),
        tba=bool(game.get("tba", False)),
        updated_at=game.get("updated"),
        rating=float(game["rating"]) if game.get("rating") is not None else None,
        ratings_count=int(game["ratings_count"]) if game.get("ratings_count") is not None else None,
        metacritic=int(game["metacritic"]) if game.get("metacritic") is not None else None,
        added=int(game["added"]) if game.get("added") is not None else None,
        platforms_json=_json_dump(game.get("platforms"), []),
        genres_json=_json_dump(game.get("genres"), []),
        stores_json=_json_dump(game.get("stores"), []),
        esrb_rating_json=(
            json.dumps(game["esrb_rating"], sort_keys=True)
            if game.get("esrb_rating") is not None
            else None
        ),
        background_image_url=game.get("background_image"),
    )


class RawgClient:
    """Minimal RAWG list endpoint client with retry handling."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        page_size: int,
        retry_attempts: int,
        retry_backoff_seconds: float,
        timeout_seconds: int,
    ) -> None:
        if not api_key:
            raise ValueError("RAWG_API_KEY is required for live API calls.")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.timeout_seconds = timeout_seconds

    def _request_page(self, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/games?{urlencode(params)}"
        last_error: Exception | None = None

        for attempt in range(1, self.retry_attempts + 1):
            try:
                request = Request(url, headers=RAWG_HEADERS)
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt == self.retry_attempts:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)

        raise RuntimeError(f"RAWG request failed after {self.retry_attempts} attempts: {last_error}")

    def _fetch_segment(
        self,
        *,
        segment: str,
        start_date: date,
        end_date: date,
        ordering: str,
        run_id: str,
        window: SnapshotWindow,
        max_pages: int,
    ) -> tuple[list[RawgGameRecord], int]:
        records: list[RawgGameRecord] = []
        pages_fetched = 0

        for page_number in range(1, max_pages + 1):
            payload = self._request_page(
                {
                    "key": self.api_key,
                    "dates": f"{start_date.isoformat()},{end_date.isoformat()}",
                    "ordering": ordering,
                    "page_size": self.page_size,
                    "page": page_number,
                }
            )
            results = payload.get("results", [])
            if not results:
                break

            pages_fetched += 1
            records.extend(
                _map_game_record(
                    game,
                    window=window,
                    run_id=run_id,
                    segment=segment,
                    page_number=page_number,
                )
                for game in results
            )

            if not payload.get("next"):
                break

        return records, pages_fetched

    def fetch_snapshot(self, window: SnapshotWindow, max_pages_per_segment: int) -> FetchedSnapshot:
        """Fetch recent and upcoming pages into one snapshot payload."""

        run_id = str(uuid.uuid4())
        all_records: list[RawgGameRecord] = []
        total_pages = 0
        segments_fetched = 0

        for segment, start_date, end_date, ordering in (
            ("recent", window.recent_start, window.recent_end, "-released"),
            ("upcoming", window.upcoming_start, window.upcoming_end, "released"),
        ):
            segment_records, pages = self._fetch_segment(
                segment=segment,
                start_date=start_date,
                end_date=end_date,
                ordering=ordering,
                run_id=run_id,
                window=window,
                max_pages=max_pages_per_segment,
            )
            if segment_records:
                segments_fetched += 1
                all_records.extend(segment_records)
                total_pages += pages

        return FetchedSnapshot(
            run_id=run_id,
            snapshot_date=window.as_of_date,
            window_start_date=window.window_start_date,
            window_end_date=window.window_end_date,
            records=all_records,
            pages_fetched=total_pages,
            segments_fetched=segments_fetched,
        )


class FixtureRawgClient:
    """Fixture-backed RAWG client for tests and local smoke runs."""

    def __init__(self, fixtures_dir: Path | str) -> None:
        self.fixtures_dir = Path(fixtures_dir)

    def fetch_snapshot(self, window: SnapshotWindow, max_pages_per_segment: int) -> FetchedSnapshot:
        run_id = str(uuid.uuid4())
        all_records: list[RawgGameRecord] = []
        total_pages = 0
        segments_fetched = 0

        for segment in ("recent", "upcoming"):
            segment_records: list[RawgGameRecord] = []
            segment_pages = 0

            for page_number in range(1, max_pages_per_segment + 1):
                fixture_path = self.fixtures_dir / f"{segment}_page_{page_number}.json"
                if not fixture_path.exists():
                    break

                payload = json.loads(fixture_path.read_text(encoding="utf-8"))
                results = payload.get("results", [])
                if not results:
                    break

                segment_pages += 1
                segment_records.extend(
                    _map_game_record(
                        game,
                        window=window,
                        run_id=run_id,
                        segment=segment,
                        page_number=page_number,
                    )
                    for game in results
                )

                if not payload.get("next"):
                    break

            if segment_records:
                segments_fetched += 1
                total_pages += segment_pages
                all_records.extend(segment_records)

        return FetchedSnapshot(
            run_id=run_id,
            snapshot_date=window.as_of_date,
            window_start_date=window.window_start_date,
            window_end_date=window.window_end_date,
            records=all_records,
            pages_fetched=total_pages,
            segments_fetched=segments_fetched,
        )
