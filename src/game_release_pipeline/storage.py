"""DuckDB persistence helpers for raw ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from .rawg import FetchedSnapshot


@dataclass(frozen=True)
class IngestionSummary:
    """A persisted ingestion run summary."""

    run_id: str
    snapshot_date: str
    rows_loaded: int
    pages_fetched: int
    segments_fetched: int


CREATE_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
    run_id VARCHAR PRIMARY KEY,
    snapshot_date DATE,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR,
    window_start_date DATE,
    window_end_date DATE,
    pages_fetched INTEGER,
    segments_fetched INTEGER,
    rows_loaded INTEGER,
    error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.rawg_games_snapshot (
    snapshot_date DATE,
    window_start_date DATE,
    window_end_date DATE,
    run_id VARCHAR,
    segment VARCHAR,
    page_number INTEGER,
    game_id BIGINT,
    slug VARCHAR,
    name VARCHAR,
    released DATE,
    tba BOOLEAN,
    updated_at VARCHAR,
    rating DOUBLE,
    ratings_count INTEGER,
    metacritic INTEGER,
    added INTEGER,
    platforms_json VARCHAR,
    genres_json VARCHAR,
    stores_json VARCHAR,
    esrb_rating_json VARCHAR,
    background_image_url VARCHAR
);
"""


def get_connection(db_path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(CREATE_TABLES_SQL)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS rawg_games_snapshot_snapshot_game_idx
        ON raw.rawg_games_snapshot (snapshot_date, game_id);
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS rawg_games_snapshot_released_idx
        ON raw.rawg_games_snapshot (released, segment);
        """
    )


def persist_snapshot(db_path: Path, snapshot: FetchedSnapshot) -> IngestionSummary:
    """Persist one fetched snapshot and update its run status."""

    conn = get_connection(db_path)
    started_at = datetime.utcnow()

    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO raw.ingestion_runs (
                run_id,
                snapshot_date,
                started_at,
                completed_at,
                status,
                window_start_date,
                window_end_date,
                pages_fetched,
                segments_fetched,
                rows_loaded,
                error_message
            )
            VALUES (?, ?, ?, NULL, 'running', ?, ?, 0, 0, 0, NULL);
            """,
            [
                snapshot.run_id,
                snapshot.snapshot_date,
                started_at,
                snapshot.window_start_date,
                snapshot.window_end_date,
            ],
        )
        conn.execute(
            "DELETE FROM raw.rawg_games_snapshot WHERE snapshot_date = ?;",
            [snapshot.snapshot_date],
        )

        rows = [
            [
                record.snapshot_date,
                record.window_start_date,
                record.window_end_date,
                record.run_id,
                record.segment,
                record.page_number,
                record.game_id,
                record.slug,
                record.name,
                record.released,
                record.tba,
                record.updated_at,
                record.rating,
                record.ratings_count,
                record.metacritic,
                record.added,
                record.platforms_json,
                record.genres_json,
                record.stores_json,
                record.esrb_rating_json,
                record.background_image_url,
            ]
            for record in snapshot.records
        ]

        if rows:
            conn.executemany(
                """
                INSERT INTO raw.rawg_games_snapshot (
                    snapshot_date,
                    window_start_date,
                    window_end_date,
                    run_id,
                    segment,
                    page_number,
                    game_id,
                    slug,
                    name,
                    released,
                    tba,
                    updated_at,
                    rating,
                    ratings_count,
                    metacritic,
                    added,
                    platforms_json,
                    genres_json,
                    stores_json,
                    esrb_rating_json,
                    background_image_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                rows,
            )

        conn.execute(
            """
            UPDATE raw.ingestion_runs
            SET completed_at = ?, status = 'succeeded', pages_fetched = ?, segments_fetched = ?, rows_loaded = ?
            WHERE run_id = ?;
            """,
            [datetime.utcnow(), snapshot.pages_fetched, snapshot.segments_fetched, len(rows), snapshot.run_id],
        )
        return IngestionSummary(
            run_id=snapshot.run_id,
            snapshot_date=snapshot.snapshot_date.isoformat(),
            rows_loaded=len(rows),
            pages_fetched=snapshot.pages_fetched,
            segments_fetched=snapshot.segments_fetched,
        )
    except Exception as exc:
        conn.execute(
            """
            UPDATE raw.ingestion_runs
            SET completed_at = ?, status = 'failed', error_message = ?
            WHERE run_id = ?;
            """,
            [datetime.utcnow(), str(exc), snapshot.run_id],
        )
        raise
    finally:
        conn.close()
