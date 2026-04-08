"""Report generation for final stakeholder-facing outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import math
from pathlib import Path

import duckdb
import pandas as pd

from .settings import PipelineSettings


@dataclass(frozen=True)
class ReportArtifacts:
    """Paths to generated reporting files."""

    csv_path: Path
    markdown_path: Path
    rows_exported: int


def _render_markdown_table(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return "_No rows available._"

    headers = list(dataframe.columns)
    rows = [
        [_format_table_value(value) for value in row]
        for row in dataframe.itertuples(index=False, name=None)
    ]
    widths = [len(header) for header in headers]

    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def _line(values: list[str]) -> str:
        padded = [value.ljust(widths[index]) for index, value in enumerate(values)]
        return "| " + " | ".join(padded) + " |"

    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    rendered = [_line(headers), separator]
    rendered.extend(_line(row) for row in rows)
    return "\n".join(rendered)


def _format_metric(value: float | int | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:.{digits}f}"


def _format_table_value(value: object) -> str:
    if pd.isna(value):
        return "n/a"
    if isinstance(value, pd.Timestamp):
        if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
            return value.date().isoformat()
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, datetime):
        if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
            return value.date().isoformat()
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return f"{int(value):,}"
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def _slice_release_window(
    release_calendar: pd.DataFrame,
    *,
    release_bucket: str,
    min_days: int,
    max_days: int,
) -> pd.DataFrame:
    return release_calendar[
        (release_calendar["release_bucket"] == release_bucket)
        & release_calendar["days_from_snapshot"].between(min_days, max_days, inclusive="both")
    ].copy()


def _rounded_mean(series: pd.Series) -> float | None:
    value = series.dropna().mean()
    if value is None or pd.isna(value):
        return None
    return round(float(value), 2)


def _build_window_summary(release_calendar: pd.DataFrame) -> pd.DataFrame:
    windows = [
        ("Last 30 days", "recent", -30, 0),
        ("Last 90 days", "recent", -90, 0),
        ("Next 30 days", "upcoming", 1, 30),
        ("Next 90 days", "upcoming", 1, 90),
        ("Days 91-365 ahead", "upcoming", 91, 365),
    ]
    rows: list[dict[str, object]] = []

    for label, release_bucket, min_days, max_days in windows:
        frame = _slice_release_window(
            release_calendar,
            release_bucket=release_bucket,
            min_days=min_days,
            max_days=max_days,
        )

        standout_title = "n/a"
        standout_metric = "n/a"
        if not frame.empty:
            if release_bucket == "recent":
                standout = frame.sort_values(
                    by=["rating", "metacritic", "released", "game_name"],
                    ascending=[False, False, False, True],
                    na_position="last",
                ).iloc[0]
                standout_title = str(standout["game_name"])
                standout_metric = f"rating {_format_table_value(standout['rating'])}"
            else:
                standout = frame.sort_values(
                    by=["added", "rating", "released", "game_name"],
                    ascending=[False, False, True, True],
                    na_position="last",
                ).iloc[0]
                standout_title = str(standout["game_name"])
                standout_metric = f"RAWG added {_format_table_value(standout['added'])}"

        rows.append(
            {
                "window": label,
                "titles": len(frame),
                "avg_rating": _rounded_mean(frame["rating"]) if not frame.empty else None,
                "avg_metacritic": _rounded_mean(frame["metacritic"]) if not frame.empty else None,
                "standout": standout_title,
                "standout_metric": standout_metric,
            }
        )

    return pd.DataFrame(rows)


def _count_values(values: pd.Series, label: str) -> pd.DataFrame:
    dataframe = (
        values.fillna("Unknown")
        .value_counts()
        .head(5)
        .rename_axis(label)
        .reset_index(name="titles")
    )
    return dataframe


def _split_and_count(values: pd.Series, label: str) -> pd.DataFrame:
    counts: dict[str, int] = {}
    for value in values.dropna():
        for item in [part.strip() for part in str(value).split(",") if part.strip()]:
            counts[item] = counts.get(item, 0) + 1

    if not counts:
        return pd.DataFrame(columns=[label, "titles"])

    return pd.DataFrame(
        [{label: name, "titles": count} for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:5]]
    )


def _format_highlight(value: object, prefix: str) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{prefix}{_format_table_value(value)}"


def _select_report_columns(
    dataframe: pd.DataFrame,
    columns: list[str],
    rename_map: dict[str, str],
) -> pd.DataFrame:
    return dataframe.reindex(columns=columns).rename(columns=rename_map)


def render_release_digest(
    *,
    snapshot_date: date,
    release_calendar: pd.DataFrame,
    top_titles: pd.DataFrame,
) -> str:
    """Render the stakeholder Markdown digest."""

    recent_30 = _slice_release_window(release_calendar, release_bucket="recent", min_days=-30, max_days=0)
    recent_90 = _slice_release_window(release_calendar, release_bucket="recent", min_days=-90, max_days=0)
    upcoming_30 = _slice_release_window(release_calendar, release_bucket="upcoming", min_days=1, max_days=30)
    upcoming_90 = _slice_release_window(release_calendar, release_bucket="upcoming", min_days=1, max_days=90)
    full_window_summary = _build_window_summary(release_calendar)

    top_platforms = _count_values(
        upcoming_90["primary_platform"],
        "platform",
    )
    top_genres = _split_and_count(upcoming_90["genre_names"], "genre")

    top_upcoming = top_titles[top_titles["title_group"] == "upcoming_next_90_most_added"].head(5).copy()
    if not top_upcoming.empty:
        top_upcoming["days_until_release"] = top_upcoming["days_from_snapshot"]

    top_recent = top_titles[top_titles["title_group"] == "recent_last_90_highest_rated"].head(5).copy()
    if not top_recent.empty:
        top_recent["days_since_release"] = top_recent["days_from_snapshot"].abs()

    recent_start = release_calendar["window_start_date"].dropna().min()
    upcoming_end = release_calendar["window_end_date"].dropna().max()
    upcoming_start = snapshot_date + timedelta(days=1)

    recent_highlight = top_recent.iloc[0] if not top_recent.empty else None
    upcoming_highlight = top_upcoming.iloc[0] if not top_upcoming.empty else None

    lines = [
        "# RAWG Game Release Digest",
        "",
        "Source: [RAWG Video Games Database](https://rawg.io/apidocs)",
        "",
        "## Coverage",
        "",
        f"- Snapshot date: **{snapshot_date.isoformat()}**",
        f"- Recent release window: **{_format_table_value(recent_start)}** to **{snapshot_date.isoformat()}**",
        f"- Upcoming release window: **{upcoming_start.isoformat()}** to **{_format_table_value(upcoming_end)}**",
        "",
        "## Near-Term Snapshot",
        "",
        f"- Titles in current snapshot: {_format_metric(len(release_calendar), digits=0)}",
        f"- Releases in last 30 days: {_format_metric(len(recent_30), digits=0)}",
        f"- Releases in last 90 days: {_format_metric(len(recent_90), digits=0)}",
        f"- Releases in next 30 days: {_format_metric(len(upcoming_30), digits=0)}",
        f"- Releases in next 90 days: {_format_metric(len(upcoming_90), digits=0)}",
        (
            f"- Best-rated recent release: {recent_highlight['game_name']} "
            f"({_format_highlight(recent_highlight['rating'], '')})"
            if recent_highlight is not None
            else "- Best-rated recent release: n/a"
        ),
        (
            f"- Most anticipated upcoming release: {upcoming_highlight['game_name']} "
            f"({_format_highlight(upcoming_highlight['added'], 'RAWG added ')})"
            if upcoming_highlight is not None
            else "- Most anticipated upcoming release: n/a"
        ),
        "",
        "## Release Window Summary",
        "",
        _render_markdown_table(full_window_summary),
        "",
        "## Upcoming Releases in Next 90 Days",
        "",
        _render_markdown_table(
            _select_report_columns(
                top_upcoming,
                [
                    "rank_in_group",
                    "game_name",
                    "released",
                    "days_until_release",
                    "primary_platform",
                    "genre_names",
                    "added",
                ],
                {
                    "rank_in_group": "rank",
                    "game_name": "title",
                    "released": "release_date",
                    "days_until_release": "days_until_release",
                    "primary_platform": "platform",
                    "genre_names": "genres",
                    "added": "RAWG added",
                },
            )
        ),
        "",
        "## Recent Release Highlights",
        "",
        _render_markdown_table(
            _select_report_columns(
                top_recent,
                [
                    "rank_in_group",
                    "game_name",
                    "released",
                    "days_since_release",
                    "primary_platform",
                    "genre_names",
                    "rating",
                    "metacritic",
                ],
                {
                    "rank_in_group": "rank",
                    "game_name": "title",
                    "released": "release_date",
                    "days_since_release": "days_since_release",
                    "primary_platform": "platform",
                    "genre_names": "genres",
                },
            )
        ),
        "",
        "## Upcoming Platform Mix (Next 90 Days)",
        "",
        _render_markdown_table(top_platforms),
        "",
        "## Upcoming Genre Mix (Next 90 Days)",
        "",
        _render_markdown_table(top_genres),
        "",
        "## Notes",
        "",
        "- `RAWG added` is the platform interest count RAWG exposes and uses for popularity or anticipation-style sorting.",
        "- The report keeps the one-year recent and one-year upcoming snapshot in the warehouse, but surfaces the next 90 days and last 90 days for faster interpretation.",
        "- Airflow and the package CLI generate the same final report artifacts.",
    ]
    return "\n".join(lines) + "\n"


def export_reports(settings: PipelineSettings, snapshot_date: date) -> ReportArtifacts:
    """Export the final CSV and Markdown outputs."""

    settings.output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
        release_calendar = conn.execute(
            """
            SELECT
                snapshot_date,
                window_start_date,
                window_end_date,
                game_id,
                game_name,
                release_bucket,
                released,
                days_from_snapshot,
                primary_platform,
                platform_names,
                genre_names,
                rating,
                ratings_count,
                metacritic,
                added,
                source_url
            FROM analytics.marts_games__release_calendar
            ORDER BY
                CASE release_bucket
                    WHEN 'upcoming' THEN 1
                    WHEN 'recent' THEN 2
                    ELSE 3
                END,
                released NULLS LAST,
                rating DESC NULLS LAST,
                game_name
            """
        ).df()
        top_titles = conn.execute(
            """
            SELECT
                title_group,
                rank_in_group,
                game_name,
                released,
                days_from_snapshot,
                primary_platform,
                genre_names,
                rating,
                metacritic,
                added
            FROM analytics.marts_games__top_titles
            ORDER BY title_group, rank_in_group
            """
        ).df()

    csv_path = settings.output_dir / f"game_release_calendar_{snapshot_date.isoformat()}.csv"
    markdown_path = settings.output_dir / f"game_release_digest_{snapshot_date.isoformat()}.md"
    release_calendar.to_csv(csv_path, index=False)
    markdown_path.write_text(
        render_release_digest(
            snapshot_date=snapshot_date,
            release_calendar=release_calendar,
            top_titles=top_titles,
        ),
        encoding="utf-8",
    )
    return ReportArtifacts(
        csv_path=csv_path,
        markdown_path=markdown_path,
        rows_exported=len(release_calendar),
    )
