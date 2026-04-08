"""Report generation for final stakeholder-facing outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
        ["" if pd.isna(value) else str(value) for value in row]
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


def render_release_digest(
    *,
    snapshot_date: date,
    release_calendar: pd.DataFrame,
    monthly_trends: pd.DataFrame,
    top_titles: pd.DataFrame,
) -> str:
    """Render the stakeholder Markdown digest."""

    recent_titles = release_calendar[release_calendar["release_bucket"] == "recent"]
    upcoming_titles = release_calendar[release_calendar["release_bucket"] == "upcoming"]
    rated_recent = recent_titles[recent_titles["rating"].fillna(0) > 0]

    top_platforms = (
        release_calendar["primary_platform"]
        .fillna("Unknown")
        .value_counts()
        .head(5)
        .rename_axis("platform")
        .reset_index(name="titles")
    )

    genre_counts: dict[str, int] = {}
    for value in release_calendar["genre_names"].dropna():
        for genre in [item.strip() for item in value.split(",") if item.strip()]:
            genre_counts[genre] = genre_counts.get(genre, 0) + 1
    top_genres = (
        pd.DataFrame(
            [{"genre": genre, "titles": count} for genre, count in sorted(genre_counts.items(), key=lambda item: (-item[1], item[0]))[:5]]
        )
        if genre_counts
        else pd.DataFrame(columns=["genre", "titles"])
    )

    top_upcoming = top_titles[top_titles["title_group"] == "upcoming_most_anticipated"].head(5)
    top_recent = top_titles[top_titles["title_group"] == "recent_highest_rated"].head(5)
    trend_excerpt = monthly_trends.head(12)

    lines = [
        "# RAWG Game Release Digest",
        "",
        f"Snapshot date: **{snapshot_date.isoformat()}**",
        "",
        "Source: [RAWG Video Games Database](https://rawg.io/apidocs)",
        "",
        "## Headline KPIs",
        "",
        f"- Titles in current snapshot: {_format_metric(len(release_calendar), digits=0)}",
        f"- Upcoming titles: {_format_metric(len(upcoming_titles), digits=0)}",
        f"- Recent releases: {_format_metric(len(recent_titles), digits=0)}",
        f"- Average recent rating: {_format_metric(rated_recent['rating'].mean())}",
        f"- Average recent Metacritic: {_format_metric(recent_titles['metacritic'].dropna().mean())}",
        "",
        "## Top Upcoming Releases",
        "",
        _render_markdown_table(
            top_upcoming.loc[
                :,
                ["rank_in_group", "game_name", "released", "primary_platform", "genre_names", "added"],
            ].rename(
                columns={
                    "rank_in_group": "rank",
                    "game_name": "title",
                    "released": "release_date",
                    "primary_platform": "platform",
                    "genre_names": "genres",
                    "added": "added_score",
                }
            )
        ),
        "",
        "## Highest Rated Recent Releases",
        "",
        _render_markdown_table(
            top_recent.loc[
                :,
                ["rank_in_group", "game_name", "released", "primary_platform", "genre_names", "rating", "metacritic"],
            ].rename(
                columns={
                    "rank_in_group": "rank",
                    "game_name": "title",
                    "released": "release_date",
                    "primary_platform": "platform",
                    "genre_names": "genres",
                }
            )
        ),
        "",
        "## Monthly Release Trends",
        "",
        _render_markdown_table(trend_excerpt),
        "",
        "## Platform Mix",
        "",
        _render_markdown_table(top_platforms),
        "",
        "## Genre Mix",
        "",
        _render_markdown_table(top_genres),
        "",
        "## Notes",
        "",
        "- Airflow and the local CLI both generate these same report artifacts.",
        "- CI uses fixture-backed RAWG responses so automated runs do not rely on live API access.",
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
        monthly_trends = conn.execute(
            """
            SELECT
                release_month,
                primary_platform,
                total_titles,
                upcoming_titles,
                recent_titles,
                avg_recent_rating,
                avg_metacritic
            FROM analytics.marts_games__monthly_trends
            ORDER BY release_month, total_titles DESC, primary_platform
            """
        ).df()
        top_titles = conn.execute(
            """
            SELECT
                title_group,
                rank_in_group,
                game_name,
                released,
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
            monthly_trends=monthly_trends,
            top_titles=top_titles,
        ),
        encoding="utf-8",
    )
    return ReportArtifacts(
        csv_path=csv_path,
        markdown_path=markdown_path,
        rows_exported=len(release_calendar),
    )
