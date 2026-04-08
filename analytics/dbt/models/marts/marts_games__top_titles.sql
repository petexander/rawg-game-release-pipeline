with release_calendar as (
    select *
    from {{ ref('marts_games__release_calendar') }}
),

recent_ranked as (
    select
        'recent_last_90_highest_rated' as title_group,
        row_number() over (
            order by rating desc nulls last, metacritic desc nulls last, released desc nulls last, game_name
        ) as rank_in_group,
        snapshot_date,
        game_id,
        game_name,
        released,
        days_from_snapshot,
        primary_platform,
        genre_names,
        rating,
        metacritic,
        added,
        source_url
    from release_calendar
    where release_bucket = 'recent'
        and days_from_snapshot between -90 and 0
),

upcoming_ranked as (
    select
        'upcoming_next_90_most_added' as title_group,
        row_number() over (
            order by added desc nulls last, rating desc nulls last, released asc nulls last, game_name
        ) as rank_in_group,
        snapshot_date,
        game_id,
        game_name,
        released,
        days_from_snapshot,
        primary_platform,
        genre_names,
        rating,
        metacritic,
        added,
        source_url
    from release_calendar
    where release_bucket = 'upcoming'
        and days_from_snapshot between 1 and 90
)

select *
from recent_ranked
where rank_in_group <= 10

union all

select *
from upcoming_ranked
where rank_in_group <= 10
