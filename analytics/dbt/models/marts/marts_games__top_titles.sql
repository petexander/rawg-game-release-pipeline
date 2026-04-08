with release_calendar as (
    select *
    from {{ ref('marts_games__release_calendar') }}
),

recent_ranked as (
    select
        'recent_highest_rated' as title_group,
        row_number() over (
            order by rating desc nulls last, metacritic desc nulls last, released desc nulls last, game_name
        ) as rank_in_group,
        snapshot_date,
        game_id,
        game_name,
        released,
        primary_platform,
        genre_names,
        rating,
        metacritic,
        added,
        source_url
    from release_calendar
    where release_bucket = 'recent'
),

upcoming_ranked as (
    select
        'upcoming_most_anticipated' as title_group,
        row_number() over (
            order by added desc nulls last, rating desc nulls last, released asc nulls last, game_name
        ) as rank_in_group,
        snapshot_date,
        game_id,
        game_name,
        released,
        primary_platform,
        genre_names,
        rating,
        metacritic,
        added,
        source_url
    from release_calendar
    where release_bucket = 'upcoming'
)

select *
from recent_ranked
where rank_in_group <= 10

union all

select *
from upcoming_ranked
where rank_in_group <= 10
