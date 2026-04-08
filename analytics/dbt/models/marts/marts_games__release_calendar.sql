with release_cohorts as (
    select *
    from {{ ref('int_games__release_cohorts') }}
),

platform_summary as (
    select *
    from {{ ref('int_games__platform_summary') }}
),

genre_summary as (
    select *
    from {{ ref('int_games__genre_summary') }}
)

select
    release_cohorts.snapshot_date,
    release_cohorts.window_start_date,
    release_cohorts.window_end_date,
    release_cohorts.game_id,
    release_cohorts.slug,
    release_cohorts.game_name,
    release_cohorts.source_url,
    release_cohorts.released,
    release_cohorts.release_month,
    release_cohorts.release_bucket,
    release_cohorts.days_from_snapshot,
    coalesce(platform_summary.primary_platform, 'Unknown') as primary_platform,
    platform_summary.platform_names,
    genre_summary.genre_names,
    release_cohorts.rating,
    release_cohorts.ratings_count,
    release_cohorts.metacritic,
    release_cohorts.added,
    release_cohorts.esrb_rating_name,
    release_cohorts.background_image_url
from release_cohorts
left join platform_summary
    on release_cohorts.snapshot_date = platform_summary.snapshot_date
    and release_cohorts.game_id = platform_summary.game_id
left join genre_summary
    on release_cohorts.snapshot_date = genre_summary.snapshot_date
    and release_cohorts.game_id = genre_summary.game_id
