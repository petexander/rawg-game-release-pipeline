with release_cohorts as (
    select *
    from {{ ref('int_games__release_cohorts') }}
    where release_bucket in ('recent', 'upcoming')
        and release_month is not null
),

platforms as (
    select *
    from {{ ref('int_games__platform_summary') }}
)

select
    release_cohorts.release_month,
    coalesce(platforms.primary_platform, 'Unknown') as primary_platform,
    count(*) as total_titles,
    sum(case when release_cohorts.release_bucket = 'upcoming' then 1 else 0 end) as upcoming_titles,
    sum(case when release_cohorts.release_bucket = 'recent' then 1 else 0 end) as recent_titles,
    round(avg(case when release_cohorts.release_bucket = 'recent' and release_cohorts.rating > 0 then release_cohorts.rating end), 2) as avg_recent_rating,
    round(avg(release_cohorts.metacritic), 2) as avg_metacritic
from release_cohorts
left join platforms
    on release_cohorts.snapshot_date = platforms.snapshot_date
    and release_cohorts.game_id = platforms.game_id
group by 1, 2
