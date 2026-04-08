with release_calendar as (
    select *
    from analytics.marts_games__release_calendar
)

select
    release_month,
    primary_platform,
    count(*) as titles,
    round(avg(rating), 2) as avg_rating,
    round(avg(metacritic), 2) as avg_metacritic
from release_calendar
where release_bucket in ('recent', 'upcoming')
group by 1, 2
order by release_month, titles desc, primary_platform
