select
    release_month,
    primary_platform,
    total_titles,
    upcoming_titles,
    recent_titles,
    avg_recent_rating,
    avg_metacritic
from {{ ref('int_games__monthly_platform_releases') }}
