with games as (
    select *
    from {{ ref('base_rawg__games_snapshot') }}
)

select
    snapshot_date,
    window_start_date,
    window_end_date,
    game_id,
    slug,
    game_name,
    source_url,
    released,
    cast(date_trunc('month', released) as date) as release_month,
    is_tba,
    case
        when released is null then 'tba'
        when released > snapshot_date then 'upcoming'
        else 'recent'
    end as release_bucket,
    case
        when released is null then null
        else datediff('day', snapshot_date, released)
    end as days_from_snapshot,
    rating,
    ratings_count,
    metacritic,
    added,
    esrb_rating_name,
    background_image_url
from games
