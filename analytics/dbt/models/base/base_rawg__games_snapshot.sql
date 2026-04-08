with latest_snapshot as (
    select max(snapshot_date) as snapshot_date
    from {{ source('raw', 'rawg_games_snapshot') }}
)

select
    snapshot_date,
    window_start_date,
    window_end_date,
    run_id,
    segment,
    page_number,
    game_id,
    slug,
    name as game_name,
    case
        when slug is not null then concat('https://rawg.io/games/', slug)
        else null
    end as source_url,
    cast(released as date) as released,
    coalesce(tba, false) as is_tba,
    cast(updated_at as timestamp) as updated_at,
    cast(rating as double) as rating,
    cast(ratings_count as integer) as ratings_count,
    cast(metacritic as integer) as metacritic,
    cast(added as integer) as added,
    coalesce(platforms_json, '[]') as platforms_json,
    coalesce(genres_json, '[]') as genres_json,
    coalesce(stores_json, '[]') as stores_json,
    esrb_rating_json,
    nullif(json_extract_string(esrb_rating_json, '$.name'), '') as esrb_rating_name,
    background_image_url
from {{ source('raw', 'rawg_games_snapshot') }}
where snapshot_date = (select snapshot_date from latest_snapshot)
