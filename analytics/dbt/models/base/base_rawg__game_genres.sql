with games as (
    select *
    from {{ ref('base_rawg__games_snapshot') }}
)

select
    games.snapshot_date,
    games.game_id,
    cast(json_extract(genre.value, '$.id') as bigint) as genre_id,
    nullif(json_extract_string(genre.value, '$.slug'), '') as genre_slug,
    nullif(json_extract_string(genre.value, '$.name'), '') as genre_name
from games,
    json_each(games.genres_json) as genre
where nullif(json_extract_string(genre.value, '$.name'), '') is not null
