with games as (
    select *
    from {{ ref('base_rawg__games_snapshot') }}
)

select
    games.snapshot_date,
    games.game_id,
    cast(json_extract(platform.value, '$.platform.id') as bigint) as platform_id,
    nullif(json_extract_string(platform.value, '$.platform.slug'), '') as platform_slug,
    nullif(json_extract_string(platform.value, '$.platform.name'), '') as platform_name,
    cast(nullif(json_extract_string(platform.value, '$.released_at'), '') as date) as platform_released_at
from games,
    json_each(games.platforms_json) as platform
where nullif(json_extract_string(platform.value, '$.platform.name'), '') is not null
