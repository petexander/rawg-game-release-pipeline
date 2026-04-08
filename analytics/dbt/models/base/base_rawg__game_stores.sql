with games as (
    select *
    from {{ ref('base_rawg__games_snapshot') }}
)

select
    games.snapshot_date,
    games.game_id,
    cast(json_extract(store.value, '$.store.id') as bigint) as store_id,
    nullif(json_extract_string(store.value, '$.store.slug'), '') as store_slug,
    nullif(json_extract_string(store.value, '$.store.name'), '') as store_name
from games,
    json_each(games.stores_json) as store
where nullif(json_extract_string(store.value, '$.store.name'), '') is not null
