with ranked_platforms as (
    select
        snapshot_date,
        game_id,
        platform_name,
        row_number() over (
            partition by snapshot_date, game_id
            order by platform_name
        ) as platform_rank
    from {{ ref('base_rawg__game_platforms') }}
)

select
    snapshot_date,
    game_id,
    max(case when platform_rank = 1 then platform_name end) as primary_platform,
    string_agg(distinct platform_name, ', ' order by platform_name) as platform_names
from ranked_platforms
group by 1, 2
