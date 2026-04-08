select
    snapshot_date,
    game_id,
    string_agg(distinct genre_name, ', ' order by genre_name) as genre_names
from {{ ref('base_rawg__game_genres') }}
group by 1, 2
