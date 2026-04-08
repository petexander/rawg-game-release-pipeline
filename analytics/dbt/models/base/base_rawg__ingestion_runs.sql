select
    run_id,
    snapshot_date,
    started_at,
    completed_at,
    status,
    window_start_date,
    window_end_date,
    pages_fetched,
    segments_fetched,
    rows_loaded,
    error_message
from {{ source('raw', 'ingestion_runs') }}
