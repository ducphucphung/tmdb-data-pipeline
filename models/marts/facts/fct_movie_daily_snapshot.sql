select
    movie_id,
    snapshot_date,
    popularity,
    vote_average,
    vote_count,
    revenue,
    runtime,
    status
from {{ ref('stg_fact_movie_daily_snapshot') }}