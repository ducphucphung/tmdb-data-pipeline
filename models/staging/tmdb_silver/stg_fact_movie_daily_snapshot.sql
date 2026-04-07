select
    cast(movie_id as int64) as movie_id,
    cast(snapshot_date as date) as snapshot_date,
    cast(popularity as float64) as popularity,
    cast(vote_average as float64) as vote_average,
    cast(vote_count as int64) as vote_count,
    cast(revenue as int64) as revenue,
    cast(runtime as int64) as runtime,
    status
from {{ source('tmdb_silver', 'fact_movie_daily_snapshot') }}