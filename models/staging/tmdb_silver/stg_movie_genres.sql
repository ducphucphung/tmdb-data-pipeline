select
    cast(movie_id as int64) as movie_id,
    cast(snapshot_date as date) as snapshot_date,
    cast(genre_id as int64) as genre_id,
    genre_name
from {{ source('tmdb_silver', 'movie_genres') }}