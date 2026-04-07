select
    cast(genre_id as int64) as genre_id,
    genre_name
from {{ source('tmdb_silver', 'dim_genre') }}
where genre_id is not null