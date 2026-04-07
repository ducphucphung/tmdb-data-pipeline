select distinct
    genre_id,
    genre_name
from {{ ref('stg_dim_genre') }}
where genre_id is not null