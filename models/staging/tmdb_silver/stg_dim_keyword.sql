select
    cast(keyword_id as int64) as keyword_id,
    keyword_name
from {{ source('tmdb_silver', 'dim_keyword') }}
where keyword_id is not null