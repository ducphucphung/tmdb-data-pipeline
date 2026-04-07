select
    language_code,
    language_english_name,
    language_name
from {{ source('tmdb_silver', 'dim_language') }}
where language_code is not null