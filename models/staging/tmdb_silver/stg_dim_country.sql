select
    country_code,
    country_name
from {{ source('tmdb_silver', 'dim_country') }}
where country_code is not null