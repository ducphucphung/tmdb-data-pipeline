select distinct
    country_code,
    country_name
from {{ ref('stg_dim_country') }}
where country_code is not null