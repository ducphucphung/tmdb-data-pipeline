select
    cast(company_id as int64) as company_id,
    company_name,
    company_origin_country,
    company_logo_path
from {{ source('tmdb_silver', 'dim_company') }}
where company_id is not null