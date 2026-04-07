select distinct
    company_id,
    company_name,
    company_origin_country,
    company_logo_path
from {{ ref('stg_dim_company') }}
where company_id is not null