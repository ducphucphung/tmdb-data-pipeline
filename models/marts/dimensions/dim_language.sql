select distinct
    language_code,
    language_english_name,
    language_name
from {{ ref('stg_dim_language') }}
where language_code is not null