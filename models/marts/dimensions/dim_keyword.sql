select distinct
    keyword_id,
    keyword_name
from {{ ref('stg_dim_keyword') }}
where keyword_id is not null