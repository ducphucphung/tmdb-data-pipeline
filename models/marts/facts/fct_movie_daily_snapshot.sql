{{ config(
    materialized='incremental',
    unique_key=['movie_id', 'snapshot_date'],
    incremental_strategy='merge'
) }}

select
    movie_id,
    snapshot_date,
    popularity,
    vote_average,
    vote_count,
    revenue,
    runtime,
    status
from {{ ref('stg_fact_movie_daily_snapshot') }}

{% if is_incremental() %}
where snapshot_date > (
    select coalesce(max(snapshot_date), date('2026-01-01'))
    from {{ this }}
)
{% endif %}