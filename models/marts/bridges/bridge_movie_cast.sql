with latest_cast as (

    select *
    from {{ ref('stg_movie_cast') }}
    qualify row_number() over (
        partition by movie_id, person_id, credit_id
        order by snapshot_date desc
    ) = 1

)

select
    movie_id,
    person_id,
    character,
    cast_order,
    credit_id
from latest_cast
where movie_id is not null
  and person_id is not null