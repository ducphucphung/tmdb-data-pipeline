with latest_crew as (

    select *
    from {{ ref('stg_movie_crew') }}
    qualify row_number() over (
        partition by movie_id, person_id, credit_id
        order by snapshot_date desc
    ) = 1

)

select
    movie_id,
    person_id,
    department,
    job,
    credit_id
from latest_crew
where movie_id is not null
  and person_id is not null