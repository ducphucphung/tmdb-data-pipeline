with ranked_person as (

    select
        *,
        row_number() over (
            partition by person_id
            order by person_popularity desc nulls last, person_name
        ) as rn
    from {{ ref('stg_dim_person') }}

)

select
    person_id,
    person_name,
    original_name,
    gender,
    known_for_department,
    profile_path,
    person_popularity,
    adult
from ranked_person
where rn = 1