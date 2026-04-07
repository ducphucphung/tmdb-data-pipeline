select
    cast(person_id as int64) as person_id,
    person_name,
    original_name,
    cast(gender as int64) as gender,
    known_for_department,
    profile_path,
    cast(person_popularity as float64) as person_popularity,
    cast(adult as bool) as adult
from {{ source('tmdb_silver', 'dim_person') }}
where person_id is not null