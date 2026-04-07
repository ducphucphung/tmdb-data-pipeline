select
    cast(movie_id as int64) as movie_id,
    cast(snapshot_date as date) as snapshot_date,
    cast(person_id as int64) as person_id,
    person_name,
    original_name,
    cast(gender as int64) as gender,
    known_for_department,
    department,
    job,
    credit_id,
    profile_path,
    cast(person_popularity as float64) as person_popularity,
    cast(adult as bool) as adult
from {{ source('tmdb_silver', 'movie_crew') }}
where person_id is not null