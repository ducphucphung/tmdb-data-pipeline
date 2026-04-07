with latest_movie as (
    select *
    from {{ ref('stg_movies') }}
    qualify row_number() over (
        partition by movie_id
        order by snapshot_date desc
    ) = 1
)

select
    movie_id,
    title,
    original_title,
    original_language,
    overview,
    homepage,
    imdb_id,
    release_date,
    runtime,
    status,
    tagline,
    adult,
    video,
    budget,
    poster_path,
    backdrop_path
from latest_movie