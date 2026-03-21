with source as (
    select * from {{ ref('int_genre_ratings') }}
)

select
    genre,
    rating_year,
    rating_count,
    avg_rating,
    movie_count,
    user_count
from source
where genre != '(no genres listed)'
order by genre, rating_year