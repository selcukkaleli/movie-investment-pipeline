with source as (
    select * from {{ ref('int_genre_ratings') }}
),

aggregated as (
    select
        genre,
        sum(rating_count)                                       as total_ratings,
        round(avg(avg_rating), 2)                               as avg_rating,
        sum(movie_count)                                        as total_movies
    from source
    where genre != '(no genres listed)'
    group by genre
)

select * from aggregated
order by total_ratings desc