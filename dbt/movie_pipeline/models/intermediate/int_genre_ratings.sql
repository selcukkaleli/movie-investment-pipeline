with base as (
    select * from {{ ref('stg_ratings_with_genres') }}
),

aggregated as (
    select
        genre,
        extract(year from rated_at)                                               as rating_year,
        count(distinct concat(cast(user_id as string), '_', cast(movie_id as string))) as rating_count,
        round(avg(rating), 2)                                                     as avg_rating,
        count(distinct movie_id)                                                  as movie_count,
        count(distinct user_id)                                                   as user_count
    from base
    group by genre, rating_year
)

select * from aggregated