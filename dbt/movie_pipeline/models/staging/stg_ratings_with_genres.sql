with source as (
    select * from {{ source('raw', 'ratings_with_genres') }}
),

renamed as (
    select
        movieId     as movie_id,
        userId      as user_id,
        rating      as rating,
        title       as title,
        genre       as genre,
        rating_timestamp as rated_at
    from source
)

select * from renamed
