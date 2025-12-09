-- Cleaned movie metadata from OMDb
with source as (
    select * from {{ source('raw', 'stg_movies_enriched') }}
),

cleaned as (
    select
        title,
        year as year_released,
        rated,
        released,
        runtime,
        genre,
        director,
        actors,
        plot,
        language,
        country,
        awards,
        poster_url,
        metascore,
        imdb_rating,
        imdb_votes,
        imdb_id,
        box_office as box_office_reported,
        enriched_at,
        api_response_type,
        -- Flag for whether we have complete data
        case 
            when imdb_rating is not null then true 
            else false 
        end as has_rating,
        row_number() over (partition by title order by enriched_at desc) as row_num
    from source
    where api_response_type = 'match'  -- Only matched movies
)

select * from cleaned
where row_num = 1