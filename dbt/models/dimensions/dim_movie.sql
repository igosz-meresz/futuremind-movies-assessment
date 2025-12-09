-- Movie dimension combining revenue titles with OMDb enrichment
with revenue_movies as (
    -- Get all unique movies from revenue data
    select distinct title
    from {{ ref('stg_revenues') }}
),

enriched_movies as (
    select * from {{ ref('stg_movies') }}
),

combined as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['rm.title']) }} as movie_key,
        rm.title,
        em.year_released,
        em.rated,
        em.runtime,
        em.genre,
        em.director,
        em.actors,
        em.plot,
        em.language,
        em.country,
        em.imdb_rating,
        em.imdb_votes,
        em.imdb_id,
        em.metascore,
        em.box_office_reported,
        case 
            when em.title is not null then true 
            else false 
        end as is_enriched,
        em.enriched_at
    from revenue_movies rm
    left join enriched_movies em on rm.title = em.title
)

select * from combined