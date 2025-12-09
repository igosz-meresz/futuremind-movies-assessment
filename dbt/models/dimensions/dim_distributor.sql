-- Distributor dimension
with distributors as (
    select distinct
        distributor
    from {{ ref('stg_revenues') }}
    where distributor is not null
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['distributor']) }} as distributor_key,
        distributor as distributor_name,
        -- Flag major studios
        case
            when distributor in (
                'Walt Disney Studios Motion Pictures',
                'Warner Bros.',
                'Universal Pictures',
                'Paramount Pictures',
                'Sony Pictures Entertainment (SPE)',
                'Twentieth Century Fox',
                '20th Century Studios',
                'Lionsgate',
                'Metro-Goldwyn-Mayer (MGM)'
            ) then true
            else false
        end as is_major_studio
    from distributors
)

select * from enriched