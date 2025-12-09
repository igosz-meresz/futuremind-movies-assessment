-- Cleaned daily revenue data
with source as (
    select * from {{ source('raw', 'stg_revenues_raw') }}
),

cleaned as (
    select
        id as revenue_id,
        date as revenue_date,
        title,
        revenue as daily_revenue,
        theaters as theater_count,
        distributor,
        has_valid_theaters,
        has_valid_distributor
    from source
    where revenue > 0  -- Filter out zero-revenue rows for analysis
)

select * from cleaned