-- Fact table: daily movie revenue with dimension keys
with revenues as (
    select * from {{ ref('stg_revenues') }}
),

dim_movie as (
    select movie_key, title from {{ ref('dim_movie') }}
),

dim_distributor as (
    select distributor_key, distributor_name from {{ ref('dim_distributor') }}
),

dim_date as (
    select date_key, full_date from {{ ref('dim_date') }}
),

fact as (
    select
        r.revenue_id,
        d.date_key,
        m.movie_key,
        dist.distributor_key,
        r.daily_revenue,
        r.theater_count,
        -- Derived metric: revenue per theater
        case 
            when r.theater_count > 0 
            then round(r.daily_revenue / r.theater_count, 2)
            else null 
        end as revenue_per_theater
    from revenues r
    left join dim_date d on r.revenue_date = d.full_date
    left join dim_movie m on r.title = m.title
    left join dim_distributor dist on r.distributor = dist.distributor_name
)

select * from fact