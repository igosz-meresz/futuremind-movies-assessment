-- Date dimension spanning the full revenue date range
with date_spine as (
    select
        date
    from unnest(
        generate_date_array('2000-01-01', '2023-12-31', interval 1 day)
    ) as date
),

enriched as (
    select
        -- Surrogate key as integer YYYYMMDD
        cast(format_date('%Y%m%d', date) as int64) as date_key,
        date as full_date,
        extract(dayofweek from date) as day_of_week,
        format_date('%A', date) as day_of_week_name,
        extract(day from date) as day_of_month,
        extract(week from date) as week_of_year,
        extract(month from date) as month,
        format_date('%B', date) as month_name,
        extract(quarter from date) as quarter,
        extract(year from date) as year,
        case 
            when extract(dayofweek from date) in (1, 7) then true 
            else false 
        end as is_weekend
    from date_spine
)

select * from enriched