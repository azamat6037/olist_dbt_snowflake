{{
    config(
        materialized='table'
    )
}}

-- Generate date spine from Olist data range (2016-2018)
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-01-01' as date)"
    ) }}
),

final as (
    select
        -- Date key (integer YYYYMMDD format)
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
        
        -- Date value
        date_day,
        
        -- Day attributes
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,
        dayofmonth(date_day) as day_of_month,
        dayofyear(date_day) as day_of_year,
        
        -- Week attributes
        weekofyear(date_day) as week_of_year,
        date_trunc('week', date_day)::date as week_start_date,
        
        -- Month attributes
        month(date_day) as month_number,
        monthname(date_day) as month_name,
        date_trunc('month', date_day)::date as month_start_date,
        
        -- Quarter attributes
        quarter(date_day) as quarter_number,
        concat('Q', quarter(date_day)) as quarter_name,
        date_trunc('quarter', date_day)::date as quarter_start_date,
        
        -- Year attributes
        year(date_day) as year_number,
        date_trunc('year', date_day)::date as year_start_date,
        
        -- Flags
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
        
    from date_spine
)

select * from final
