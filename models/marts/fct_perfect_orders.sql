{{
    config(
        materialized='table'
    )
}}

with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

-- Daily aggregation of Perfect Orders
daily_metrics as (
    select
        date_trunc('day', order_purchase_timestamp)::date as order_date,
        
        count(*) as total_orders,
        sum(case when is_delivered then 1 else 0 end) as delivered_orders,
        sum(case when is_on_time then 1 else 0 end) as on_time_orders,
        sum(case when has_good_review then 1 else 0 end) as good_review_orders,
        sum(case when is_perfect_order then 1 else 0 end) as perfect_orders

    from orders_enriched
    group by 1
),

final as (
    select
        order_date,
        total_orders,
        delivered_orders,
        on_time_orders,
        good_review_orders,
        perfect_orders,
        
        -- Rates
        round(delivered_orders / nullif(total_orders, 0), 4) as delivery_rate,
        round(on_time_orders / nullif(delivered_orders, 0), 4) as on_time_rate,
        round(good_review_orders / nullif(total_orders, 0), 4) as good_review_rate,
        round(perfect_orders / nullif(total_orders, 0), 4) as perfect_order_rate

    from daily_metrics
)

select * from final
order by order_date
