{{
    config(
        materialized='table'
    )
}}

with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

-- Monthly aggregation of satisfaction metrics
monthly_metrics as (
    select
        date_trunc('month', o.order_purchase_timestamp)::date as order_month,
        
        count(*) as total_orders,
        
        -- On-Time Delivery Rate
        sum(case when o.is_on_time then 1 else 0 end) as on_time_orders,
        round(
            sum(case when o.is_on_time then 1 else 0 end) / nullif(count(*), 0), 
            4
        ) as on_time_delivery_rate,
        
        -- Review Score Distribution
        sum(case when o.review_score = 5 then 1 else 0 end) as five_star_reviews,
        sum(case when o.review_score = 1 then 1 else 0 end) as one_star_reviews,
        sum(case when o.review_score is not null then 1 else 0 end) as total_reviews,
        
        -- NPS Proxy (% 5-star minus % 1-star)
        round(
            (sum(case when o.review_score = 5 then 1 else 0 end) - 
             sum(case when o.review_score = 1 then 1 else 0 end)) / 
            nullif(sum(case when o.review_score is not null then 1 else 0 end), 0),
            4
        ) as nps_proxy,
        
        -- Average Review Score
        round(avg(o.review_score), 2) as avg_review_score

    from orders_enriched o
    where o.order_status = 'delivered'
    group by 1
),

-- Repeat buyer metrics (calculated separately for accuracy)
repeat_metrics as (
    select
        date_trunc('month', o.order_purchase_timestamp)::date as order_month,
        count(*) as total_orders,
        sum(case when c.is_repeat_order then 1 else 0 end) as repeat_orders,
        round(
            sum(case when c.is_repeat_order then 1 else 0 end) / nullif(count(*), 0),
            4
        ) as repeat_buyer_rate
    from orders_enriched o
    inner join customer_orders c on o.order_id = c.order_id
    group by 1
),

final as (
    select
        m.order_month,
        m.total_orders,
        m.on_time_delivery_rate,
        m.nps_proxy,
        m.avg_review_score,
        m.five_star_reviews,
        m.one_star_reviews,
        m.total_reviews,
        r.repeat_buyer_rate
    from monthly_metrics m
    left join repeat_metrics r on m.order_month = r.order_month
)

select * from final
order by order_month
