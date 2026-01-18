{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

reviews as (
    select * from {{ ref('stg_olist__reviews') }}
),

payments as (
    select * from {{ ref('stg_olist__payments') }}
),

items as (
    select * from {{ ref('stg_olist__items') }}
),

-- Aggregate payments per order
order_payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        count(distinct payment_type) as payment_types_count
    from payments
    group by 1
),

-- Aggregate items per order
order_items_agg as (
    select
        order_id,
        sum(price) as total_items_price,
        sum(freight_value) as total_freight_value,
        count(*) as items_count
    from items
    group by 1
),

-- Build fact table
final as (
    select
        -- Primary key
        o.order_id,
        
        -- Foreign keys
        dc.customer_key,
        cast(to_char(o.order_purchase_timestamp::date, 'YYYYMMDD') as integer) as order_date_key,
        
        -- Natural keys (for convenience)
        o.customer_id,
        c.customer_unique_id,
        
        -- Order attributes
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        
        -- Measures: Revenue
        coalesce(oi.total_items_price, 0) as order_items_value,
        coalesce(oi.total_freight_value, 0) as order_freight_value,
        coalesce(oi.total_items_price, 0) + coalesce(oi.total_freight_value, 0) as order_total_value,
        coalesce(op.total_payment_value, 0) as order_payment_value,
        coalesce(oi.items_count, 0) as order_items_count,
        
        -- Measures: Timing (hours)
        timestampdiff(hour, o.order_purchase_timestamp, o.order_approved_at) as approval_time_hours,
        timestampdiff(hour, o.order_approved_at, o.order_delivered_carrier_date) as processing_time_hours,
        timestampdiff(hour, o.order_delivered_carrier_date, o.order_delivered_customer_date) as shipping_time_hours,
        timestampdiff(day, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_time_days,
        
        -- Review data
        r.review_id,
        r.review_score,
        r.review_creation_date,
        
        -- Delivery flags
        case when o.order_status = 'delivered' then true else false end as is_delivered,
        case when o.order_status = 'canceled' then true else false end as is_canceled,
        case 
            when o.order_delivered_customer_date <= o.order_estimated_delivery_date then true 
            else false 
        end as is_on_time,
        case when r.review_score >= 4 then true else false end as has_good_review,
        
        -- Perfect order flag
        case 
            when o.order_status = 'delivered' 
                and o.order_delivered_customer_date <= o.order_estimated_delivery_date 
                and r.review_score >= 4 
            then true 
            else false 
        end as is_perfect_order

    from orders o
    inner join customers c on o.customer_id = c.customer_id
    left join dim_customers dc on c.customer_unique_id = dc.customer_unique_id
    left join reviews r on o.order_id = r.order_id
    left join order_payments op on o.order_id = op.order_id
    left join order_items_agg oi on o.order_id = oi.order_id
)

select * from final
