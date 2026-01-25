{{
    config(
        materialized='view'
    )
}}

{#
    Intermediate model: Enrich orders with review data and delivery timing calculations.
    
    This model centralizes order enrichment logic including timing calculations,
    delivery flags, and review data. This logic was previously embedded in fct_orders.
#}

with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

reviews as (
    select * from {{ ref('stg_olist__reviews') }}
),

-- Deduplicate reviews: keep the most recent review per order
reviews_deduped as (
    select
        order_id,
        review_id,
        review_score,
        review_creation_date,
        review_comment_title,
        review_comment_message,
        row_number() over (
            partition by order_id 
            order by review_creation_date desc nulls last
        ) as rn
    from reviews
),

reviews_single as (
    select
        order_id,
        review_id,
        review_score,
        review_creation_date,
        review_comment_title,
        review_comment_message
    from reviews_deduped
    where rn = 1
),

-- Enrich orders with review data and calculated metrics
enriched_orders as (
    select
        -- Primary key
        o.order_id,
        
        -- Foreign key
        o.customer_id,
        
        -- Order attributes
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        
        -- Review data (deduplicated to single review per order)
        r.review_id,
        r.review_score,
        r.review_creation_date,
        r.review_comment_title,
        r.review_comment_message,
        
        -- Timing metrics (hours)
        timestampdiff(
            hour, 
            o.order_purchase_timestamp, 
            o.order_approved_at
        ) as approval_time_hours,
        
        timestampdiff(
            hour, 
            o.order_approved_at, 
            o.order_delivered_carrier_date
        ) as processing_time_hours,
        
        timestampdiff(
            hour, 
            o.order_delivered_carrier_date, 
            o.order_delivered_customer_date
        ) as shipping_time_hours,
        
        timestampdiff(
            day, 
            o.order_purchase_timestamp, 
            o.order_delivered_customer_date
        ) as delivery_time_days,
        
        -- Delivery flags
        case 
            when o.order_status = 'delivered' then true 
            else false 
        end as is_delivered,
        
        case 
            when o.order_status = 'canceled' then true 
            else false 
        end as is_canceled,
        
        case 
            when o.order_delivered_customer_date <= o.order_estimated_delivery_date then true 
            else false 
        end as is_on_time,
        
        case 
            when r.review_score >= 4 then true 
            else false 
        end as has_good_review,
        
        -- Perfect order flag (delivered + on-time + good review)
        case 
            when o.order_status = 'delivered' 
                and o.order_delivered_customer_date <= o.order_estimated_delivery_date 
                and r.review_score >= 4 
            then true 
            else false 
        end as is_perfect_order

    from orders o
    left join reviews_single r on o.order_id = r.order_id
)

select * from enriched_orders
