with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

reviews as (
    select * from {{ ref('stg_olist__reviews') }}
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        
        -- Review data
        r.review_id,
        r.review_score,
        
        -- Delivery timing calculations (in hours)
        timestampdiff(hour, o.order_purchase_timestamp, o.order_approved_at) as order_approval_hours,
        timestampdiff(hour, o.order_approved_at, o.order_delivered_carrier_date) as seller_processing_hours,
        timestampdiff(hour, o.order_delivered_carrier_date, o.order_delivered_customer_date) as last_mile_hours,
        
        -- Perfect Order flags
        case when o.order_status = 'delivered' then true else false end as is_delivered,
        case 
            when o.order_delivered_customer_date <= o.order_estimated_delivery_date then true 
            else false 
        end as is_on_time,
        case when r.review_score >= 4 then true else false end as has_good_review,
        
        -- North Star: Perfect Order
        case 
            when o.order_status = 'delivered' 
                and o.order_delivered_customer_date <= o.order_estimated_delivery_date 
                and r.review_score >= 4 
            then true 
            else false 
        end as is_perfect_order

    from orders o
    left join reviews r on o.order_id = r.order_id
)

select * from enriched
