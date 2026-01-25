{{
    config(
        materialized='table'
    )
}}

{#
    Fact: Orders
    
    This fact table contains one row per order with all metrics and foreign keys.
    Uses intermediate models for aggregations and enrichments, making this
    model much simpler and focused on assembly rather than computation.
#}

with orders_enriched as (
    select * from {{ ref('int_orders__enriched') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

order_payments as (
    select * from {{ ref('int_order_payments__aggregated') }}
),

order_items as (
    select * from {{ ref('int_order_items__enriched') }}
),

-- Build fact table by joining pre-computed intermediate models
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
        
        -- Measures: Revenue (from intermediate)
        coalesce(oi.total_items_price, 0) as order_items_value,
        coalesce(oi.total_freight_value, 0) as order_freight_value,
        coalesce(oi.total_order_value, 0) as order_total_value,
        coalesce(op.total_payment_value, 0) as order_payment_value,
        coalesce(oi.items_count, 0) as order_items_count,
        
        -- Measures: Additional metrics (from intermediate)
        coalesce(oi.unique_products_count, 0) as unique_products_count,
        coalesce(oi.unique_sellers_count, 0) as unique_sellers_count,
        coalesce(op.payment_types_count, 0) as payment_types_count,
        op.primary_payment_type,
        
        -- Measures: Timing (from intermediate)
        o.approval_time_hours,
        o.processing_time_hours,
        o.shipping_time_hours,
        o.delivery_time_days,
        
        -- Review data (from intermediate)
        o.review_id,
        o.review_score,
        o.review_creation_date,
        
        -- Delivery flags (from intermediate)
        o.is_delivered,
        o.is_canceled,
        o.is_on_time,
        o.has_good_review,
        o.is_perfect_order

    from orders_enriched o
    inner join customers c on o.customer_id = c.customer_id
    left join dim_customers dc on c.customer_unique_id = dc.customer_unique_id
    left join order_payments op on o.order_id = op.order_id
    left join order_items oi on o.order_id = oi.order_id
)

select * from final
