{{
    config(
        materialized='view'
    )
}}

{#
    Intermediate model: Calculate customer-level order history metrics.
    
    This model computes lifetime metrics for each unique customer, including
    order counts, dates, and value calculations. This logic was previously
    embedded in dim_customers.
#}

with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

items as (
    select * from {{ ref('stg_olist__items') }}
),

-- Aggregate order values
order_values as (
    select
        order_id,
        sum(price) + sum(freight_value) as order_total_value
    from items
    group by 1
),

-- Calculate customer order history metrics
customer_order_history as (
    select
        c.customer_unique_id,
        
        -- Order date metrics
        min(o.order_purchase_timestamp) as first_order_date,
        max(o.order_purchase_timestamp) as last_order_date,
        
        -- Order count metrics
        count(distinct o.order_id) as total_orders,
        
        -- Value metrics
        coalesce(sum(ov.order_total_value), 0) as lifetime_value,
        coalesce(avg(ov.order_total_value), 0) as avg_order_value,
        
        -- Time-based metrics
        datediff(
            'day', 
            min(o.order_purchase_timestamp), 
            max(o.order_purchase_timestamp)
        ) as days_between_first_and_last_order

    from customers c
    inner join orders o on c.customer_id = o.customer_id
    left join order_values ov on o.order_id = ov.order_id
    group by 1
)

select * from customer_order_history
