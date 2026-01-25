{{
    config(
        materialized='view'
    )
}}

with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

items as (
    select * from {{ ref('stg_olist__items') }}
),

order_values as (
    select
        order_id,
        sum(price) + sum(freight_value) as order_total_value
    from items
    group by 1
),

customer_order_history as (
    select
        c.customer_unique_id,
        min(o.order_purchase_timestamp) as first_order_date,
        max(o.order_purchase_timestamp) as last_order_date,
        count(distinct o.order_id) as total_orders,
        coalesce(sum(ov.order_total_value), 0) as lifetime_value,
        coalesce(avg(ov.order_total_value), 0) as avg_order_value,
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
