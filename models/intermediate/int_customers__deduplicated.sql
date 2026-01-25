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

customers_with_orders as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp,
        row_number() over (
            partition by c.customer_unique_id 
            order by o.order_purchase_timestamp desc nulls last
        ) as rn
    from customers c
    left join orders o on c.customer_id = o.customer_id
),

customers_deduped as (
    select
        customer_unique_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from customers_with_orders
    where rn = 1
)

select * from customers_deduped
