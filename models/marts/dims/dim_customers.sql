{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

-- Customer order history for enrichment
customer_stats as (
    select
        c.customer_unique_id,
        min(o.order_purchase_timestamp) as first_order_date,
        max(o.order_purchase_timestamp) as last_order_date,
        count(distinct o.order_id) as total_orders
    from customers c
    inner join orders o on c.customer_id = o.customer_id
    group by 1
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} as customer_key,
        
        -- Natural keys
        c.customer_id,
        c.customer_unique_id,
        
        -- Attributes
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        
        -- Enriched metrics
        cs.first_order_date,
        cs.last_order_date,
        coalesce(cs.total_orders, 0) as total_orders,
        case when coalesce(cs.total_orders, 0) > 1 then true else false end as is_repeat_buyer
        
    from customers c
    left join customer_stats cs on c.customer_unique_id = cs.customer_unique_id
)

select * from final
