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

-- Deduplicate customers: one row per customer_unique_id
-- Pick the most recent customer_id based on order date
customers_with_orders as (
    select
        c.*,
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
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from customers_with_orders
    where rn = 1
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
        
    from customers_deduped c
    left join customer_stats cs on c.customer_unique_id = cs.customer_unique_id
)

select * from final
