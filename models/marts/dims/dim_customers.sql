{{
    config(
        materialized='table'
    )
}}

{#
    Dimension: Customers
    
    This dimension contains one row per unique customer, with their
    attributes and order history metrics. Uses intermediate models
    for deduplication and history calculation.
#}

with customers_deduped as (
    select * from {{ ref('int_customers__deduplicated') }}
),

customer_history as (
    select * from {{ ref('int_customers__order_history') }}
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
        
        -- Order history metrics
        ch.first_order_date,
        ch.last_order_date,
        coalesce(ch.total_orders, 0) as total_orders,
        coalesce(ch.lifetime_value, 0) as lifetime_value,
        coalesce(ch.avg_order_value, 0) as avg_order_value,
        
        -- Derived flags
        case when coalesce(ch.total_orders, 0) > 1 then true else false end as is_repeat_buyer
        
    from customers_deduped c
    left join customer_history ch on c.customer_unique_id = ch.customer_unique_id
)

select * from final
