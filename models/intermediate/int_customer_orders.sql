with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

customer_order_history as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.order_purchase_timestamp,
        
        -- Order sequence for this customer
        row_number() over (
            partition by c.customer_unique_id 
            order by o.order_purchase_timestamp
        ) as customer_order_sequence,
        
        -- Total orders for this customer (for repeat buyer identification)
        count(*) over (
            partition by c.customer_unique_id
        ) as customer_total_orders

    from orders o
    inner join customers c on o.customer_id = c.customer_id
),

final as (
    select
        *,
        case when customer_order_sequence > 1 then true else false end as is_repeat_order,
        case when customer_total_orders > 1 then true else false end as is_repeat_buyer
    from customer_order_history
)

select * from final
