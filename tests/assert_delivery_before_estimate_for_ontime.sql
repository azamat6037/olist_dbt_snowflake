-- Business Test: Orders flagged as on-time must have delivery <= estimate
-- Validates the is_on_time flag logic

select
    order_id,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    is_on_time
from {{ ref('fct_orders') }}
where 
    is_on_time = true 
    and order_delivered_customer_date > order_estimated_delivery_date
