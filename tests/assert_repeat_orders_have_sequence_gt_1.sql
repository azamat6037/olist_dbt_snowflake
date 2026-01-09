-- Business Test: Repeat orders must have order sequence > 1
-- Validates the repeat buyer logic

select
    order_id,
    customer_unique_id,
    customer_order_sequence,
    is_repeat_order
from {{ ref('int_customer_orders') }}
where 
    is_repeat_order = true 
    and customer_order_sequence <= 1
