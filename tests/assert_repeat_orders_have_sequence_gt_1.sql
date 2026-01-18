-- Business Test: Repeat buyers must have total orders > 1
-- Validates the repeat buyer logic in dim_customers

select
    customer_unique_id,
    total_orders,
    is_repeat_buyer
from {{ ref('dim_customers') }}
where 
    is_repeat_buyer = true 
    and total_orders <= 1
