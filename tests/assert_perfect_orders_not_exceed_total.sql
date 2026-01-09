-- Business Test: Perfect orders cannot exceed total orders
-- Validates the logical relationship between metrics

select
    order_date,
    total_orders,
    perfect_orders
from {{ ref('fct_perfect_orders') }}
where perfect_orders > total_orders
