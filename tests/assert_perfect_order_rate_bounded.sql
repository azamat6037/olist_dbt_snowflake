-- Business Test: Perfect order rate must be between 0 and 1
-- This validates our North Star Metric calculation

select
    order_date,
    perfect_order_rate
from {{ ref('fct_perfect_orders') }}
where perfect_order_rate < 0 or perfect_order_rate > 1
