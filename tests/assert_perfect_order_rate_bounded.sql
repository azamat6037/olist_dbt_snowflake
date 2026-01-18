-- Business Test: Perfect order rate must be between 0 and 1
-- This validates our North Star Metric calculation

select
    metric_month,
    perfect_order_rate
from {{ ref('agg_executive__company_metrics') }}
where perfect_order_rate < 0 or perfect_order_rate > 1
