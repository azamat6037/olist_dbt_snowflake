-- Business Test: On-time delivery rate must be between 0 and 1
-- Validates the logical rate calculation

select
    metric_month,
    on_time_delivery_rate
from {{ ref('agg_operations__logistics_metrics') }}
where on_time_delivery_rate < 0 or on_time_delivery_rate > 1
