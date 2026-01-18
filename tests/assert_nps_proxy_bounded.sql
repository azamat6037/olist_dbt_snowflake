-- Business Test: CSAT score must be between 1 and 5
-- Validates the customer satisfaction score calculation

select
    metric_month,
    customer_satisfaction_score
from {{ ref('agg_cx__satisfaction_metrics') }}
where customer_satisfaction_score < 1 or customer_satisfaction_score > 5
