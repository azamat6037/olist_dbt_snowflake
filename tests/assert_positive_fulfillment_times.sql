-- Business Test: Fulfillment times should be non-negative
-- Negative times indicate data quality issues or timestamp errors

select
    order_month,
    avg_order_approval_hours,
    avg_seller_processing_hours,
    avg_last_mile_hours
from {{ ref('fct_fulfillment_metrics') }}
where 
    avg_order_approval_hours < 0
    or avg_seller_processing_hours < 0
    or avg_last_mile_hours < 0
