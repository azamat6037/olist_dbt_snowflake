-- Business Test: Fulfillment times should be non-negative
-- Negative times indicate data quality issues or timestamp errors

select
    order_id,
    approval_time_hours,
    processing_time_hours,
    shipping_time_hours
from {{ ref('fct_orders') }}
where 
    approval_time_hours < 0
    or processing_time_hours < 0
    or shipping_time_hours < 0
