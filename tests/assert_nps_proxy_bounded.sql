-- Business Test: NPS Proxy must be between -1 and 1
-- (-1 = all 1-star, +1 = all 5-star)

select
    order_month,
    nps_proxy
from {{ ref('fct_customer_satisfaction') }}
where nps_proxy < -1 or nps_proxy > 1
