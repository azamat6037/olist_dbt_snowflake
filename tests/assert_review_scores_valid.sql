-- Business Test: Review scores must be between 1 and 5
-- This ensures data quality from the source system

select
    review_id,
    review_score
from {{ ref('stg_olist__reviews') }}
where review_score < 1 or review_score > 5 or review_score is null
