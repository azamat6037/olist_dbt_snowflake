{{
    config(
        materialized='table'
    )
}}

with sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        
        -- Natural key
        seller_id,
        
        -- Attributes
        seller_zip_code_prefix,
        seller_city,
        seller_state
        
    from sellers
)

select * from final
