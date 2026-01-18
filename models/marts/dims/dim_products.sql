{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ ref('stg_olist__products') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        
        -- Natural key
        product_id,
        
        -- Attributes
        product_category_name,
        product_category_name_english,
        product_name_length,
        product_description_length,
        product_photos_qty,
        
        -- Physical dimensions
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        
        -- Calculated volume (cm³)
        product_length_cm * product_height_cm * product_width_cm as product_volume_cm3
        
    from products
)

select * from final
