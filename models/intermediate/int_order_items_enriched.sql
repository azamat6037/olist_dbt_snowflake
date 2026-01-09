with items as (
    select * from {{ ref('stg_olist__items') }}
),

products as (
    select * from {{ ref('stg_olist__products') }}
),

sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

enriched as (
    select
        i.order_id,
        i.order_item_id,
        i.product_id,
        i.seller_id,
        i.shipping_limit_date,
        i.price,
        i.freight_value,
        
        -- Freight as percentage of price
        case 
            when i.price > 0 then i.freight_value / i.price 
            else 0 
        end as freight_cost_pct,
        
        -- Product info
        p.product_category_name,
        p.product_category_name_english,
        
        -- Seller info
        s.seller_city,
        s.seller_state

    from items i
    left join products p on i.product_id = p.product_id
    left join sellers s on i.seller_id = s.seller_id
)

select * from enriched
