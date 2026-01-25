{{
    config(
        materialized='view'
    )
}}

{#
    Intermediate model: Aggregate payment data at the order level.
    
    This model centralizes payment aggregation logic that was previously
    embedded in fct_orders, following dbt best practices for the intermediate layer.
#}

with payments as (
    select * from {{ ref('stg_olist__payments') }}
),

-- Aggregate payments per order
order_payments as (
    select
        order_id,
        
        -- Revenue metrics
        sum(payment_value) as total_payment_value,
        
        -- Payment method metrics
        count(distinct payment_type) as payment_types_count,
        max(payment_installments) as payment_installments_max,
        
        -- Primary payment type (most used by value)
        max_by(payment_type, payment_value) as primary_payment_type,
        
        -- Payment counts
        count(*) as payment_transactions_count

    from payments
    group by 1
)

select * from order_payments
