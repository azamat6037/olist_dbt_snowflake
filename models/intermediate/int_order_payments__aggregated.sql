{{
    config(
        materialized='view'
    )
}}

with payments as (
    select * from {{ ref('stg_olist__payments') }}
),

order_payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        count(distinct payment_type) as payment_types_count,
        max(payment_installments) as payment_installments_max,
        max_by(payment_type, payment_value) as primary_payment_type,
        count(*) as payment_transactions_count
    from payments
    group by 1
)

select * from order_payments
