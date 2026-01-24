with source as (

    select * from {{ source('olist', 'olist_orders') }}

),

renamed as (

    select *

    from source

)

select * from renamed
