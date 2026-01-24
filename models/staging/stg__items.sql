with source as (

    select * from {{ source('olist', 'olist_items') }}

),

renamed as (

    select *

    from source

)

select * from renamed
