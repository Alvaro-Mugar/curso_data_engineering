{{ config(
    materialized = 'view'
) }}

with source as (
    select * 
    from {{ source('sql_server_dbo', 'promos') }}
),

transformed as (
    select
        md5(promo_id) as promo_id,         
        promo_id as promotion_name,            
        cast(discount as integer) as discount_dollars,
        lower(status) as promo_status,
        convert_timezone('UTC', _fivetran_synced) as synced_utc
    from source
    union all
    select
        md5('no_promo') as promo_id,
        'no_promo' as promotion_name,
        0 as discount_dollars,
        'inactive' as promo_status,
        convert_timezone('UTC', current_timestamp) as synced_utc
)

select * from transformed