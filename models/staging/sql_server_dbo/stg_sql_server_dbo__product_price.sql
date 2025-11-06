{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ ref('stg_sql_server_dbo__products')}}
),

transformed as (
  select
    md5(product_price_id) as product_price_id,
    md5(product_id) as product_id,
    cast(price as float) as product_price,
    --convert_timezone('UTC', _fivetran_synced) as valid_from,
    --valid_to timestamp,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed