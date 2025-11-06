{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'order_items') }}
),

transformed as (
  select
    md5(coalesce(order_id, '') || '|' || coalesce(product_id, '')) as order_item_id,
    md5(order_id) as order_id,
    md5(product_id) as product_id,
    order_id as order_uuid,
    product_id as product_uuid,
    cast(quantity as integer) as quantity,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed