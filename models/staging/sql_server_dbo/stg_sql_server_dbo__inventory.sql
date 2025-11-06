{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ ref('stg_sql_server_dbo__products')}}
),

transformed as (
  select
    md5(inventory_id),
    md5(product_id) as product_id,
    cast(inventory as int) as inventory_units
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed
