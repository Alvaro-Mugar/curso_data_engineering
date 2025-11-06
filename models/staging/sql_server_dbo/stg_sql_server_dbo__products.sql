{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'products') }}
),

transformed as (
  select
    md5(product_id) as product_id,
    product_id as product_uuid,
    name as product_name,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
  union all
  select
    md5('no_product_id') as product_id,
    'prod-000' as product_uuid,
    'no name' as product_name,
    convert_timezone('UTC', timestamp '2025-11-06 15:00:00') as synced_utc
)

select * from transformed
