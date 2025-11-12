{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'products') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['product_id'])}} as product_id,
    product_id as product_uuid,
    cast(price as float) as product_price,
    name as product_name,
    cast(inventory as int) as inventory_units,
    _fivetran_deleted as fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
  union all
  select
    {{ dbt_utils.generate_surrogate_key(["\'no_product_id\'"]) }} as product_id,
    'prod-000' as product_uuid,
    0 as product_price,
    'no name' as product_name,
    0 as inventory_units,
    null as fivetran_deleted,
    convert_timezone('UTC', timestamp '2025-11-06 15:00:00') as synced_utc
)

select * from transformed
