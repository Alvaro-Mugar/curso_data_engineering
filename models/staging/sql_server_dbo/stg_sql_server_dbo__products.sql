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
    {{ dbt_utils.generate_surrogate_key(['price']) }} as product_price_id,
    name as product_name,
    {{ dbt_utils.generate_surrogate_key(['inventory']) }} as inventory_id,
    _fivetran_deleted as fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
  union all
  select
    {{ dbt_utils.generate_surrogate_key(["\'no_product_id\'"]) }} as product_id,
    'prod-000' as product_uuid,
    {{ dbt_utils.generate_surrogate_key(["\'000-000\'"]) }} as product_price_id,
    'no name' as product_name,
    {{ dbt_utils.generate_surrogate_key(["\'0000-0000\'"]) }} as inventory_id,
    null as fivetran_deleted,
    convert_timezone('UTC', timestamp '2025-11-06 15:00:00') as synced_utc
)

select * from transformed
