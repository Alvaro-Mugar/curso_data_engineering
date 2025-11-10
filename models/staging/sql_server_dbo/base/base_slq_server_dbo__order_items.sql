{{ config(
  materialized = 'incremental',
  unique_key = 'order_item_id'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'order_items') }}

{% if is_incremental() %}

    WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

{% endif %}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key (['order_id', 'product_id']) }} as order_item_id,
    {{ dbt_utils.generate_surrogate_key (['order_id']) }} as order_id,
    {{ dbt_utils.generate_surrogate_key (['product_id']) }} as product_id,
    order_id as order_uuid,
    product_id as product_uuid,
    cast(quantity as integer) as quantity,
    _fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed