{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'events') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key (['event_id']) }} as event_id,
    event_id as event_uuid,
    trim(page_url) as page_url,
    {{ dbt_utils.generate_surrogate_key (['event_type']) }} as event_type_id,
    {{ dbt_utils.generate_surrogate_key (['user_id']) }} as user_id,
    {{ dbt_utils.generate_surrogate_key([ "coalesce(product_id, 'no_product_id')" ]) }} as product_id,
    {{ dbt_utils.generate_surrogate_key (['session_id']) }} as session_id,
    convert_timezone('UTC', created_at) as created_at_utc,
    {{ dbt_utils.generate_surrogate_key (["coalesce(order_id, 'no_order_id')"]) }} as order_id,
    _fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed