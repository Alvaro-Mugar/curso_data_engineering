{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'events') }}
),

transformed as (
  select
    md5(event_id) as event_id,
    event_id as event_uuid,
    trim(page_url) as page_url,
    lower(trim(event_type)) as event_type,
    md5(user_id) as user_id,
    md5(coalesce(product_id, 'no_product_id')) as product_id,
    nullif(trim(session_id), '') as session_id,
    convert_timezone('UTC', created_at) as created_at_utc,
    md5(coalesce(order_id, 'no_order_id')) as order_id,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed