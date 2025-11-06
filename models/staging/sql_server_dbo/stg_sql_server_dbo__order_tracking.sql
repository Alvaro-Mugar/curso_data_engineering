{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ ref('stg_sql_server_dbo__orders') }}
),

transformed as (
  select
    md5(order_tracking_id) as order_tracking_id,
    md5(order_id) as order_id,
    nullif(trim(tracking_id), '') as tracking_id,
    convert_timezone('UTC', created_at) as created_at_utc,
    convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_at_utc,
    convert_timezone('UTC', delivered_at) as delivered_at_utc,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed