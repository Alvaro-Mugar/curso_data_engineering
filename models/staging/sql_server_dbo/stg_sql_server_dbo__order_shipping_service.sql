{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ ref('stg_sql_server_dbo__orders') }}
),

transformed as (
  select
    md5(shipping_service_id) as shipping_service_id,
    md5(order_id) as order_id,
    nullif(trim(shipping_service), '') as shipping_service,
    nullif(trim(speed_category), '') as speed_category,
    cast(shipping_cost as float) as shipping_cost,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed