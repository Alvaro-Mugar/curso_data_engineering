{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'orders') }}
),

transformed as (
  select
    md5(order_id) as order_id,
    order_id as order_uuid,
    md5(address_id) as address_id,
    md5(coalesce(promo_id, 'no_promo')) as promo_id,
    cast(order_cost as float) as order_cost,
    md5(user_id) as user_id,
    cast(order_total as float) as order_total,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
  union all
  select
    md5('no_order_id') as order_id,
    'order-0000' as order_uuid,
    md5('address_0') as address_id,
    md5('no_promo') as promo_id,
    0 as order_cost,
    md5('user_0') as user_id,
    0 as order_total,
    convert_timezone('UTC', current_timestamp) as synced_utc
)

select * from transformed