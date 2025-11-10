{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'orders') }}

    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

    {% endif %}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key (['order_id']) }} as order_id,
    order_id as order_uuid,
    {{ dbt_utils.generate_surrogate_key (['shipping_service']) }} as shipping_service_id,
    cast(shipping_cost as float) as shipping_cost,
    {{ dbt_utils.generate_surrogate_key (['address_id']) }} as address_id,
    convert_timezone('UTC', created_at) as created_at_utc,
    {{ dbt_utils.generate_surrogate_key ([ "coalesce(promo_id, 'no_promo')" ]) }} as promo_id,
    convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_at_utc,
    cast(order_cost as float) as order_cost,
    {{ dbt_utils.generate_surrogate_key (['user_id']) }} as user_id,
    cast(order_total as float) as order_total,
    convert_timezone('UTC', delivered_at) as delivered_at_utc,
    nullif(trim(tracking_id), '') as tracking_id,
    {{ dbt_utils.generate_surrogate_key (['status']) }} as order_status_id,
    _fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
  union all
  select
    {{ dbt_utils.generate_surrogate_key ("'no_order_id'") }} as order_id,
    'order-0000' as order_uuid,
    {{ dbt_utils.generate_surrogate_key (['0000-0000']) }} as shipping_service_id,
    0 as shipping_cost,
    {{ dbt_utils.generate_surrogate_key (["'address_0'"]) }} as address_id,
    convert_timezone('UTC', current_timestamp) as created_at_utc,
    {{ dbt_utils.generate_surrogate_key (["'no_promo'"]) }} as promo_id,
    convert_timezone('UTC', current_timestamp) as estimated_delivery_at_utc,
    0 as order_cost,
    {{ dbt_utils.generate_surrogate_key (["'user_0'"]) }} as user_id,
    0 as order_total,
    convert_timezone('UTC', current_timestamp) as delivered_at_utc,
    '0-0' as tracking_id,
    {{ dbt_utils.generate_surrogate_key (["'0000-0000'"]) }} as order_status_id,
    null as _fivetran_deleted,
    convert_timezone('UTC', current_timestamp) as synced_utc
)

select * from transformed