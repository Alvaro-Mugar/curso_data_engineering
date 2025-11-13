{{ config(
  materialized = 'incremental',
  unique_key = 'order_item_with_shipping_id'
) }}

with order_items as (
  select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_item_id,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id,
    order_id as order_uuid,
    cast(quantity as integer) as quantity,
    _fivetran_synced
  from {{ source('sql_server_dbo', 'order_items') }}
  {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
  {% endif %}
),

orders as (
  select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_id,
    order_id as order_uuid,
    {{ dbt_utils.generate_surrogate_key(["coalesce(promo_id, 'no_promo')"]) }} as promo_id,
    cast(shipping_cost as float) as shipping_cost,
    _fivetran_synced
  from {{ source('sql_server_dbo', 'orders') }}
  {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
  {% endif %}
),

order_totals as (
  select
    order_id,
    sum(quantity) as total_quantity
  from order_items
  group by order_id
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['oi.order_item_id', 'o.order_id']) }} as order_item_with_shipping_id,
    oi.order_item_id,
    o.order_id,
    o.promo_id,
    o.shipping_cost,
    round(o.shipping_cost / nullif(ot.total_quantity, 0), 2) as shipping_cost_per_product
  from order_items oi
  left join orders o on oi.order_id = o.order_id
  left join order_totals ot on oi.order_id = ot.order_id
)

select * from final
