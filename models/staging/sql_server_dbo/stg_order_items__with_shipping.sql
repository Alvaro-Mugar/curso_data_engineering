{{ config(
  materialized = 'incremental',
  unique_key = 'order_item_with_shipping_id'
) }}

with order_items as (
  select *
  from {{ ref('base_slq_server_dbo__order_items') }}
  {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
  {% endif %}
),

orders as (
  select *
  from {{ ref('base_sql_server_dbo__orders') }}
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
