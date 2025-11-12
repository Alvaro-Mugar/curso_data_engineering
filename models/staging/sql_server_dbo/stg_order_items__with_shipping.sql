{{ config(
    materialized = 'incremental'
) }}


with order_items as (
    select
        order_id,
        product_id,
        quantity
    from {{ ref('base_slq_server_dbo__order_items') }}
),
orders as (
    select
        order_id,
        shipping_cost
    from {{ ref('base_sql_server_dbo__orders') }}

    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

    {% endif %}
),
item_counts as (
    select
        order_id,
        count(*) as num_items
    from order_items
    group by order_id
),
joined as (
    select
        oi.order_id,
        oi.product_id,
        oi.quantity,
        o.shipping_cost,
        ic.num_items,
        o.shipping_cost / ic.num_items as shipping_cost_allocated
    from order_items oi
    join orders o on oi.order_id = o.order_id
    join item_counts ic on oi.order_id = ic.order_id
)
select * from joined
