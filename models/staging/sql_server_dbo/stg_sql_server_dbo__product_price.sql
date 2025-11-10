{{ config(
    materialized = 'view'
) }}

with source as (
  select distinct price from {{ ref('stg_sql_server_dbo__products')}}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['price']) }} as product_price_id,
    cast(price as float) as product_price
  from source
)

select * from transformed