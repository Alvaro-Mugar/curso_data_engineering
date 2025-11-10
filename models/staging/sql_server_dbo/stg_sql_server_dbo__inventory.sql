{{ config(
    materialized = 'view'
) }}

with source as (
  select distinct inventory
    from {{ ref('stg_sql_server_dbo__products')}}
),

transformed as (
  select
   {{ dbt_utils.generate_surrogate_key(['inventory']) }} as inventory_id,
    cast(inventory as int) as inventory_units
  from source
)

select * from transformed
