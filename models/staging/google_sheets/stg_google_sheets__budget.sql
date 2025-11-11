{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ source('google_sheets', 'budget') }}
),

transformed as (
    select
    date_trunc('month', month) as month_start,
    {{dbt_utils.generate_surrogate_key (['product_id', 'month_start'])}} as budget_id,
    cast(quantity as int) as quantity_budget,
    {{ dbt_utils.generate_surrogate_key  (['product_id']) }} as product_id,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
    from source
)

select * from transformed