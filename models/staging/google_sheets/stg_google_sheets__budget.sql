{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ source('google_sheets', 'budget') }}
),

transformed as (
    select
    md5(product_id || '_' || to_char(date_trunc('month', month), 'YYYY-MM')) as budget_id,
    cast(quantity as int) as quantity_budget,
    date_trunc('month', month) as month_start,
    md5(product_id) as product_id,
    _fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
    from source
)

select * from transformed