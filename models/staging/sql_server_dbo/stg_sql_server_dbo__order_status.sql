{{ config(
    materialized = 'view'
) }}

with source as (
  select * from {{ ref('stg_sql_server_dbo__orders') }}
),

transformed as (
  select
    md5(order_status_id) as order_status_id,
    md5(order_id) as order_id,
    lower(trim(status)) as status,           
    nullif(trim(is_final),'') as is_final,
    nullif(trim(can_cancel),'') as can_cancel,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed