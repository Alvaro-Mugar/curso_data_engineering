{{ config(materialized = 'view') }}

with source as (
  select distinct trim(country) as country
  from {{ source('sql_server_dbo', 'addresses') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['country']) }} as country_id,
    country
  from source
)

select * from transformed
