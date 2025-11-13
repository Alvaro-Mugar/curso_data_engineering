{{ config(materialized = 'view') }}

with source as (
  select distinct
    nullif(trim(to_varchar(zipcode)), '') as zipcode,
    trim(state) as state
  from {{ source('sql_server_dbo', 'addresses') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['zipcode']) }} as zipcode_id,
    {{ dbt_utils.generate_surrogate_key(['state']) }} as state_id,
    zipcode
  from source
)

select * from transformed
