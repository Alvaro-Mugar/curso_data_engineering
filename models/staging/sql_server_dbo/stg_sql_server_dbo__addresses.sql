{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'addresses') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key (['addresses_id']) }} as address_id,    
    addresses_id as address_uuid,
    {{ dbt_utils.generate_surrogate_key(['country', 'state', 'zipcode']) }} as geo_id,
    trim(address) as address_line,
    _fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed
