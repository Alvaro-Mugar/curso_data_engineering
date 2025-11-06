{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'addresses') }}
),

transformed as (
  select
    md5(addresses_id) as address_id,    
    addresses_id as address_uuid,
    nullif(trim(to_varchar(zipcode)), '') as zipcode,
    trim(country) as country,
    trim(address) as address_line,
    trim(state) as state,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed
