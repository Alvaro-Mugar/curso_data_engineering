{{ config(
  materialized = 'view'
) }}

with source as (
  select * from {{ source('sql_server_dbo', 'users') }}
),

transformed as (
  select
    md5(user_id) as user_id,
    user_id as user_uuid,
    convert_timezone('UTC', updated_at) as updated_at_utc,
    md5(address_id) as address_id,
    address_id as address_uuid,
    trim(last_name) as user_last_name,
    convert_timezone('UTC', created_at) as created_at_utc,
    trim(phone_number) as user_phone_number,
    trim(first_name) as user_first_name,
    lower(trim(email)) as user_email,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
  from source
)

select * from transformed