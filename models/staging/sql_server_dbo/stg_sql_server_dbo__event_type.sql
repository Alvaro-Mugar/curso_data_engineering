{{ config(
  materialized = 'view'
) }}

with source as (
  select distinct event_type
    from {{ source('sql_server_dbo', 'events') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key (['event_type']) }} as event_type_id,
    lower(trim(event_type)) as event_type_name
  from source
)

select * from transformed