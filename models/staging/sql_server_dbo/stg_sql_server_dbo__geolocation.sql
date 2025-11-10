{{ config(
  materialized = 'view'
) }}

with source as (
  select distinct (country, state, zipcode) 
    from {{ ref('stg_sql_server_dbo__addresses') }}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['country', 'state', 'zipcode']) }} as geo_id,
    trim(country) as country,
    trim(state) as state,
    nullif(trim(to_varchar(zipcode)), '') as zipcode
  from source
)

select * from transformed
