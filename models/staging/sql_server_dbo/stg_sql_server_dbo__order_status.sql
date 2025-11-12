{{ config(
    materialized = 'incremental'
) }}

with source as (
  select distinct status from {{ source('sql_server_dbo', 'orders') }}

    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

    {% endif %}
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['status']) }} as order_status_id,
    lower(trim(status)) as order_status          
  from source
)

select * from transformed