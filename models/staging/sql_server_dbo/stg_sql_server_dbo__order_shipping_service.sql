{{ config(
    materialized = 'incremental'
) }}

with source as (
  select distinct shipping_service as shipping_service_name
    from {{ ref('base_sql_server_dbo__orders') }}

    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

    {% endif %}    
),

transformed as (
  select
    {{ dbt_utils.generate_surrogate_key(['shipping_service_name']) }} as shipping_service_id,
    shipping_service_name
  from source
)

select * from transformed