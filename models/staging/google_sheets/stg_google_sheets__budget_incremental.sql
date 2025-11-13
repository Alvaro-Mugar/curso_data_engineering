{{ config(
    materialized='incremental',
    unique_key = '_row'
    ) 
    }}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}

{% if is_incremental() %}

	  WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

{% endif %}
    ),

renamed_casted AS (
    SELECT
    date_trunc('month', month) as month_start,
    {{dbt_utils.generate_surrogate_key (['product_id', 'month_start'])}} as budget_id,
    cast(quantity as int) as quantity_budget,
    {{ dbt_utils.generate_surrogate_key  (['product_id']) }} as product_id,
    convert_timezone('UTC', _fivetran_synced) as synced_utc
    FROM stg_budget_products
    )

SELECT * FROM renamed_casted