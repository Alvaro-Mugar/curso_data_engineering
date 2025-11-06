with cnt as (
  select count(*) as conteo
  from {{ ref('stg_sql_server_dbo__promos') }}
  where promotion_name = 'no_promo'
)
select *
from cnt
where conteo <> 1