select status
from {{ ref('stg_sql_server_dbo__promos') }}
where status != lower(status)