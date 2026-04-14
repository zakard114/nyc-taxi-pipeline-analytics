-- Singular test: one row per (revenue_month, service_type).
select revenue_month, service_type, count(*) as c
from {{ ref('dm_citywide_monthly') }}
group by 1, 2
having c > 1
