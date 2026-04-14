-- Singular test: mart grain is (revenue_zone, revenue_month, service_type) — no duplicate rows.
select revenue_zone, revenue_month, service_type, count(*) as c
from {{ ref('dm_monthly_zone_revenue') }}
group by 1, 2, 3
having c > 1
