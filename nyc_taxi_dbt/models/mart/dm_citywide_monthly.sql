-- Citywide roll-up (no zone): one row per calendar month × service_type.
-- Use in Looker Studio with metric `trips` (SUM in Looker = additive; avoids errors on
-- re-aggregating `total_monthly_trips` from dm_monthly_zone_revenue).

{{ config(materialized='table') }}

select
  revenue_month,
  extract(year from revenue_month) as trip_year,
  extract(month from revenue_month) as trip_month,
  service_type,
  sum(total_monthly_trips) as trips,
  sum(revenue_monthly_total_amount) as revenue_total
from {{ ref('dm_monthly_zone_revenue') }}
group by revenue_month, service_type
