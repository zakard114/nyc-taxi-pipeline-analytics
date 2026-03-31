-- Two rows (Yellow / Green): total trips over all months in dm_citywide_monthly.
-- Use in Looker pie: dimension = service_type, metric = trip_total (aggregation: None or Auto — avoids SUM errors).

{{ config(materialized='table') }}

select
  service_type,
  sum(trips) as trip_total
from {{ ref('dm_citywide_monthly') }}
group by service_type
