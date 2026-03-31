-- Mart: monthly revenue and trip metrics by pickup zone (dashboard-friendly grain).
-- Grain: one row per (pickup zone, calendar month, service type).

{{ config(materialized='table') }} -- Pre-aggregate for fast Looker Studio / BI dashboards.

with trips_data as (
  select * from {{ ref('fact_trips') }}
)

select
  -- Grain: where revenue is attributed (pickup zone), which month, and Yellow vs Green.
  pickup_zone as revenue_zone,
  {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month,
  service_type,

  -- Revenue components (monthly sums for each zone / month / service_type slice).
  sum(fare_amount) as revenue_monthly_fare,
  sum(extra) as revenue_monthly_extra,
  sum(mta_tax) as revenue_monthly_mta_tax,
  sum(tip_amount) as revenue_monthly_tip_amount,
  sum(tolls_amount) as revenue_monthly_tolls_amount,
  sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
  sum(total_amount) as revenue_monthly_total_amount,

  -- Trip volume and averages within the same slice.
  count(tripid) as total_monthly_trips,
  avg(passenger_count) as avg_monthly_passenger_count,
  avg(trip_distance) as avg_monthly_trip_distance

from trips_data
group by 1, 2, 3
