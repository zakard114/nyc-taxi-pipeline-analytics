-- Fact table: per-trip rows enriched with pickup/dropoff zone details from dim_zones.
-- Yellow + Green from staging (same column layout).

{{ config(materialized='table') }} -- Materialize as a physical table for fast analytics on large trip volumes.

with yellow_tripdata as (
  select
    *,
    'Yellow' as service_type
  from {{ ref('stg_yellow_tripdata') }}
),

green_tripdata as (
  select
    *,
    'Green' as service_type
  from {{ ref('stg_green_tripdata') }}
),

trips_unioned as (
  select * from yellow_tripdata
  union all
  select * from green_tripdata
),

-- Zone dimension (borough, zone names) for join keys.
dim_zones as (
  select * from {{ ref('dim_zones') }}
)

select
  trips_unioned.tripid,
  trips_unioned.vendorid,
  trips_unioned.service_type,
  trips_unioned.ratecodeid,
  trips_unioned.pickup_locationid,
  -- Pickup: borough and zone name from the dimension.
  pickup_zone.borough as pickup_borough,
  pickup_zone.zone as pickup_zone,
  trips_unioned.dropoff_locationid,
  -- Dropoff: borough and zone name from the dimension.
  dropoff_zone.borough as dropoff_borough,
  dropoff_zone.zone as dropoff_zone,
  trips_unioned.pickup_datetime,
  trips_unioned.dropoff_datetime,
  trips_unioned.passenger_count,
  trips_unioned.trip_distance,
  trips_unioned.fare_amount,
  trips_unioned.extra,
  trips_unioned.mta_tax,
  trips_unioned.tip_amount,
  trips_unioned.tolls_amount,
  trips_unioned.improvement_surcharge,
  trips_unioned.total_amount,
  trips_unioned.payment_type,
  trips_unioned.congestion_surcharge
from trips_unioned
-- Match pickup LocationID to the zone dimension (inner join: unknown IDs are dropped).
inner join dim_zones as pickup_zone
  on trips_unioned.pickup_locationid = pickup_zone.locationid
-- Match dropoff LocationID to the zone dimension (inner join: unknown IDs are dropped).
inner join dim_zones as dropoff_zone
  on trips_unioned.dropoff_locationid = dropoff_zone.locationid
