-- Staging layer: type casts, light cleaning, and deduplication for NYC Green Taxi.
-- Source: {{ source('staging','green_tripdata') }} - Green uses lpep_* datetime columns (not tpep_*).
-- Align output columns with stg_yellow_tripdata so fact_trips can UNION ALL.

{{ config(materialized='view') }}

with tripdata as (
  select
    *,
    row_number() over (
      partition by VendorID, lpep_pickup_datetime
    ) as rn
  from {{ source('staging', 'green_tripdata') }}
  where VendorID is not null
)

select
  to_hex(
    md5(concat(cast(VendorID as string), '|', cast(lpep_pickup_datetime as string)))
  ) as tripid,

  cast(VendorID as int64) as vendorid,
  cast(RatecodeID as int64) as ratecodeid,
  cast(PULocationID as int64) as pickup_locationid,
  cast(DOLocationID as int64) as dropoff_locationid,

  cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
  cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

  passenger_count,
  trip_distance,

  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  payment_type,
  cast(congestion_surcharge as float64) as congestion_surcharge

from tripdata
where rn = 1
