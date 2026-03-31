-- Staging layer: type casts, light cleaning, and deduplication for NYC Yellow Taxi.
-- Source: {{ source('staging','yellow_tripdata') }} — see models/schema.yml for source → table mapping.
-- Note: NYC Yellow Taxi does not include ehail_fee (that column is typical on Green Taxi).
-- To add optional columns (e.g. airport_fee), verify names/types in BigQuery (INFORMATION_SCHEMA, or sample SELECT).

{{ config(materialized='view') }} -- Build this model as a BigQuery view

with tripdata as (
  select
    *,
    -- Number rows per (VendorID, tpep_pickup_datetime) to flag duplicate trip records.
    row_number() over (
      partition by VendorID, tpep_pickup_datetime
    ) as rn
  from {{ source('staging', 'yellow_tripdata') }}
  where VendorID is not null -- drop rows missing a vendor id
)

select
  -- Surrogate key (hex of MD5) without dbt_utils — avoids dbt_packages install issues on Windows.
  to_hex(
    md5(concat(cast(VendorID as string), '|', cast(tpep_pickup_datetime as string)))
  ) as tripid,

  -- ID-like columns as int64
  cast(VendorID as int64) as vendorid,
  cast(RatecodeID as int64) as ratecodeid,
  cast(PULocationID as int64) as pickup_locationid,
  cast(DOLocationID as int64) as dropoff_locationid,

  -- Timestamps
  cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
  cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

  -- Passenger & trip
  passenger_count,
  trip_distance,

  -- Fare & payment
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
where rn = 1 -- keep the first row per (vendor, pickup time) to remove duplicates
