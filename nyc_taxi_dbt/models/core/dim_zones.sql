-- Dimension: TLC taxi zone lookup (LocationID → borough, zone, service zone).
-- Built from the dbt seed `taxi_zone_lookup` (NYC TLC reference data).

{{ config(materialized='table') }} -- Small, frequently joined reference data: persist as a physical table.

select
  locationid,
  borough,
  zone,
  -- Workshop transform: replace the substring 'Boro' with 'Green' in service_zone
  -- (e.g. 'Boro Zone' → 'Green Zone'). Other values (e.g. 'Yellow Zone', 'EWR') are unchanged.
  replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('taxi_zone_lookup') }}
