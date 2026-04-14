-- Prerequisites: `taxi_trips` Kafka source table exists and uses Yellow-taxi style columns (VendorID, tpep_pickup_datetime, ...).
-- Run after flink_pg_sink_finalize.sql in the same Flink SQL session (or include both in one -f batch).

INSERT INTO pg_trip_stats
SELECT
    CAST(VendorID AS STRING),
    COUNT(*) AS trip_count,
    window_start,
    window_end
FROM TABLE(
    TUMBLE(TABLE taxi_trips, DESCRIPTOR(tpep_pickup_datetime), INTERVAL '5' MINUTES))
GROUP BY VendorID, window_start, window_end;
