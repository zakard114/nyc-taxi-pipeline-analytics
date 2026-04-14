-- Matches this repo: scripts/streaming/producer.py -> topic "taxi-topic", JSON keys vendor_id, lpep_* (Green taxi).
-- NOT Gemini's Yellow schema (topic taxi_trips, tpep_*).
-- Run: docker compose cp scripts/sql/flink_green_taxi_topic_to_pg.sql flink-jobmanager:/tmp/g.sql
--      docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh embedded -f /tmp/g.sql
--
-- Uses PROCTIME() for 5-minute windows so datetime string formats from JSON do not break parsing.
-- scan.startup.mode = latest-offset: only records produced after this job starts (not full topic replay).

DROP TABLE IF EXISTS taxi_trips_green;

DROP TABLE IF EXISTS pg_trip_stats;

CREATE TABLE pg_trip_stats (
    vendor_id STRING,
    trip_count BIGINT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (vendor_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/kestra',
    'table-name' = 'trip_stats_realtime',
    'username' = 'kestra',
    'password' = 'k3str4',
    'sink.buffer-flush.max-rows' = '1'
);

CREATE TABLE taxi_trips_green (
    vendor_id INT,
    lpep_pickup_datetime STRING,
    lpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    total_amount DOUBLE,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'taxi-topic',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-consumer-group-green-latest',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

INSERT INTO pg_trip_stats
SELECT
    CAST(vendor_id AS STRING),
    COUNT(*) AS trip_count,
    window_start,
    window_end
FROM TABLE(
    TUMBLE(TABLE taxi_trips_green, DESCRIPTOR(proc_time), INTERVAL '5' MINUTES))
GROUP BY vendor_id, window_start, window_end;
