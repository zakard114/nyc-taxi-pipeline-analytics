-- Run inside Flink container, e.g.:
--   docker compose cp scripts/sql/flink_pg_sink_finalize.sql flink-jobmanager:/tmp/flink_pg_sink_finalize.sql
--   docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh embedded -f /tmp/flink_pg_sink_finalize.sql
--
-- INSERT below needs table `taxi_trips` (Kafka) defined in the same session first.

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
