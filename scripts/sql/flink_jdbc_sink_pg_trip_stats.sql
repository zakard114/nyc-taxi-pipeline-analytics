-- Flink SQL Client: use after flink-libs contains flink-connector-jdbc + postgresql driver and Flink was restarted.
-- Postgres must have table trip_stats_realtime (see postgres_trip_stats_realtime.sql).
-- URLs match docker-compose.yml: postgres service, DB kestra, user kestra.

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
