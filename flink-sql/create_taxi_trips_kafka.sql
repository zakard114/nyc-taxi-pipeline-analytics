-- Kafka (Redpanda) source — run from JobManager: sql-client.sh embedded -f /path/...
CREATE TABLE taxi_trips (
    vendor_id INT,
    lpep_pickup_datetime STRING,
    lpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance FLOAT,
    total_amount FLOAT
) WITH (
    'connector' = 'kafka',
    'topic' = 'taxi-topic',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-consumer-group-latest',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);
