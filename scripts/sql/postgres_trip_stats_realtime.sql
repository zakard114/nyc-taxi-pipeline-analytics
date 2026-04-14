-- Run against THIS repo's docker-compose Postgres (database kestra, user kestra).
-- Example:
--   docker compose exec -T postgres psql -U kestra -d kestra -f - < scripts/sql/postgres_trip_stats_realtime.sql
-- Or interactive: docker compose exec postgres psql -U kestra -d kestra

CREATE TABLE IF NOT EXISTS trip_stats_realtime (
    vendor_id VARCHAR(128) NOT NULL,
    trip_count BIGINT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    PRIMARY KEY (vendor_id, window_start)
);
