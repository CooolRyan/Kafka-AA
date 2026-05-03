CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.mirror_lag
(
    ts DateTime DEFAULT now(),
    lag_messages Int64,
    source_hwm Int64,
    mirror_hwm Int64,
    source_cluster LowCardinality(String),
    mirror_cluster LowCardinality(String),
    source_topic LowCardinality(String),
    mirror_topic LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY ts;
