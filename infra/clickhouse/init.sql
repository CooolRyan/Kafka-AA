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

-- Spring 이 주기적으로 소스/미러 토픽 tail 을 넣음 → 8123/play 또는 Grafana 테이블 패널로 한 화면 비교
CREATE TABLE IF NOT EXISTS default.mirror_message_tail
(
    ts DateTime64(3) DEFAULT now64(3),
    lag_at_scrape Int64,
    role LowCardinality(String),
    cluster LowCardinality(String),
    topic LowCardinality(String),
    partition Int32,
    offset Int64,
    kafka_ts_ms Int64,
    message_key String,
    value String
)
ENGINE = MergeTree
ORDER BY (ts, role, topic, partition, offset);
