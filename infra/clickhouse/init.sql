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

-- id 기반 source/mirror 매칭 결과. replication_delay_* 는 scraper가 source/mirror를
-- 처음 관측한 시각 차이이므로 scrape interval 단위의 근사값이다.
CREATE TABLE IF NOT EXISTS default.mirror_message_compare
(
    ts DateTime64(3) DEFAULT now64(3),
    message_id String,
    value String,
    source_cluster LowCardinality(String),
    mirror_cluster LowCardinality(String),
    source_topic LowCardinality(String),
    mirror_topic LowCardinality(String),
    source_partition Int32,
    source_offset Int64,
    mirror_partition Int32,
    mirror_offset Int64,
    sent_at_ms Int64,
    source_kafka_ts_ms Int64,
    mirror_kafka_ts_ms Int64,
    source_seen_at_ms Int64,
    mirror_seen_at_ms Int64,
    replication_delay_ms Int64,
    replication_delay_sec Float64,
    end_to_end_delay_ms Int64,
    end_to_end_delay_sec Float64,
    kafka_timestamp_delta_ms Int64
)
ENGINE = MergeTree
ORDER BY (ts, message_id);
