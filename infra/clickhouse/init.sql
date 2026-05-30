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

-- Failover delivery verification (see infra/clickhouse/failover-schema.sql)
CREATE TABLE IF NOT EXISTS default.failover_test_run
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    status LowCardinality(String),
    produce_target_cluster LowCardinality(String),
    consumer_topic LowCardinality(String),
    consumer_group LowCardinality(String),
    notes String
)
ENGINE = MergeTree
ORDER BY (ts, run_id);

CREATE TABLE IF NOT EXISTS default.failover_control_event
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    event_type LowCardinality(String),
    consumer_role LowCardinality(String),
    detail String
)
ENGINE = MergeTree
ORDER BY (run_id, ts);

CREATE TABLE IF NOT EXISTS default.failover_message_produced
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    message_id String,
    produce_cluster LowCardinality(String),
    topic LowCardinality(String),
    partition Int32,
    offset Int64,
    sent_at_ms Int64,
    payload String
)
ENGINE = MergeTree
ORDER BY (run_id, message_id, ts);

CREATE TABLE IF NOT EXISTS default.failover_message_consumed
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    message_id String,
    consumer_role LowCardinality(String),
    consume_cluster LowCardinality(String),
    topic LowCardinality(String),
    partition Int32,
    offset Int64,
    consume_seq UInt32,
    is_duplicate UInt8,
    payload String
)
ENGINE = MergeTree
ORDER BY (run_id, message_id, ts);

CREATE TABLE IF NOT EXISTS default.failover_message_dedup
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    message_id String,
    consumer_role LowCardinality(String),
    consume_cluster LowCardinality(String),
    topic LowCardinality(String),
    partition Int32,
    offset Int64,
    action LowCardinality(String),
    payload String
)
ENGINE = MergeTree
ORDER BY (run_id, message_id, ts);

CREATE TABLE IF NOT EXISTS default.failover_mirror_partition_lag
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    phase LowCardinality(String),
    partition Int32,
    source_topic LowCardinality(String),
    mirror_topic LowCardinality(String),
    source_committed_offset Int64,
    source_end_offset Int64,
    mirror_end_offset Int64,
    partition_lag Int64
)
ENGINE = MergeTree
ORDER BY (run_id, phase, partition);

CREATE TABLE IF NOT EXISTS default.failover_mirror_backlog_check
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    phase LowCardinality(String),
    check_type LowCardinality(String),
    message_id String,
    mirror_status LowCardinality(String),
    primary_committed_lag_sum Int64,
    mirror_lag_messages Int64,
    source_partition Int32,
    source_offset Int64,
    mirror_partition Int32,
    mirror_offset Int64
)
ENGINE = MergeTree
ORDER BY (run_id, phase, check_type, message_id);

CREATE TABLE IF NOT EXISTS default.failover_test_summary
(
    ts DateTime64(3) DEFAULT now64(3),
    run_id String,
    status LowCardinality(String),
    produced_count UInt32,
    consumed_events UInt32,
    consumed_unique UInt32,
    duplicate_consume_count UInt32,
    dedup_skipped_count UInt32,
    missing_count UInt32,
    primary_committed_lag_at_failover Int64,
    mirror_lag_at_failover Int64,
    pending_unread_at_failover UInt32,
    missing_on_mirror_at_failover UInt32,
    active_consumer_role LowCardinality(String),
    notes String
)
ENGINE = MergeTree
ORDER BY (ts, run_id);
