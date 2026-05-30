-- Failover delivery verification (apply: clickhouse-client --multiquery < failover-schema.sql)

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
