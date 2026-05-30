-- 기존 147 등: summary 컬럼 추가 (테이블이 이미 있으면 실행)
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS dedup_skipped_count UInt32 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS primary_committed_lag_at_failover Int64 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS mirror_lag_at_failover Int64 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS pending_unread_at_failover UInt32 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS missing_on_mirror_at_failover UInt32 DEFAULT 0;

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

ALTER TABLE default.failover_mirror_backlog_check ADD COLUMN IF NOT EXISTS check_type LowCardinality(String) DEFAULT '';
ALTER TABLE default.failover_mirror_backlog_check ADD COLUMN IF NOT EXISTS source_partition Int32 DEFAULT -1;
ALTER TABLE default.failover_mirror_backlog_check ADD COLUMN IF NOT EXISTS source_offset Int64 DEFAULT -1;
