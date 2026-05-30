-- 기존 147 등: summary 컬럼 추가 (테이블이 이미 있으면 실행)
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS dedup_skipped_count UInt32 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS primary_committed_lag_at_failover Int64 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS mirror_lag_at_failover Int64 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS pending_unread_at_failover UInt32 DEFAULT 0;
ALTER TABLE default.failover_test_summary ADD COLUMN IF NOT EXISTS missing_on_mirror_at_failover UInt32 DEFAULT 0;
