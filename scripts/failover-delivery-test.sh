#!/usr/bin/env bash
# Failover 유실·중복 API 시나리오 (147 등)
set -euo pipefail
BASE="${BASE:-http://127.0.0.1:8080}"

echo "=== 1) 테스트 시작 ==="
curl -sS -X POST "$BASE/api/failover/test/start" | python3 -m json.tool 2>/dev/null || curl -sS -X POST "$BASE/api/failover/test/start"
echo

echo "=== 2) produce 20 (Primary/A) ==="
curl -sS -X POST "$BASE/api/failover/test/produce?count=20&prefix=fo-p1"
echo
sleep 4

echo "=== 3) failover standby ==="
curl -sS -X POST "$BASE/api/failover/test/failover/standby"
echo
sleep 2

echo "=== 4) produce 10 ==="
curl -sS -X POST "$BASE/api/failover/test/produce?count=10&prefix=fo-p2"
echo
sleep 4

echo "=== 5) failback primary ==="
curl -sS -X POST "$BASE/api/failover/test/failback/primary"
echo
sleep 2

echo "=== 6) produce 10 ==="
curl -sS -X POST "$BASE/api/failover/test/produce?count=10&prefix=fo-p3"
echo
sleep 4

echo "=== 7) 종료 · 요약 ==="
curl -sS -X POST "$BASE/api/failover/test/finish" | python3 -m json.tool 2>/dev/null || curl -sS -X POST "$BASE/api/failover/test/finish"
echo
echo "CH: SELECT * FROM default.failover_test_summary ORDER BY ts DESC LIMIT 3;"
