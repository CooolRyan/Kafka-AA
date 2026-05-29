#!/usr/bin/env bash
# Spring Kafka AA consumer failover 수동 테스트 (147 등 앱이 떠 있는 호스트에서 실행)
# 사용: BASE=http://127.0.0.1:8080 bash scripts/failover-test.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
LOG="${LOG:-/home/ubuntu/apps/active.log}"
PREFIX="${PREFIX:-fo-test}"

echo "=== 1) 상태 (primary 소비 중이어야 함) ==="
curl -sS "$BASE/api/failover/status" | python3 -m json.tool 2>/dev/null || curl -sS "$BASE/api/failover/status"
echo

echo "=== 2) produce x10 (A/B 50:50) — PRIMARY 소비 로그 확인 ==="
curl -sS -X POST "$BASE/api/produce?count=10&prefix=$PREFIX-primary"
echo
sleep 3
echo "--- recent PRIMARY consumes ---"
grep -F '[consume][PRIMARY]' "$LOG" 2>/dev/null | tail -5 || echo "(로그 없음: $LOG)"

echo
echo "=== 3) 수동 failover → standby (B 클러스터에서 consume) ==="
curl -sS -X POST "$BASE/api/failover/standby"
echo
curl -sS "$BASE/api/failover/status" | python3 -m json.tool 2>/dev/null || curl -sS "$BASE/api/failover/status"
echo

echo "=== 4) produce x10 — STANDBY 소비 로그 확인 ==="
curl -sS -X POST "$BASE/api/produce?count=10&prefix=$PREFIX-standby"
echo
sleep 3
echo "--- recent STANDBY consumes ---"
grep -F '[consume][STANDBY]' "$LOG" 2>/dev/null | tail -5 || echo "(로그 없음)"

echo
echo "=== 5) failback → primary ==="
curl -sS -X POST "$BASE/api/failover/primary"
echo
curl -sS "$BASE/api/failover/status" | python3 -m json.tool 2>/dev/null || curl -sS "$BASE/api/failover/status"
echo

echo "=== 6) produce x10 — 다시 PRIMARY ==="
curl -sS -X POST "$BASE/api/produce?count=10&prefix=$PREFIX-back"
echo
sleep 3
grep -F '[consume][PRIMARY]' "$LOG" 2>/dev/null | tail -3 || true

echo
echo "=== 7) 클러스터 reachability ==="
curl -sS -X POST "$BASE/api/probe?cluster=A"; echo
curl -sS -X POST "$BASE/api/probe?cluster=B"; echo
echo "완료. failover 로그:"
grep -E 'failover|failback|stopping primary|stopping standby' "$LOG" 2>/dev/null | tail -10 || true
