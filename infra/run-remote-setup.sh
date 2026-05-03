#!/usr/bin/env bash
# NCP Compute(Rocky) Kafka 2대만 PEM으로 설치. 로컬 Ubuntu(예: 192.168.160.147)는 PEM 아님 — ubuntu/ubuntu 등 비밀번호.
# 예: PEM=~/keys/ncp.pem ./run-remote-setup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PEM="${PEM:-$REPO_ROOT/ncp-kafka.pem}"
KAFKA_USER="${KAFKA_USER:-root}"
H1="${KAFKA1_HOST:-211.188.50.31}"
H2="${KAFKA2_HOST:-223.130.154.172}"

chmod 600 "$PEM" 2>/dev/null || true

deploy_one() {
  local host="$1" node="$2"
  local remote="/tmp/kafka-infra-${node}"
  echo "=== Kafka node ${node} -> ${KAFKA_USER}@${host} ==="
  scp -i "$PEM" -o StrictHostKeyChecking=accept-new -r "$REPO_ROOT/infra/kafka" "${KAFKA_USER}@${host}:${remote}"
  ssh -i "$PEM" -o StrictHostKeyChecking=accept-new "${KAFKA_USER}@${host}" "sudo bash ${remote}/install-kafka4-kraft-rocky9.sh ${node}"
  ssh -i "$PEM" -o StrictHostKeyChecking=accept-new "${KAFKA_USER}@${host}" \
    "sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic aa-demo-events --partitions 3 --replication-factor 1"
  echo "done ${host}"
}

deploy_one "$H1" 1
deploy_one "$H2" 2

echo "끝. ClickHouse는 Ubuntu(192.168.160.147)에서: sudo bash infra/clickhouse/install-clickhouse-ubuntu.sh"
