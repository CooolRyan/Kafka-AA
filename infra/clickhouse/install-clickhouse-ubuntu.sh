#!/usr/bin/env bash
# Ubuntu 22.04 — ClickHouse APT 설치 + mirror_lag 테이블 (192.168.160.147 등)
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "sudo 로 실행하세요."
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y apt-transport-https ca-certificates curl gnupg

curl -fsSL 'https://packages.clickhouse.com/deb/clickhouse.key' | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list
apt-get update -y
apt-get install -y clickhouse-server clickhouse-client

systemctl enable clickhouse-server
systemctl restart clickhouse-server

SQL_FILE="$(cd "$(dirname "$0")" && pwd)/init.sql"
clickhouse-client --multiquery < "$SQL_FILE"

echo "HTTP http://$(hostname -I | awk '{print $1}'):8123  기본 사용자 default / 빈 비밀번호"
echo "쿼리 예: clickhouse-client -q \"SELECT * FROM default.mirror_lag ORDER BY ts DESC LIMIT 20\""
