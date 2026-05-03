#!/usr/bin/env bash
# Rocky Linux 9 — ClickHouse RPM + mirror_lag 테이블 (로컬 서버 등)
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "root 또는 sudo 로 실행하세요."
  exit 1
fi

dnf install -y dnf-plugins-core curl ca-certificates gnupg

curl -fsSL 'https://packages.clickhouse.com/rpm/clickhouse.repo' -o /etc/yum.repos.d/clickhouse.repo
dnf install -y clickhouse-server clickhouse-client

systemctl enable clickhouse-server
systemctl restart clickhouse-server

SQL_FILE="$(cd "$(dirname "$0")" && pwd)/init.sql"
clickhouse-client --multiquery < "$SQL_FILE"

echo "HTTP http://$(hostname -I | awk '{print $1}'):8123  기본 사용자 default / 빈 비밀번호"
echo "쿼리 예: clickhouse-client -q \"SELECT * FROM default.mirror_lag ORDER BY ts DESC LIMIT 20\""
