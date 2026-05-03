#!/usr/bin/env bash
# docker compose up -d 이후 한 번 실행: 테이블 생성
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
docker exec -i clickhouse clickhouse-client --multiquery < "$DIR/init.sql"
echo "OK: default.mirror_lag"
