#!/usr/bin/env bash
# Ubuntu 로컬 서버(기본 192.168.160.147:22 / ubuntu)로 Spring Boot fat JAR 전송
# 사용 전: cp local-server.env.example local-server.env && 편집
# 실행: 저장소 루트(kafka-aa)에서 bash infra/deploy/deploy-jar.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${DEPLOY_ENV_FILE:-$ROOT/infra/deploy/local-server.env}"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  set -a && source "$ENV_FILE" && set +a
fi

DEPLOY_HOST="${DEPLOY_HOST:-192.168.160.147}"
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"
DEPLOY_PORT="${DEPLOY_PORT:-22}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/apps/kafka-active}"

JAR="$(ls -1 "$ROOT/active/active/build/libs"/active-*-SNAPSHOT.jar 2>/dev/null | head -1 || true)"
if [[ ! -f "$JAR" ]]; then
  echo "JAR 없음. 먼저: cd active/active && ./gradlew bootJar"
  exit 1
fi

echo "배포: $JAR -> ${DEPLOY_USER}@${DEPLOY_HOST}:${DEPLOY_PORT}:${REMOTE_DIR}/"
ssh -p "$DEPLOY_PORT" -o StrictHostKeyChecking=accept-new "${DEPLOY_USER}@${DEPLOY_HOST}" "mkdir -p '$REMOTE_DIR'"
scp -P "$DEPLOY_PORT" -o StrictHostKeyChecking=accept-new "$JAR" "${DEPLOY_USER}@${DEPLOY_HOST}:${REMOTE_DIR}/active.jar"
echo "완료. 원격에서 예: java -jar $REMOTE_DIR/active.jar (또는 systemd 서비스로 등록)"
