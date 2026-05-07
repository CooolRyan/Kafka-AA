#!/usr/bin/env bash
# 리눅스(Rocky/Ubuntu 등)에서 Apache JMeter 바이너리 설치 후 비 GUI 부하 테스트 실행
# 사용:
#   chmod +x setup-linux.sh
#   ./setup-linux.sh                    # Spring 이 같은 머신이면 127.0.0.1:8080
#   BASE_HOST=10.0.0.5 ./setup-linux.sh # Spring 이 다른 호스트일 때
set -euo pipefail

JMETER_VERSION="${JMETER_VERSION:-5.6.3}"
PREFIX="${JMETER_PREFIX:-/opt}"
JMETER_HOME="${PREFIX}/apache-jmeter-${JMETER_VERSION}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JMX="${SCRIPT_DIR}/kafka-aa-via-spring.jmx"
BASE_HOST="${BASE_HOST:-127.0.0.1}"
BASE_PORT="${BASE_PORT:-8080}"

if [[ ! -f "$JMX" ]]; then
  echo "없음: $JMX"
  exit 1
fi

if ! command -v java &>/dev/null; then
  if command -v dnf &>/dev/null; then
    sudo dnf install -y java-17-openjdk-headless wget tar
  elif command -v apt-get &>/dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y openjdk-17-jre-headless wget
  else
    echo "Java 17+ 필요. 패키지 매니저로 설치 후 다시 실행하세요."
    exit 1
  fi
fi

if [[ ! -x "$JMETER_HOME/bin/jmeter" ]]; then
  TGZ="/tmp/apache-jmeter-${JMETER_VERSION}.tgz"
  URL_PRIMARY="https://dlcdn.apache.org/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz"
  URL_ARCHIVE="https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz"
  echo "JMeter ${JMETER_VERSION} 다운로드..."
  wget -qO "$TGZ" "$URL_PRIMARY" || wget -qO "$TGZ" "$URL_ARCHIVE"
  sudo mkdir -p "$PREFIX"
  sudo tar -xzf "$TGZ" -C "$PREFIX"
  sudo chmod -R a+rX "$JMETER_HOME"
fi

OUT="${JMETER_RESULTS:-/tmp/kafka-aa-jmeter-$(date +%Y%m%d-%H%M%S).jtl}"
JMETER_LOG="${JMETER_LOG:-/tmp/jmeter-kafka-aa-$(date +%Y%m%d-%H%M%S).log}"
echo "실행: BASE_HOST=$BASE_HOST BASE_PORT=$BASE_PORT"
echo "결과 JTL: $OUT"
echo "JMeter 로그: $JMETER_LOG"

"$JMETER_HOME/bin/jmeter" -n \
  -j "$JMETER_LOG" \
  -t "$JMX" \
  -l "$OUT" \
  -JBASE_HOST="$BASE_HOST" \
  -JBASE_PORT="$BASE_PORT"

echo "완료. 실패 샘플 확인: grep ',false,' $OUT | head"
