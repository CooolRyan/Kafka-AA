#!/usr/bin/env bash
# Rocky Linux 9 — Kafka 4.x KRaft 단일 브로커 설치 스켈레톤
# 사용: NODE=1|2 를 지정해 각 서버에서 실행 (또는 인자로 1 또는 2)
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.0.2}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
DOWNLOAD_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${ARCHIVE}"

NODE="${1:-${NODE:-}}"
if [[ "$NODE" != "1" && "$NODE" != "2" ]]; then
  echo "사용법: NODE=1|2 $0   또는   $0 1|2"
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "root 로 실행하세요."
  exit 1
fi

dnf install -y java-17-openjdk-headless curl tar

id kafka &>/dev/null || useradd -r -m -d /var/lib/kafka -s /sbin/nologin kafka

install -d -o kafka -g kafka /opt/kafka /var/lib/kafka /etc/kafka

TMP="/tmp/${ARCHIVE}"
if [[ ! -f "$TMP" ]]; then
  curl -fsSL -o "$TMP" "$DOWNLOAD_URL"
fi
tar -xzf "$TMP" -C /opt/kafka --strip-components=1
chown -R kafka:kafka /opt/kafka

if [[ "$NODE" == "1" ]]; then
  install -m 0644 "$(dirname "$0")/server-kraft-node1.properties" /etc/kafka/server.properties
else
  install -m 0644 "$(dirname "$0")/server-kraft-node2.properties" /etc/kafka/server.properties
fi
chown root:kafka /etc/kafka/server.properties

install -m 0644 "$(dirname "$0")/kafka.service" /etc/systemd/system/kafka.service

CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)"
echo "포맷에 사용할 CLUSTER_ID=${CLUSTER_ID}"
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c /etc/kafka/server.properties --ignore-formatted

systemctl daemon-reload
systemctl enable kafka
systemctl restart kafka
systemctl --no-pager -l status kafka || true

echo "토픽 생성 예(advertised.listeners 가 사설 IP이면 해당 IP 사용): /opt/kafka/bin/kafka-topics.sh --bootstrap-server <이_노드_advertised_IP>:9092 --create --if-not-exists --topic aa-demo-events --partitions 3 --replication-factor 1"
