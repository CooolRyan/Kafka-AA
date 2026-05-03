#!/usr/bin/env bash
# kafka-active001 에서 실행: MM2 전용 프로세스 + systemd 등록
set -euo pipefail
install -d -o root -g kafka /etc/kafka
install -m 0640 -o root -g kafka "$(dirname "$0")/mm2-remote.properties" /etc/kafka/mm2-remote.properties
install -m 0644 "$(dirname "$0")/kafka-mm2.service" /etc/systemd/system/kafka-mm2.service
systemctl daemon-reload
systemctl enable kafka-mm2
systemctl restart kafka-mm2
systemctl --no-pager -l status kafka-mm2 || true
