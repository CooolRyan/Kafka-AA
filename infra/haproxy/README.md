# HAProxy Kafka Failover (TCP)

## 왜 앱 수동 failover 가 아닌가

현재 `ConsumerFailoverCoordinator` 는 Spring이 **listener A/B 를 코드로 stop/start** 합니다.  
운영에서 기대하는 failover 는 보통:

1. TCP 연결 대상이 죽음
2. 클라이언트(consumer) **재연결**
3. 로드밸런서/프록시가 **살아 있는 브로커(또는 backup 클러스터)** 로 넘김

→ **HAProxy(또는 NLB) + 단일 bootstrap** 이 이에 가깝습니다.

## 이 레포의 두 모드

| 모드 | 설정 | failover 트리거 |
|------|------|-----------------|
| `dual-listener` (기본) | `app.kafka.consumer-mode=dual-listener` | API / Watchdog 가 listener 전환 |
| `proxy` | `app.kafka.consumer-mode=proxy` | **A 브로커 down** → HAProxy 가 B(backup)로 TCP |

프록시 모드 상세: [docs/failover-haproxy-design.md](../docs/failover-haproxy-design.md)

## 147 서버에서 실행 예

```bash
# 설정 IP 는 kafka-failover-active-backup.cfg 와 application.yaml A/B bootstrap 과 일치해야 함
sudo haproxy -f /home/ubuntu/kafka-aa/infra/haproxy/kafka-failover-active-backup.cfg -D

# Spring (proxy 프로파일)
java -jar active.jar \
  --spring.profiles.active=proxy \
  --app.clickhouse.http-url=http://127.0.0.1:8123
```

## Failover 테스트 (프록시)

1. `failover/test` 웹에서 테스트 시작 → produce (A 직접)
2. **A 브로커 중지** 또는 9094 차단 → HAProxy check fail
3. consumer 는 `127.0.0.1:19092` 재연결 → **B** 로 붙음
4. CH `failover_message_consumed` 에 `consumer_role=PROXY` 로 기록
5. missing/duplicate 지표 확인

앱의 **Failover Standby 버튼** 은 proxy 모드에서 비활성(400) — 인프라 failover 만 사용.
