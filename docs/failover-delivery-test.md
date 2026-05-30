# Failover 유실·중복 검증

## Consumer failover 방식 (중요)

| 방식 | 설정 | failover 방법 |
|------|------|----------------|
| **앱 수동** (기본) | `consumer-mode=dual-listener` | `POST /api/failover/standby` |
| **HAProxy** (권장) | `consumer-mode=proxy` + `application-proxy.yaml` | **Cluster A down** → HAProxy backup(B) |

프록시 설계: [failover-haproxy-design.md](failover-haproxy-design.md) · 설정: `infra/haproxy/`

## 웹 UI

```text
http://<app-host>:8080/failover/test
```

버튼 순서대로 시나리오 실행 → **missing** / **duplicate reads** 지표 확인.

## API

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/api/failover/test/start?produceCluster=A` | 테스트 run 시작 (기본 produce = Primary) |
| POST | `/api/failover/test/produce?count=20&prefix=fo-p1` | 현재 run에 메시지 발행 (A만) |
| POST | `/api/failover/test/failover/standby` | consumer → Standby(B) |
| POST | `/api/failover/test/failback/primary` | consumer → Primary(A) |
| POST | `/api/failover/test/finish` | run 종료 + CH 요약 |
| GET | `/api/failover/test/active` | 진행 중 run 리포트 |

## ClickHouse 테이블

DDL: `infra/clickhouse/failover-schema.sql` (또는 `init.sql` 하단)

| 테이블 | 용도 |
|--------|------|
| `failover_test_run` | run 메타 |
| `failover_control_event` | start / failover / finish 이벤트 |
| `failover_message_produced` | produce 1건당 1 row |
| `failover_message_consumed` | consume 1건당 1 row (`is_duplicate`) |
| `failover_test_summary` | run 종료 시 집계 |

### 조회 예

```sql
SELECT * FROM default.failover_test_summary ORDER BY ts DESC LIMIT 10;

SELECT message_id, consumer_role, consume_seq, is_duplicate
FROM default.failover_message_consumed
WHERE run_id = '<run_id>'
ORDER BY ts DESC;

SELECT message_id FROM default.failover_message_produced WHERE run_id = '<run_id>'
  AND message_id NOT IN (
    SELECT message_id FROM default.failover_message_consumed WHERE run_id = '<run_id>'
  );
```

## At-least-once + 멱등 (effectively-once)

- Kafka **EOS**(transactional) 가 아니라 **at-least-once delivery + 앱 `id` 멱등** (`ProcessedMessageStore`).
- 이미 처리한 `id` 가 미러/Standby 에서 다시 오면 **`[SKIP-DUP]`** 로 버리고 `failover_message_dedup` 에 기록.

## Failover consume 토픽 (3번)

| 역할 | 토픽 | 클러스터 |
|------|------|----------|
| Primary | `aa-demo-events` | A |
| Standby / Proxy failover | **`A.aa-demo-events`** (MM2 미러) | B |

`app.kafka.consumer.mirror-topic: A.aa-demo-events`

## 미러 backlog 검증 (1번)

`POST /api/failover/test/mirror-backlog-check` 또는 **Failover Standby 직전 자동** 실행:

- Primary **committed~HWM lag** 합 (`primary_committed_lag_sum`)
- 테스트 run 에서 **아직 consume 안 한 id** 가 미러 tail 에 있는지 → `failover_mirror_backlog_check`

## HAProxy = L4 (TCP)

Kafka 바이너리 프로토콜 → **L7 HTTP 프록시 부적합**. `mode tcp` + active/backup 이 맞는 판단.  
(브로커 `advertised.listeners` 가 공인 IP 이면 클라이언트가 프록시를 우회할 수 있음 — 운영 시 listener 설계 필요.)

## 지표 정의

| 지표 | 의미 |
|------|------|
| `produced_count` | 테스트 run에서 A(또는 지정 클러스터)에 발행한 고유 `id` 수 |
| `consumed_events` | 실제 처리(멱등 통과) consume 횟수 |
| `dedup_skipped_count` | 멱등 store 에서 **버린** 중복 consume |
| `missing_count` | produce 됐으나 consume 0인 `id` 수 |
| `missing_on_mirror_at_failover` | failover 직전 pending id 중 미러에 없던 수 |
| `duplicate_consume_count` | id별 (consume 횟수 − 1) — 멱등 **전** Kafka 중복 delivery |

## CH 스키마 적용 (147)

```bash
clickhouse-client --multiquery < /path/to/kafka-aa/infra/clickhouse/failover-schema.sql
```

## HyperDX

소스 `Kafka Failover Summary` → `failover_test_summary` (compose 또는 Mongo 수동 추가).
