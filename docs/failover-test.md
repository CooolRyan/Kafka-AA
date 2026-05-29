# Consumer Failover 테스트 가이드

Spring 앱은 **Primary 클러스터(A)** 에서 `aa-demo-events` 를 consume 하다가, 장애 시 **Standby(B)** 로 listener 를 옮긴다.

## 동작 요약

| 항목 | 값 (기본) |
|------|-----------|
| Primary consumer | 클러스터 **A**, topic `aa-demo-events`, listener id `listenPrimary` |
| Standby consumer | 클러스터 **B**, topic `aa-demo-events`, listener id `listenStandby` (`autoStartup=false`) |
| 수동 전환 | `POST /api/failover/standby` / `POST /api/failover/primary` |
| 자동 전환 | `PrimaryClusterWatchdog` — Primary Admin 연결 실패 시 standby (15초 주기) |
| Consumer group | `aa-demo-app` (양쪽 동일 group id, **동시에 둘 다 켜지지 않음**) |

> **주의:** MM2 미러 토픽은 `A.aa-demo-events` (B에 있음). Consumer failover 는 **B의 `aa-demo-events`** 를 읽는다.  
> produce 가 A/B 50:50 이므로, standby 전환 후에는 **B로 들어간 메시지** 위주로 `[consume][STANDBY]` 로그가 보인다.

## API

```bash
# 상태
curl -sS http://127.0.0.1:8080/api/failover/status

# 수동 failover / failback
curl -sS -X POST http://127.0.0.1:8080/api/failover/standby
curl -sS -X POST http://127.0.0.1:8080/api/failover/primary

# 클러스터 연결 probe
curl -sS -X POST 'http://127.0.0.1:8080/api/probe?cluster=A'
curl -sS -X POST 'http://127.0.0.1:8080/api/probe?cluster=B'
```

## 자동 스크립트 (147 서버)

```bash
chmod +x scripts/failover-test.sh
BASE=http://127.0.0.1:8080 LOG=/home/ubuntu/apps/active.log bash scripts/failover-test.sh
```

성공 기준:

1. failover 전: `active.log` 에 `[consume][PRIMARY]` 증가
2. `POST .../failover/standby` 후 status `"consumingOnStandby": true`
3. produce 후 `[consume][STANDBY]` 로그
4. `POST .../failover/primary` 후 다시 PRIMARY 소비

## Watchdog 테스트 (Primary 장애 시뮬레이션)

1. 앱은 **primary** 소비 중인지 확인 (`/api/failover/status`)
2. 클러스터 **A** 브로커 중지 또는 Spring 서버에서 A:9094 방화벽 차단
3. 최대 ~15초 후 로그: `primary cluster unreachable — failing over to B`
4. A 복구 후 **수동** `POST /api/failover/primary` (watchdog 은 failback 자동 없음)

## 로그 키워드

```text
stopping primary consumer, starting standby
stopping standby consumer, starting primary
[consume][PRIMARY]
[consume][STANDBY]
primary cluster unreachable
```
