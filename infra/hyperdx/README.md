# HyperDX for Kafka Mirror Visualization

HyperDX is used here as a ClickHouse-backed log/event UI, not as a direct Kafka topic browser.

The Spring app already writes:

- `default.mirror_lag`: approximate source-vs-mirror offset lag
- `default.mirror_message_tail`: recent source/mirror messages for side-by-side inspection
- `default.mirror_message_compare`: id-matched source/mirror messages with approximate replication delay

This compose file starts only HyperDX + MongoDB and connects HyperDX to the existing host ClickHouse at `127.0.0.1:8123`.

## Run on `192.168.160.147`

Docker is required.

```bash
cd /path/to/kafka-aa/infra/hyperdx
docker compose up -d
```

Open:

```text
http://192.168.160.147:3000
```

Default sources:

- `Kafka Mirror Messages` -> `default.mirror_message_tail`
- `Kafka Mirror Delay` -> `default.mirror_message_compare`
- `Kafka Mirror Lag` -> `default.mirror_lag`

## Notes

- The HyperDX container uses `network_mode: host` so it can reach the host-local ClickHouse bound to `127.0.0.1:8123`.
- Ports:
  - HyperDX UI: `3000`
  - HyperDX API: `8000`
  - MongoDB: `127.0.0.1:27017` only
