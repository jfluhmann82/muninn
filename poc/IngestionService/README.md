# Muninn - Ingestion Service

> ⚠️ **This is a minimal proof-of-concept.** It connects to a single market data provider (Alpaca Markets), publishes to a single Kafka topic, and is not yet production-ready. Full architecture, multi-provider support, and hardening are planned for later phases.

---

## What This Is

The ingestion service is the entry point of the Muninn platform. It connects to a live market data WebSocket feed, normalizes inbound events to a canonical envelope schema, and publishes them to Kafka for downstream consumption by the alert engine, risk calculator, and WebSocket gateway.

In its current state, this is a working prototype that validates the core data path: **market data provider → normalization → Kafka**.

---

## Current State (Phase 0)

- Connects to the **Alpaca Markets WebSocket** (`wss://stream.data.alpaca.markets/v2/iex`)
- Subscribes to **trades, quotes, and bars** for a configured symbol list
- Normalizes events to the [Muninn canonical envelope schema](#event-schema)
- Publishes normalized events to the `market.prices.raw` Kafka topic, keyed by symbol
- Reconnects automatically on disconnect with exponential backoff
- Structured JSON logging throughout

**What is not here yet:**
- Polygon.io integration (primary provider – Phase 1)
- Finnhub reference data poller
- FastAPI health and metrics endpoints (`/health/ready`, `/health/live`, `/metrics`)
- Prometheus instrumentation
- Per-symbol sequence number persistence across reconnects
- Kubernetes ConfigMap-driven symbol configuration
- Unit tests

---

## Prerequisites

- Python 3.12+
- A running Kafka broker on `localhost:9092` (see [local stack setup](#local-stack))
- Alpaca Markets account - [free tier](https://alpaca.markets) is sufficient for IEX real-time data
- environment variables set for:
  - ALPACA_API_KEY
  - ALPACA_SECRET_KEY
  - ALPACA_WS_URL

---

## Local Stack

The ingestion service requires Kafka.

## Event Schema

Every event published to Kafka conforms to the Muninn canonical envelope, regardless of source provider. This contract is stable.  Downstream consumers are written against it, not against provider-specific formats.

| Field | Type | Description |
|---|---|---|
| `event_id` | UUID v4 | Unique identifier for idempotency |
| `event_type` | string enum | `TRADE` \| `QUOTE` \| `AGGREGATE` |
| `symbol` | string | Ticker symbol (e.g. `AAPL`) |
| `timestamp_utc` | ISO 8601 | Exchange timestamp in UTC |
| `ingested_at` | ISO 8601 | System ingestion time - used for latency tracking |
| `source` | string | Provider identifier (`alpaca` \| `polygon` \| `finnhub`) |
| `payload` | object | Raw provider-specific event, preserved for replay and audit |
| `sequence_num` | int64 | Monotonically increasing per symbol per session |

**Kafka topic:** `market.prices.raw`
**Partition key:** symbol (ensures per-symbol message ordering)

---

## Roadmap

This service will be substantially expanded in subsequent phases. Key planned work:

**Phase 1 - Core Pipeline**
- [ ] Polygon.io WebSocket integration as primary provider
- [ ] Provider failover: Polygon.io → Alpaca on disconnect
- [ ] FastAPI wrapper: `/health/ready`, `/health/live`, `/metrics`
- [ ] Prometheus counters: ingestion latency, message rate, reconnect events
- [ ] Persistent sequence numbering across reconnects
- [ ] Configurable symbol list via environment variable and Kubernetes ConfigMap
- [ ] Unit tests for normalization layer

**Phase 3 - Kubernetes**
- [ ] Dockerfile and Helm chart
- [ ] Kubernetes readiness and liveness probes wired to FastAPI endpoints
- [ ] KEDA autoscaling based on Kafka consumer group lag
- [ ] Sealed Secrets for provider API key management

**Phase 4 - Observability**
- [ ] Grafana dashboard: ingestion latency histogram, message rate, reconnect frequency
- [ ] PagerDuty alert: provider disconnect sustained > 30 seconds

---

## License

Copyright (c) 2026. All Rights Reserved.

This repository is public for portfolio and demonstration purposes only. No license is granted to use, copy, modify, or distribute this code.