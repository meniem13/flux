# Crypto Streaming Platform

**Real-time cryptocurrency analytics using Kafka, Flink, ClickHouse, and an Iceberg lakehouse.**

Python · Apache Kafka · Apache Flink · Avro · ClickHouse · Iceberg · MinIO · Nessie · Airflow · Grafana · Docker Compose

## Overview

The platform ingests live trades from Binance and Coinbase, processes them through Kafka and Flink, stores analytical results in ClickHouse, and archives raw trades in an Iceberg data lake.

An hourly Airflow job recomputes VWAP from Iceberg and compares it with streaming results in ClickHouse to detect discrepancies.

## Architecture

```text
Binance / Coinbase
       |
Python WebSocket Producers
       |
Kafka + Avro + Schema Registry
       |
Apache Flink
       |
       +------> ClickHouse ------> Grafana
       |
       +------> MinIO + Iceberg + Nessie
                         |
                   Airflow (hourly)
                         |
                  VWAP Reconciliation
```

Prometheus collects infrastructure metrics for Grafana monitoring.

## Stream Processing

Five Flink jobs run continuously:

- **VWAP:** Calculates volume-weighted average prices over time windows.
- **Whale Detector:** Identifies unusually large trades.
- **Arbitrage Monitor:** Detects price differences between exchanges.
- **Double Bottom CEP:** Uses Flink SQL `MATCH_RECOGNIZE` to detect sequential price patterns.
- **Iceberg Sink:** Archives raw trades to the Iceberg lakehouse.

The CEP strategy demonstrates event-pattern detection, not a production trading signal.

## Storage and Analytics

- **Kafka:** Buffers exchange trade events.
- **ClickHouse:** Stores streaming analytics and dashboard results.
- **MinIO + Iceberg:** Stores raw trade history.
- **Nessie:** Provides the Iceberg catalog.

ClickHouse uses `ReplacingMergeTree` for eventual deduplication.

## Batch Reconciliation

An hourly Airflow workflow:

1. Reads raw trades from Iceberg.
2. Recomputes VWAP using batch processing.
3. Compares batch results with streaming VWAP in ClickHouse.
4. Stores reconciliation results for monitoring.

This helps detect inconsistencies between streaming and batch computations.

## Monitoring

Grafana dashboards cover:

- Market analytics and VWAP.
- Large trade alerts.
- Cross-exchange arbitrage signals.
- Batch reconciliation results.
- Kafka and ClickHouse monitoring.

Prometheus collects infrastructure metrics.

## Tech Stack

| Area              | Technologies                  |
| ----------------- | ----------------------------- |
| Ingestion         | Python, WebSockets            |
| Messaging         | Kafka, Avro, Schema Registry  |
| Stream processing | Apache Flink, Flink SQL, CEP  |
| OLAP              | ClickHouse                    |
| Lakehouse         | MinIO, Apache Iceberg, Nessie |
| Orchestration     | Apache Airflow                |
| Observability     | Prometheus, Grafana           |
| Deployment        | Docker Compose                |

## Getting Started

Requirements:

- Docker Desktop with at least 16 GB assigned memory.
- Python 3.10+.
- Approximately 2 GB free disk space for Docker images.

The project was tested on macOS with an M1 Pro and 32 GB RAM. Windows has not been tested.

See the Makefile and project setup instructions for startup commands. Producers are started separately from the Docker services.

## Testing

Run the test suite:

```bash
make test
```

Tests cover producer parsing, Flink record transformations, and Avro schema serialization and deserialization. Tests requiring optional dependencies may be skipped.

## Known Limitations

- Producers run on the host rather than in Docker.
- The ClickHouse sink uses micro-batching and provides at-least-once delivery; it is not fully integrated with Flink checkpoint state.
- `ReplacingMergeTree` handles eventual duplicate removal.
- The dead-letter queue topic exists in the design but is not fully implemented.
- Full Avro schema evolution and compatibility handling are not implemented.
- Iceberg compaction, snapshot expiration, and orphan-file cleanup are not implemented.
- Some Docker images use unpinned `latest` tags.
- The complete stack requires substantial memory.

## Project Layout

```text
producers/       Binance and Coinbase WebSocket producers
flink_jobs/      Streaming jobs and reconciliation logic
scripts/         Kafka topic initialization
clickhouse/      Database configuration and SQL schemas
monitoring/      Prometheus and Grafana configuration
airflow/         Reconciliation DAG
docker/          Custom Flink image and dependencies
tests/           Automated tests
docker-compose.yml
Makefile
```

## Engineering Lessons

The project addresses practical issues involving:

- UTC-aware timestamps and deduplication.
- Flink direct-memory allocation.
- WebSocket heartbeat detection and reconnection.
- ClickHouse eventual deduplication.
- Event-time watermarks and idle Kafka partitions.
- Consumer-lag alert tuning.
