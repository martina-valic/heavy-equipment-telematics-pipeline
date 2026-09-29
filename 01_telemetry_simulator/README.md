# Module 1: Real-Time IoT Telemetry Simulator

Simulates 40 underground mining machines. Each machine emits one telemetry event every 5 seconds. Events are validated against a JSON Schema data contract and then published to Kafka.

```
FleetSimulator ──► AnomalyInjector ──► contract validation ──┬─► telematics.equipment.telemetry.v1
 (state machine)     (~7.5% of events)   (JSON Schema)        └─► telematics.equipment.telemetry.dlq.v1  (violations only)
```

## Files

| File | Purpose |
|---|---|
| `equipment_config.py` | Fleet definition (40 machines) and the OPC UA signal registry, with value ranges for each state |
| `simulator.py` | Moves each machine between IDLE, OPERATIONAL and FAULT, samples its readings, and keeps engine hours and uptime increasing |
| `anomalies.py` | Injects data-quality defects into a share of events |
| `contract.py` | Loads [the data contract](../documentation/data_contracts/equipment_telemetry.md) and builds DLQ records |
| `sinks.py` | Kafka producer (idempotent, `acks=all`, keyed by serial number) and a stdout sink for dry runs |
| `run_simulator.py` | Command-line entry point |

## Run it

From the repo root:

```bash
# 1. Start Kafka, create the topics (safe to re-run), and start Kafka UI at http://localhost:8080
docker compose -f docker/docker-compose.yml up -d

# 2. Create the virtual environment and install dependencies
python -m venv .venv
.venv/Scripts/python -m pip install -r testing/python/requirements.txt   # Windows
# .venv/bin/python -m pip install -r testing/python/requirements.txt     # macOS/Linux

# 3. Optional: copy .env.example to .env and edit the settings

# 4. Stream to Kafka until Ctrl+C
.venv/Scripts/python 01_telemetry_simulator/run_simulator.py

# Or print a single tick without Kafka
.venv/Scripts/python 01_telemetry_simulator/run_simulator.py --dry-run --ticks 1 --interval 0
```

| Flag | Env var | Default |
|---|---|---|
| `--interval` | `EMIT_INTERVAL_SECONDS` | `5` |
| `--anomaly-rate` | `ANOMALY_RATE` | `0.075` |
| `--seed` | `SIMULATOR_SEED` | random |
| `--ticks` | none | run forever |
| n/a | `KAFKA_BOOTSTRAP_SERVERS`, `TELEMETRY_TOPIC`, `TELEMETRY_DLQ_TOPIC` | see `.env.example` |

## Tests

```bash
.venv/Scripts/python -m pytest -m "not integration"   # unit tests, no Kafka needed
.venv/Scripts/python -m pytest -m integration         # round-trip through the Docker Kafka stack
```

## CI

[.github/workflows/ci.yml](../.github/workflows/ci.yml) runs on every push to `main`, on pull requests, and on manual dispatch:

| Job | What it checks |
|---|---|
| Unit tests & data contract | Every `*.schema.json` in `documentation/data_contracts/` is valid JSON Schema, then `pytest -m "not integration and not snowflake"` |
| Docker Compose validation | `docker compose config` parses the stack |
| Kafka integration tests | Starts the broker and creates topics on the runner, then runs `pytest -m "integration and not snowflake"`. Missing Kafka fails the job instead of skipping |

Snowflake tests don't run in CI because they need account credentials.

## Anomalies

Anomaly types and how each one is expected to be caught are listed in the [data contract](../documentation/data_contracts/equipment_telemetry.md#injected-data-quality-anomalies). Most anomalies pass the contract on purpose so that the Silver-tier dbt tests have bad records to catch. Only `CONTRACT_VIOLATION` events are routed to the DLQ.
