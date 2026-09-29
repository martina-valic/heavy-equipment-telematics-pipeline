# Module 2: Real-Time Ingestion & Streaming

Kafka Connect streams both telemetry topics into Snowflake Bronze tables using the [Snowflake Connector for Kafka v4](https://docs.snowflake.com/en/connectors/kafkahp/setup-kafka), which writes through high-performance Snowpipe Streaming.

```
telematics.equipment.telemetry.v1      ──► snowflake-sink-telemetry     ──► TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW
telematics.equipment.telemetry.dlq.v1  ──► snowflake-sink-telemetry-dlq ──► TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_DLQ_RAW
                                                     │
                                                     └─ records the sink cannot convert or write ──► telematics.connect.snowflake.dlq.v1
```

The same stack runs in two places:

| | Docker Compose | Minikube |
|---|---|---|
| Use it for | Fast local development | Kubernetes deployment |
| Definition | [docker/docker-compose.yml](../docker/docker-compose.yml) | [kubernetes/](../kubernetes/) |
| Kafka | KRaft, single node | KRaft, single-node StatefulSet |
| Credentials | `.env` passed to the Connect container | `snowflake-credentials` Secret built from `.env` |

## Files

| Path | Purpose |
|---|---|
| `connect/Dockerfile` | Kafka Connect worker image (`cp-kafka-connect` 7.9, i.e. Kafka Connect 3.9) with the Snowflake connector jar, checksum-verified at build time |
| `connectors/*.json` | One config per connector. Secrets are `${env:VAR}` placeholders |
| `register_connectors.py` | Creates or updates every connector through the REST API, then waits for it to reach `RUNNING`. Standard library only |
| `snowflake/setup.sql` | Idempotent Snowflake setup: warehouse, database, `BRONZE` schema, landing tables, freshness view, role, service user, grants |
| `snowflake/generate_keypair.py` | Creates the RSA key pair, writes `SNOWFLAKE_PRIVATE_KEY` to `.env`, and renders `setup.sql` with the public key |

## How secrets are handled

The connector authenticates with an RSA key pair (Snowflake user `TELEMATICS_KAFKA_CONNECTOR`, `TYPE = SERVICE`, so it has no password and no interactive login).

1. `generate_keypair.py` writes the key pair to `secrets/` and the private key body to `.env`. Both are gitignored.
2. Compose passes `.env` to the Connect container. `deploy.sh` turns the `SNOWFLAKE_*` lines of `.env` into a Kubernetes Secret, which the Connect pod loads with `envFrom`.
3. Connector configs reference values as `${env:SNOWFLAKE_PRIVATE_KEY}`. The worker's `EnvVarConfigProvider` resolves them when the task starts. The plaintext key is never sent to the REST API or stored in the `_connect-configs` topic, and `GET /connectors/<name>/config` returns only the placeholder.
4. `register_connectors.py` refuses to register a config that has a literal value for any key that looks sensitive.

## Setup (one time)

From the repo root, with the virtual environment from [Module 1](../01_telemetry_simulator/README.md#run-it):

```bash
# 1. Install Module 2 dependencies (included in the test requirements)
.venv/Scripts/python -m pip install -r testing/python/requirements.txt

# 2. Set SNOWFLAKE_ACCOUNT in .env (see .env.example), then create the key pair
.venv/Scripts/python 02_streaming_ingestion/snowflake/generate_keypair.py
```

3. In Snowsight, open a SQL worksheet as `ACCOUNTADMIN`, paste in `secrets/snowflake_setup.sql` and choose **Run All**. It is safe to run again.

To rotate the key, run `generate_keypair.py --rotate`, re-run `secrets/snowflake_setup.sql`, and restart Kafka Connect (or re-run `deploy.sh`).

## Run on Docker Compose

```bash
docker compose -f docker/docker-compose.yml up -d --build --wait   # Kafka, topics, Connect, Kafka UI
.venv/Scripts/python 02_streaming_ingestion/register_connectors.py  # safe to re-run
.venv/Scripts/python 01_telemetry_simulator/run_simulator.py        # start producing
```

Connectors and their tasks are shown in Kafka UI at http://localhost:8080 under **Kafka Connect**.

## Run on Minikube

```bash
docker compose -f docker/docker-compose.yml down                    # frees ports 9092, 8083 and 8080
minikube start --driver=docker --cpus=2 --memory=6g                 # first time only; existing clusters keep their size
bash kubernetes/deploy.sh                                           # Git Bash on Windows
kubectl -n telematics port-forward svc/kafka 9092:9092              # in a second terminal, then run the simulator as usual
```

`deploy.sh` builds the Connect image inside Minikube, creates the topic-script ConfigMap and the Snowflake Secret from `.env`, applies [the manifests](../kubernetes/) with Kustomize, runs the topic Job, and registers the connectors. Every step is idempotent. Re-running it after a config or key change also restarts Kafka Connect.

## Bronze tables

Both tables use the connector's non-schematized layout. Parsing happens in Silver (Module 4).

| Column | Contents |
|---|---|
| `RECORD_CONTENT` | The Kafka message value, as `VARIANT` |
| `RECORD_METADATA` | `topic`, `partition`, `offset`, `key` (equipment serial number), `CreateTime` (produce time, epoch ms), `SnowflakeConnectorPushTime` |

Rows are append-only. Delivery is at-least-once from the producer, so `event_id` duplicates are expected here. Silver deduplicates them (see the [data contract](../documentation/data_contracts/equipment_telemetry.md)).

## SLA

**Target:** an event is queryable in Bronze within **120 seconds** of being produced to Kafka (`INGESTION_SLA_SECONDS`). The v4 connector typically lands data in 5–10 seconds.

The SLA is checked in two places:

- **Tests.** `testing/python/test_snowflake_ingestion.py` produces one tick for the full fleet and one tick of contract violations, then polls Snowflake until every event has landed or the SLA runs out.
- **Monitoring.** `TELEMATICS.BRONZE.INGESTION_FRESHNESS` shows, per table, the seconds since the newest event and the p95 Kafka-to-Snowflake push latency over the last hour. Module 5 dashboards read this view.

```sql
SELECT * FROM TELEMATICS.BRONZE.INGESTION_FRESHNESS;
```

## Error handling

| Failure | What happens |
|---|---|
| Event breaks the data contract | The producer sends it to the DLQ topic, which lands in `EQUIPMENT_TELEMETRY_DLQ_RAW` |
| Record cannot be converted or written by the sink | Sent to `telematics.connect.snowflake.dlq.v1`, with the error in the record headers (`errors.tolerance = all`, client-side validation) |
| Bad credentials or missing grants | The task fails (`enable.task.fail.on.authorization.errors = true`), and `register_connectors.py` exits non-zero with the stack trace |
| Snowflake unreachable when registering | Connect returns HTTP 400 from its config validation, and the script prints which fields failed |

## Tests

```bash
.venv/Scripts/python -m pytest testing/python/test_connectors.py   # config consistency and registration logic, no services needed
.venv/Scripts/python -m pytest -m snowflake                       # end-to-end SLA and SQL data tests, needs the running stack and Snowflake
```

The SQL data tests on Bronze (structure, Kafka lineage, exactly-once offsets, SLA latency, least-privilege grants) are in [testing/snowflake/](../testing/snowflake/README.md).
