# Module 3: Legacy Database Migration & Change Data Capture

A legacy PostgreSQL database (`telematics_legacy`) holds the fleet's equipment master data and its historical hour-meter logs. Debezium migrates the existing rows with an initial snapshot, then streams every `INSERT`, `UPDATE` and `DELETE` into Kafka through `pgoutput` logical decoding. The Snowflake sink from [Module 2](../02_streaming_ingestion/README.md) lands both CDC topics in Bronze.

```
Postgres telematics_legacy                     Kafka                                         Snowflake BRONZE
fleet.equipment        ─┐                    ┌─► telematics.legacy.equipment.cdc.v1       ─┐                        ┌─► LEGACY_EQUIPMENT_CDC_RAW
                        ├─► debezium-source ─┤                                             ├─► snowflake-sink-     ─┤
fleet.hour_meter_logs  ─┘   -legacy-fleet    └─► telematics.legacy.hour_meter_logs.cdc.v1 ─┘   legacy-cdc           └─► LEGACY_HOUR_METER_LOGS_CDC_RAW
   ▲  publication telematics_cdc_pub,
   │  slot telematics_cdc_slot (pgoutput)
legacy_activity.py (simulated legacy app)
```

Events keep the full Debezium envelope (`before`, `after`, `op`, `source`). The event format, type mapping and guarantees are in the [data contract](../documentation/data_contracts/legacy_fleet_cdc.md).

## Files

| Path | Purpose |
|---|---|
| `postgres/init/01_schema.sql` | `fleet` schema, both tables, `updated_at` triggers, `REPLICA IDENTITY FULL`, and the publication |
| `postgres/init/02_roles.sh` | Login roles with passwords from the environment: `debezium_cdc` (replication + `SELECT`) and `legacy_app` (DML only) |
| `postgres/init/03_seed.sql` | Generated seed: the 40 machines from Module 1 plus 90 days of hour-meter history (3,600 rows) |
| `postgres/generate_seed.py` | Renders `03_seed.sql` from `01_telemetry_simulator/equipment_config.py`. A unit test fails if the file is stale |
| `postgres/configure_env.py` | Adds the Module 3 settings to `.env` and generates the Postgres passwords. Never overwrites a value |
| `legacy_activity.py` | Simulates the legacy application: new readings, status and site changes, corrections, and voided duplicates |
| `connectors/debezium-source-legacy-fleet.json` | Debezium PostgreSQL source (initial snapshot, then streaming) |
| `connectors/snowflake-sink-legacy-cdc.json` | Snowflake sink for both CDC topics |
| `snowflake/setup.sql` | CDC Bronze tables, the `CDC_FRESHNESS` view, and grants to the Module 2 connector role |

Both connectors run on the Module 2 Kafka Connect worker. The image ([Dockerfile](../02_streaming_ingestion/connect/Dockerfile)) bundles Debezium 3.7 next to the Snowflake connector, checksum-verified at build time. `register_connectors.py` registers the connectors of both modules.

## Design decisions

| Decision | Why |
|---|---|
| Full envelope in Bronze, no `ExtractNewRecordState` | Bronze stays raw. Silver gets before-images, the op type and the LSN, which it needs for SCD2 history and explicit deletes |
| `REPLICA IDENTITY FULL` on both tables | Update and delete events carry the complete previous row, not just the primary key |
| Publication created in SQL (`publication.autocreate.mode = disabled`) | The CDC user needs no table ownership or `CREATE` rights: only `REPLICATION` plus `SELECT` for the snapshot |
| Topics renamed to `telematics.legacy.<table>.cdc.v1` (`RegexRouter`) | Matches the versioned topic names of the other contracts. Topics are pre-created, since broker auto-creation is off |
| Hour-meter logs keyed by `equipment_id` (`message.key.columns`) | Each machine's changes stay in order within one partition. `log_id` is still in every row |
| `decimal.handling.mode = string` | `numeric(9,1)` hour meters keep their exact value |
| No tombstones | A tombstone is a null value, which would land in Bronze as an empty row. The `d` event already describes the delete |
| Heartbeats every 30 s | Show the connector is alive, and let the slot's confirmed LSN advance when the tables are quiet |
| `max_slot_wal_keep_size = 1GB` | A stopped connector cannot fill the disk with retained WAL. If the slot falls further behind, it is invalidated, and the connector must be re-created to take a new snapshot |

## Setup (one time)

From the repo root, after the [Module 2 setup](../02_streaming_ingestion/README.md#setup-one-time):

```bash
# 1. Add the Module 3 settings to .env and generate the Postgres passwords (safe to re-run)
.venv/Scripts/python 03_cdc_migration/postgres/configure_env.py
```

2. In Snowsight, as `ACCOUNTADMIN`, run `03_cdc_migration/snowflake/setup.sql` (no rendering step, since it has no secrets). It is safe to run again.

## Run on Docker Compose

```bash
docker compose -f docker/docker-compose.yml up -d --build --wait    # adds Postgres; rebuilds Connect with Debezium
.venv/Scripts/python 02_streaming_ingestion/register_connectors.py   # all four connectors; safe to re-run
.venv/Scripts/python 03_cdc_migration/legacy_activity.py            # one legacy-app transaction every 10 s until Ctrl+C
```

On first start Postgres runs the init scripts, and Debezium's snapshot then publishes all 40 machines and 3,600 readings as `op = r` events. To try CDC without Snowflake, register only the source with `--only debezium-source-legacy-fleet`.

To look around, or to change rows by hand and watch the events arrive in Kafka UI (http://localhost:8080), open `psql` inside the container:

```bash
docker compose -f docker/docker-compose.yml exec postgres psql -U telematics_admin -d telematics_legacy
```

| `legacy_activity.py` flag | Env var | Default |
|---|---|---|
| `--interval` | `LEGACY_ACTIVITY_INTERVAL_SECONDS` | `10` |
| `--ticks` | none | run forever |
| `--seed` | none | random |

## Run on Minikube

`bash kubernetes/deploy.sh` now also deploys Postgres ([postgres.yaml](../kubernetes/postgres.yaml)) and registers the CDC connectors. The init scripts are mounted from a ConfigMap. The passwords come from two Secrets built from `.env`: `postgres-credentials` for the database pod, and `postgres-cdc-credentials` (Debezium's user only) for Kafka Connect. To run `legacy_activity.py` against the cluster:

```bash
kubectl -n telematics port-forward svc/postgres 5432:5432
```

## Operations

| Task | How |
|---|---|
| Rotate a Postgres password | Clear it in `.env`, re-run `configure_env.py`, apply it with `docker compose -f docker/docker-compose.yml exec postgres bash /docker-entrypoint-initdb.d/02_roles.sh`, then restart Kafka Connect |
| Check replication lag | `SELECT slot_name, active, wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;` |
| Re-snapshot from scratch | Delete the connector, drop the slot (`SELECT pg_drop_replication_slot('telematics_cdc_slot');`), delete its offsets (`DELETE /connectors/<name>/offsets` after stopping it), then register again |
| Reset the database | `docker compose -f docker/docker-compose.yml down -v` removes the volume. The init scripts run again on the next start |

## SLA

**Target:** a committed change is queryable in Bronze within **120 seconds** (`INGESTION_SLA_SECONDS`), measured from the Postgres commit (`source.ts_ms`). The target is shared with Module 2.

```sql
SELECT * FROM TELEMATICS.BRONZE.CDC_FRESHNESS;
```

The view shows, per table: row counts by op, the time since the last committed change, and the p95 commit-to-Kafka and commit-to-Snowflake latency over the last hour. Snapshot rows are excluded from latency.

## Error handling

| Failure | What happens |
|---|---|
| Postgres unreachable or wrong password | The Debezium task fails, and `register_connectors.py` exits non-zero with the stack trace. Debezium retries retriable connection errors itself |
| Connector stopped for a while | The slot retains WAL (up to 1 GB), and Debezium resumes from its last committed LSN without losing changes |
| Schema change in a source table | The event still flows. The contract tests and `cdc_envelope_matches_contract.sql` flag the row images that no longer match |
| Sink cannot write a record | Same as Module 2: routed to `telematics.connect.snowflake.dlq.v1` with the error in the headers |

A source connector has no dead-letter queue by design (`errors.tolerance` stays `none`). A change it cannot process stops the connector, instead of being silently skipped.

## Tests

```bash
.venv/Scripts/python -m pytest testing/python/test_cdc_config.py   # config consistency, seed, contract, env helper; no services
.venv/Scripts/python -m pytest -m "cdc and not snowflake"          # live: Postgres -> Debezium -> Kafka (needs the Compose stack)
.venv/Scripts/python -m pytest -m "cdc and snowflake"              # end-to-end SLA into Bronze
```

| Test | Checks |
|---|---|
| `test_initial_snapshot_published_every_machine` | Every machine in Postgres arrives as an `r` event with the right key |
| `test_insert_correct_and_void_a_reading` | An insert, a correction and a delete arrive as `c`, `u`, `d` in LSN order, with complete before-images |
| `test_equipment_update_carries_the_previous_row` | Equipment updates carry the old and new values, and the trigger bumps `updated_at` |
| `test_legacy_activity_changes_are_all_captured` | Every change made by `legacy_activity.py` reaches Kafka and matches the contract |
| `test_reading_life_cycle_lands_in_bronze_within_sla` | An insert and a delete land in Snowflake within the SLA, measured from the commit |

The SQL data tests on the CDC Bronze tables are in [testing/snowflake/](../testing/snowflake/README.md). CI runs the live CDC tests on every push, using throwaway passwords and only the Debezium connector.
