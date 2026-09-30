# Module 4: Modern Data Warehouse & Medallion Transformation

A dbt project turns the raw Kafka records in Snowflake Bronze into a typed, deduplicated Silver tier and a Kimball star schema in Gold. It covers both the IoT telemetry stream ([Module 2](../02_streaming_ingestion/README.md)) and the legacy Postgres CDC log ([Module 3](../03_cdc_migration/README.md)). dbt runs every 5 minutes as a Kubernetes CronJob, on demand through Docker Compose, or from the venv while developing.

```
BRONZE (raw VARIANT, append-only)      SILVER (typed, deduplicated, flagged)         GOLD (star schema + marts)
EQUIPMENT_TELEMETRY_RAW ──► stg_telemetry__events ─► telemetry_readings ─┬─► fct_equipment_signal_hourly ─┐
                                                   └► telemetry_events ──┼─► fct_equipment_hourly ────────┤
EQUIPMENT_TELEMETRY_DLQ_RAW ► stg_telemetry__dlq ──► telemetry_contract_violations ─► fct_telemetry_quality_hourly
LEGACY_EQUIPMENT_CDC_RAW ──► stg_legacy__equipment_changes ─► legacy_equipment_changes ─► equipment_history ─► dim_equipment (SCD2)
LEGACY_HOUR_METER_LOGS_CDC_RAW ► stg_legacy__hour_meter_log_changes ─► legacy_hour_meter_log_changes ─► hour_meter_logs ─► fct_hour_meter_readings
seeds/signal_catalog.csv ───────────────────────────────────────────────────────────────────────► dim_signal      dim_date
                                                                              equipment_health ◄── all of the above
```

## Tiers

| Tier | Schema | What it holds | Built as |
|---|---|---|---|
| Bronze | `BRONZE` | One row per Kafka record, payload as `VARIANT`. Written by Kafka Connect, never updated | dbt [sources](models/staging/_bronze__sources.yml) with freshness checks |
| Staging | `SILVER` (`stg_*`) | Bronze with the envelope parsed and typed. Still one row per Kafka record, so duplicates are still present | Views |
| Silver | `SILVER` | Deduplicated, typed and quality-flagged telemetry; deduplicated CDC logs; SCD2 equipment history; current hour-meter logs | Incremental merges (telemetry, change logs), tables (history, current state) |
| Gold | `GOLD` | Star schema (3 dimensions, 4 facts) and the `equipment_health` mart, each with an enforced contract | Incremental delete+insert per hour (hourly facts), tables |

### Silver

| Model | Grain | Notes |
|---|---|---|
| `telemetry_readings` | event × signal | The readings array flattened and checked against the [signal catalog](#signal-catalog): `is_missing_value`, `is_out_of_range`, `is_expected_signal` |
| `telemetry_events` | unique `event_id` | The envelope plus a quality summary: `missing_signal_count`, `is_incomplete`, `has_quality_issue` |
| `telemetry_contract_violations` | DLQ record | Events the producer rejected, with their errors |
| `legacy_equipment_changes`, `legacy_hour_meter_log_changes` | change (`lsn`, key, `op`) | CDC logs, deduplicated (Debezium delivers at least once) |
| `equipment_history` | equipment version | SCD Type 2 from the CDC log. A delete is a version with `is_deleted` |
| `hour_meter_logs` | `log_id` | Current state: corrections applied, voided (deleted) readings removed |

### Gold

| Model | Type | Grain |
|---|---|---|
| `dim_equipment` | SCD2 dimension | equipment version (`effective_from` ≤ t < `effective_to`), plus unknown member `'-1'` |
| `dim_signal` | Dimension | equipment type × signal, plus unknown member `'-1'` |
| `dim_date` | Dimension | day (`date_key` = YYYYMMDD), 2015–2030 |
| `fct_equipment_signal_hourly` | Periodic snapshot fact | hour × equipment version × signal: sample counts, min/max/avg/sum of valid values |
| `fct_equipment_hourly` | Periodic snapshot fact | hour × equipment version: idle/operating/fault hours, utilization, availability |
| `fct_hour_meter_readings` | Transaction fact | legacy hour-meter reading |
| `fct_telemetry_quality_hourly` | Periodic snapshot fact | hour: delivered, unique, duplicate, rejected, incomplete and clean events |
| `equipment_health` | Mart | machine: health status and reasons, fault and anomaly rates over 24 h, service due |

Facts join `dim_equipment` **as of the event time**, so a machine's site, status and model are reported as they were when the reading was taken. Each machine's first version is effective from 1900-01-01, so telemetry recorded before the legacy snapshot still finds a version.

`equipment_health.health_status` takes the first rule that matches: `DECOMMISSIONED`, then `CRITICAL` (in fault now, more than 20% fault events in 24 h, or service overdue), then `OFFLINE` (no telemetry for 15 min), then `WARNING` (service due within 10% of its interval, or out-of-range readings in more than 5% of events), else `HEALTHY`. `health_reasons` lists every rule that applies. The thresholds are dbt vars in [dbt_project.yml](dbt_project.yml).

## Files

| Path | Purpose |
|---|---|
| `dbt_project.yml`, `profiles.yml`, `packages.yml`, `package-lock.yml` | Project config and vars; env-var-only profile (no secrets); dbt_utils, pinned |
| `models/staging/` | Bronze sources and the four `stg_*` views |
| `models/silver/`, `models/gold/` | Models, their docs and tests (`_silver.yml`, `_gold.yml`), and dbt unit tests |
| `macros/` | Schema naming, incremental watermark and merge window, point-in-time join, two generic tests |
| `tests/` | Singular tests: SCD2 integrity and Gold-to-Silver reconciliation |
| `seeds/signal_catalog.csv` | Expected signals and ranges per equipment type. Generated, see below |
| `generate_signal_catalog.py` | Renders the seed from `01_telemetry_simulator/equipment_config.py`. A unit test fails if it is stale |
| `run_dbt.py` | Runs dbt with the settings from the repo-root `.env` |
| `snowflake/setup.sql`, `snowflake/generate_dbt_keypair.py` | Silver/Gold schemas, transform role, dbt service user; its key pair |
| `Dockerfile` | dbt image for Compose and the CronJob (non-root, packages baked in) |

## Design decisions

| Decision | Why |
|---|---|
| Own service user and role (`TELEMATICS_DBT`, `TELEMATICS_TRANSFORM_ROLE`) | Least privilege in both directions: dbt can only read Bronze, and the connector cannot touch Silver or Gold. Separate keys rotate independently |
| Incremental on `ingested_at` with a 15-minute lookback | Bronze load time only moves forward, so it is a safe watermark even for late events. Re-reading the overlap is harmless because every write is a merge or a delete+insert |
| First delivery wins (`merge_update_columns` = the key) | A redelivered or reused `event_id` can't overwrite what Silver already holds. The contract says `event_id` is unique |
| Merges scan only the last day of the target (`incremental_predicates`) | Keeps each 5-minute merge cheap as telemetry grows. Redeliveries arrive within seconds |
| Anomalies flagged, not dropped | Nothing is silently lost. Gold statistics exclude flagged values, and the quality fact shows how many there were |
| Anomaly tests are warnings with a rate threshold | Anomalies are expected at about 1.5% per type. A warning fires when a flag passes 5% over 24 h, which points at a real sensor or pipeline problem. Structural tests (keys, types, relationships, SCD2 integrity) are errors |
| SCD2 from the CDC log, not dbt snapshots | The log holds every committed change with before and after images, including changes between dbt runs. Snapshots would only see the state at each run |
| History and current state rebuilt in full each run | The CDC logs are small, and a full rebuild stays correct when changes arrive out of order |
| Hourly facts recompute whole hours (delete+insert) | Late readings land in the right hour, and an hour's totals always match Silver (checked by `gold_hourly_facts_reconcile_with_silver`) |
| Enforced contracts on every Gold model | Gold is the interface for Grafana (Module 5): a renamed column or changed type fails the build instead of breaking a dashboard |
| Signal catalog generated from the simulator config | One source of truth for which signals exist and their ranges. The FAULT range counts as expected, since FAULT is a legitimate state |

### Signal catalog

`seeds/signal_catalog.csv` has one row per equipment type and signal. `min_expected`/`max_expected` span all operating states, CODE signals list their `allowed_values`, and cumulative counters (engine hours, uptime) have no upper bound. After changing `SIGNAL_REGISTRY`, regenerate it:

```bash
.venv/Scripts/python 04_data_warehouse/generate_signal_catalog.py
```

A reading whose tag is not in the catalog fails the `is_expected_signal` test (an error), because Silver would otherwise treat a new signal as unknown.

## Setup (one time)

From the repo root, after the Module 2 and 3 setup:

```bash
# 1. dbt and its adapter (into the same venv)
.venv/Scripts/pip install -r 04_data_warehouse/requirements.txt

# 2. dbt's key pair: writes secrets/snowflake_dbt_rsa_key.p8, sets DBT_SNOWFLAKE_PRIVATE_KEY in .env,
#    and renders secrets/snowflake_dbt_setup.sql (safe to re-run; --rotate for a new key)
.venv/Scripts/python 04_data_warehouse/snowflake/generate_dbt_keypair.py

# 3. dbt packages
.venv/Scripts/python 04_data_warehouse/run_dbt.py deps
```

4. In Snowsight, as `ACCOUNTADMIN`, run `secrets/snowflake_dbt_setup.sql`. It is safe to run again.

## Run

```bash
.venv/Scripts/python 04_data_warehouse/run_dbt.py build                    # everything, including unit tests
.venv/Scripts/python 04_data_warehouse/run_dbt.py build --select +equipment_health
.venv/Scripts/python 04_data_warehouse/run_dbt.py source freshness
.venv/Scripts/python 04_data_warehouse/run_dbt.py build --full-refresh     # rebuild incremental models from Bronze
```

On **Docker Compose** the `dbt` service runs only on demand:

```bash
docker compose -f docker/docker-compose.yml run --rm --build dbt           # the scheduled build
docker compose -f docker/docker-compose.yml run --rm dbt test --select silver
```

On **Minikube**, `bash kubernetes/deploy.sh` builds the image, creates the `dbt-snowflake-credentials` Secret (dbt's keys only), and deploys the `dbt-build` CronJob ([dbt-cronjob.yaml](../kubernetes/dbt-cronjob.yaml)). It runs `dbt build` every 5 minutes, skipping unit tests, and never runs two builds at once.

| Task | How |
|---|---|
| Run now | `kubectl -n telematics create job dbt-build-manual-$(date +%s) --from=cronjob/dbt-build` |
| Follow the log | `kubectl -n telematics logs -f -l app=dbt --tail=-1` |
| Pause (saves warehouse credits) | `kubectl -n telematics patch cronjob dbt-build -p '{"spec":{"suspend":true}}'` (`false` to resume) |
| Rotate dbt's key | `generate_dbt_keypair.py --rotate`, re-run `secrets/snowflake_dbt_setup.sql`, re-run `deploy.sh` |

## Freshness

Bronze lands within the 120-second ingestion SLA ([Modules 2](../02_streaming_ingestion/README.md#sla) and [3](../03_cdc_migration/README.md#sla)). Silver and Gold follow on the 5-minute schedule, so Gold is at most about **7 minutes** behind an event (5 minutes between runs, plus the build). `dbt source freshness` warns when telemetry in Bronze is more than 5 minutes old and errors after 30. That usually means the simulator or the sink has stopped.

The warehouse auto-suspends after 60 seconds, but a run every 5 minutes keeps it busy for roughly a third of the time. Pause the CronJob when the pipeline isn't in use.

## Tests

| Kind | Where | Runs |
|---|---|---|
| Structural data tests (error) | `unique`, `not_null`, `accepted_values`, `relationships`, grain checks in the model YAML files | Every `dbt build`. On the large telemetry tables they check the last day of loads, and Gold facts the last 7 days |
| Anomaly rate tests (warn) | `anomaly_rate_at_most`, `duplicate_rate_at_most` ([macros/tests/](macros/tests/)) | Every `dbt build` |
| Singular tests | [tests/](tests/): SCD2 versions are contiguous, one dimension version at any time, hourly facts reconcile with Silver | Every `dbt build` |
| dbt unit tests | `_silver.yml`: SCD2 versioning, corrections and voids, anomaly flags | `dbt build` or `dbt test --select test_type:unit`, not in the CronJob |
| Gold contracts | `_gold.yml`: every column's name and type | Every build of a Gold model |
| Python config tests | [test_warehouse_config.py](../testing/python/test_warehouse_config.py): catalog is current, grants are least privilege, image tags and secrets are consistent | `pytest`, and in CI |

CI has no Snowflake credentials. It runs the Python tests, then `dbt deps` and `dbt parse --warn-error`, and builds the image.

Run only the anomaly reports:

```bash
.venv/Scripts/python 04_data_warehouse/run_dbt.py test --select test_name:anomaly_rate_at_most test_name:duplicate_rate_at_most
```

## Error handling

| Failure | What happens |
|---|---|
| A structural test fails | `dbt build` skips the models downstream of the failing one; everything else still builds. The CronJob's pod exits non-zero and shows as failed |
| A Gold column's type changes | The contract check fails that model before it writes anything, so Gold keeps its last good version |
| A run fails or is skipped | The next run's watermark still starts from the last row written, so it catches up. Nothing is lost |
| A new signal appears in telemetry | `is_expected_signal` fails; regenerate the signal catalog |
| Bronze is re-loaded or a model's logic changes | `run_dbt.py build --full-refresh --select <model>+` rebuilds from Bronze, which keeps every record |
