# Snowflake SQL Tests

Data tests that run directly against Snowflake. Each `.sql` file selects the rows that **violate** one expectation, so a test passes when its query returns zero rows. This is the same convention as dbt singular tests. They stay here rather than in the [Module 4 dbt project](../../04_data_warehouse/README.md) because they check the ingestion layer as the connector's service user (including its grants), independently of dbt.

| Test | Checks |
|---|---|
| `bronze_tables_have_expected_columns` | All four Bronze tables (telemetry, DLQ and both CDC tables) exist with exactly `RECORD_METADATA` and `RECORD_CONTENT` (`VARIANT`) |
| `bronze_rows_have_kafka_metadata` | Every row has topic, partition, offset, key and timestamps, and came from the topic mapped to its table |
| `bronze_kafka_offsets_are_unique` | No Kafka offset landed twice (the connector delivers exactly once) |
| `telemetry_envelope_matches_contract` | Main-table rows have every contract envelope field, and the Kafka key equals the serial number |
| `dlq_envelope_is_complete` | DLQ rows have `failed_at`, `contract`, at least one error, and the original payload |
| `push_latency_within_sla` | p95 Kafka-to-Snowflake latency over the last 15 minutes is under `$sla_seconds` |
| `freshness_view_reports_every_table` | `INGESTION_FRESHNESS` covers both tables, with no impossible values |
| `ingest_role_is_least_privilege` | The connector role has no grants beyond those in the Module 2 and 3 `setup.sql` files |
| `cdc_envelope_matches_contract` | CDC rows have a known `op`, `before`/`after` present or null as the op requires, the right source table, an LSN, and a key matching the row |
| `cdc_commit_latency_within_sla` | p95 Postgres-commit-to-Snowflake latency over the last 15 minutes is under `$sla_seconds` |
| `cdc_freshness_view_reports_every_table` | `CDC_FRESHNESS` covers both CDC tables, the initial snapshot has landed, and there are no impossible values |

These tests check **structure and lineage** only. Value-level anomalies such as temperature spikes, nulls or duplicate `event_id`s are expected in Bronze and are flagged or removed in Silver, where the dbt tests cover them. The same goes for CDC: repeated changes and deleted rows are part of the log, and Silver resolves them into current state.

## Run

```bash
.venv/Scripts/python -m pytest testing/python/test_snowflake_sql.py -v
```

The runner ([test_snowflake_sql.py](../python/test_snowflake_sql.py)) connects as the connector's service user and skips everything if Snowflake isn't set up. You can also paste any file into a Snowsight worksheet. For `push_latency_within_sla`, run `SET sla_seconds = 120;` first.

## Add a test

Create `testing/snowflake/<what_must_be_true>.sql`. Start it with a comment describing the expectation, and make its last statement return only the offending rows. The runner picks it up automatically.
