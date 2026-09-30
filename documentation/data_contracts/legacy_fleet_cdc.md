# Data Contract: Legacy Fleet CDC (v1)

| Item | Value |
|---|---|
| Schema | [legacy_fleet_cdc.v1.schema.json](legacy_fleet_cdc.v1.schema.json) (JSON Schema 2020-12) |
| Producer | Debezium PostgreSQL connector `debezium-source-legacy-fleet` ([Module 3](../../03_cdc_migration/README.md)) |
| Source | Postgres database `telematics_legacy`, schema `fleet`, publication `telematics_cdc_pub`, slot `telematics_cdc_slot` |
| Topics | `telematics.legacy.equipment.cdc.v1` (1 partition), `telematics.legacy.hour_meter_logs.cdc.v1` (3 partitions) |
| Message key | JSON `{"equipment_id": N}` on both topics, so all changes to one machine stay in order within a partition |
| Delivery | At-least-once. After a connector restart, Debezium may re-send changes it had not yet committed an offset for. Consumers deduplicate on `source.lsn` + table + primary key |
| Bronze landing | `TELEMATICS.BRONZE.LEGACY_EQUIPMENT_CDC_RAW`, `TELEMATICS.BRONZE.LEGACY_HOUR_METER_LOGS_CDC_RAW` |
| Ingestion SLA | Queryable in Bronze within 120 seconds of the Postgres commit. Tracked in `BRONZE.CDC_FRESHNESS` |

## Event shape

Each message is the full Debezium change event, serialized as JSON without embedded schemas. Nothing is flattened or dropped: Bronze keeps the envelope as is, and Silver decides how to use it.

```json
{
  "before": {"log_id": 3591, "equipment_id": 40, "hour_meter": "2856.2", "recorded_by": "shift_supervisor", "...": "..."},
  "after":  {"log_id": 3591, "equipment_id": 40, "hour_meter": "2856.4", "recorded_by": "records_clerk", "...": "..."},
  "source": {
    "version": "3.7.0.Final", "connector": "postgresql", "name": "telematics.legacy",
    "ts_ms": 1790724915245, "snapshot": "false", "db": "telematics_legacy",
    "schema": "fleet", "table": "hour_meter_logs", "txId": 812, "lsn": 28391224
  },
  "op": "u",
  "ts_ms": 1790724915301,
  "transaction": null
}
```

| `op` | Meaning | `before` | `after` |
|---|---|---|---|
| `r` | Row read by the initial snapshot | `null` | row |
| `c` | `INSERT` | `null` | row |
| `u` | `UPDATE` | complete previous row | new row |
| `d` | `DELETE` | complete deleted row | `null` |

Both tables use `REPLICA IDENTITY FULL`, so `before` is the complete row on updates and deletes, not just the key. Deletes produce a `d` event and **no** tombstone (`tombstones.on.delete = false`).

## Row images

The columns of each table are listed in the schema (`$defs.equipment_row`, `$defs.hour_meter_log_row`). Postgres types are serialized like this:

| Postgres type | JSON | Example | Silver cast (Snowflake) |
|---|---|---|---|
| `integer`, `bigint` | number | `40` | `::NUMBER` |
| `numeric(9,1)` | string, so no precision is lost (`decimal.handling.mode = string`) | `"2856.4"` | `::NUMBER(9,1)` |
| `timestamptz` | ISO 8601 string in UTC | `"2026-09-19T18:00:00.000000Z"` | `::TIMESTAMP_TZ` |
| `date` | days since 1970-01-01 | `18506` (2020-09-01) | `DATEADD(day, v, '1970-01-01')::DATE` |
| `varchar` | string | `"ACTIVE"` | `::STRING` |

## Guarantees and what Silver must handle

- **Ordering.** Changes to one machine arrive in commit order (same key, same partition). Across partitions there is no order: sort by `source.lsn`.
- **History.** The initial snapshot (`op = r`) comes first, then every committed change. Current state is the latest event per primary key. If that event is a `d`, the row no longer exists.
- **Duplicates.** At-least-once delivery means the same change can land twice. `(source.lsn, source.table, primary key, op)` identifies a change.
- **Snapshot timestamps.** For `r` events, `source.ts_ms` is the snapshot time, not when the row was last changed. Use the row's own `updated_at` for that.

## What the contract enforces

The envelope is **open**. Debezium may add fields in newer versions (for example `ts_us`, `origin`), and consumers must ignore fields they don't know. The row images are **closed** (`additionalProperties: false`): a column added to, removed from or retyped in the source tables fails validation. That makes a schema change in the legacy database a visible contract change rather than silent drift into Silver.

The contract is checked by the CDC integration tests on real events from Kafka, and by [`cdc_envelope_matches_contract.sql`](../../testing/snowflake/cdc_envelope_matches_contract.sql) on rows in Bronze.

## Versioning

- **Minor** (`1.x`): adding a nullable column to a source table, by agreement with Silver. Update the row schema in the same change.
- **Major** (`2.0`): removing, renaming or retyping a column, or changing a key. This gets a new schema file and new topics (`...cdc.v2`, via the connector's `RegexRouter`), and v1 runs alongside it until Silver has migrated.
