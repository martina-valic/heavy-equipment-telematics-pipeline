# Data Contract: Equipment Telemetry (v1)

| Item | Value |
|---|---|
| Schema | [equipment_telemetry.v1.schema.json](equipment_telemetry.v1.schema.json) (JSON Schema 2020-12) |
| Producer | `01_telemetry_simulator` |
| Topic | `telematics.equipment.telemetry.v1` |
| Dead-letter topic | `telematics.equipment.telemetry.dlq.v1` |
| Message key | `equipment_serial_number` (keeps per-machine ordering within a partition) |
| Cadence | One event per machine every 5 seconds (40 machines, about 8 events/s) |
| Delivery | At-least-once. Consumers deduplicate on `event_id` |
| Bronze landing | `TELEMATICS.BRONZE.EQUIPMENT_TELEMETRY_RAW`; DLQ records in `EQUIPMENT_TELEMETRY_DLQ_RAW` ([Module 2](../../02_streaming_ingestion/README.md)) |
| Ingestion SLA | Queryable in Bronze within 120 seconds of being produced to Kafka. Tracked in `BRONZE.INGESTION_FRESHNESS` |

## What the contract enforces

The producer validates every event before sending it. The contract covers structure and types, not business ranges:

- All envelope fields are present, with no unknown fields.
- `type_name`, `category`, `operating_state`, `unit` and `subsystem` are closed enums.
- `value` is a number, or a string when `unit = CODE`. `BOOL` values are `0` or `1`. `null` is allowed and means the sensor did not report.

Values that are out of range but have a valid type, such as a 900 °C oil temperature or a negative pressure, **pass the contract on purpose**. They land in Bronze and are flagged by the Silver-tier dbt tests.

Events that fail validation are **never** produced to the main topic. The producer wraps them in an envelope and sends them to the DLQ topic:

```json
{
  "failed_at": "2026-09-29T12:00:00.000000+00:00",
  "contract": "equipment_telemetry.v1",
  "errors": ["$: 'equipment_serial_number' is a required property"],
  "original_payload": { "...": "..." }
}
```

## Event shape

```json
{
  "schema_version": "1.0.0",
  "event_id": "5b0f3c1e-8d7a-4c1b-9d3e-2f6a1b7c9e10",
  "event_timestamp": "2026-09-29T12:00:05.012345+00:00",
  "sequence_number": 42,
  "equipment_id": 17,
  "equipment_serial_number": "SCOOP-001",
  "type_name": "Scooptram_LHD",
  "category": "BEV",
  "operating_state": "OPERATIONAL",
  "readings": [
    {"tag": "NS=2;s=Engine_Oil_Temp", "value": 94.21, "unit": "DEGC", "subsystem": "Powertrain"},
    {"tag": "NS=2;s=Location_Handshake_Zone", "value": "STOPE_402_HEADING", "unit": "CODE", "subsystem": "Location"}
  ]
}
```

Readings use a long (tag/value) format, so each machine type can report its own signal set without changing the schema. Silver flattens them with `LATERAL FLATTEN`.

## Injected data-quality anomalies

About 5–10% of events (`ANOMALY_RATE`, default 7.5%) carry one of these defects:

| Anomaly | Passes contract? | Expected catch |
|---|---|---|
| `TEMPERATURE_SPIKE`: a `DEGC` reading jumps to 250–999 | Yes | Silver range test |
| `PRESSURE_DROP`: a `PSI` reading drops to between -50 and 0 | Yes | Silver range test |
| `NULL_READINGS`: 1–3 values set to `null` | Yes | Silver `not_null` / completeness test |
| `MISSING_READINGS`: 2–5 readings dropped | Yes | Silver expected-signal-count test |
| `DUPLICATE_EVENT`: the previous event is re-sent with the same `event_id` | Yes | Silver `unique` test on `event_id` |
| `CONTRACT_VIOLATION`: a required field is removed or a numeric value becomes a string | **No** | Sent to the DLQ topic |

## Versioning

- **Minor** (`1.x.0`): adding an optional field, or adding a new enum value by agreement with consumers.
- **Major** (`2.0.0`): removing or renaming a field, or changing a type. Major versions get a new file (`equipment_telemetry.v2.schema.json`) and a new topic (`...telemetry.v2`).
