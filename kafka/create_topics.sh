#!/bin/bash
# Idempotently creates the telemetry and CDC topics. Safe to run any number of times.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

create_topic() {
  local name="$1" partitions="$2" retention_ms="$3"
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$retention_ms" \
    --config cleanup.policy=delete
  echo "topic ready: $name"
}

# Main telemetry stream: 6 partitions keyed by equipment serial, 7-day retention.
create_topic telematics.equipment.telemetry.v1 6 604800000
# Contract violations: low volume, 14-day retention for investigation.
create_topic telematics.equipment.telemetry.dlq.v1 1 1209600000
# Kafka Connect errors (records the Snowflake sink could not convert or write), 14-day retention.
create_topic telematics.connect.snowflake.dlq.v1 1 1209600000

# Module 3: Debezium CDC from the legacy Postgres database (full change-event envelope).
# Equipment master data: 40 rows, 1 partition, so every change is in commit order.
create_topic telematics.legacy.equipment.cdc.v1 1 604800000
# Hour-meter logs: keyed by equipment_id, so each machine's changes stay in order.
create_topic telematics.legacy.hour_meter_logs.cdc.v1 3 604800000
# Debezium heartbeats (connector liveness; lets the replication slot advance when tables are quiet).
create_topic __debezium-heartbeat.telematics.legacy 1 86400000

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
