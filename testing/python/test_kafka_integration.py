"""End-to-end check against the local Docker Kafka stack. Skipped if the broker is unreachable."""

import json
import os
import random
import time
import uuid

import pytest

from anomalies import AnomalyInjector
from run_simulator import run
from sinks import KafkaSink

confluent_kafka = pytest.importorskip("confluent_kafka")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TELEMETRY_TOPIC", "telematics.equipment.telemetry.v1")
DLQ_TOPIC = os.getenv("TELEMETRY_DLQ_TOPIC", "telematics.equipment.telemetry.dlq.v1")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_sink():
    try:
        return KafkaSink(BOOTSTRAP, TOPIC, DLQ_TOPIC)
    except RuntimeError as exc:
        # Locally a missing broker is a skip; in CI the stack is started on purpose, so it's a failure.
        if os.getenv("CI"):
            pytest.fail(str(exc))
        pytest.skip(str(exc))


def consumer_at_end(topic):
    """A consumer positioned at the current end of every partition of the topic."""
    consumer = confluent_kafka.Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": f"it-{uuid.uuid4()}", "enable.auto.commit": False}
    )
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    assignments = []
    for p in partitions:
        tp = confluent_kafka.TopicPartition(topic, p)
        _, high = consumer.get_watermark_offsets(tp, timeout=10)
        assignments.append(confluent_kafka.TopicPartition(topic, p, high))
    consumer.assign(assignments)
    return consumer


def consume(consumer, expected, timeout=20):
    messages, deadline = [], time.monotonic() + timeout
    while len(messages) < expected and time.monotonic() < deadline:
        msg = consumer.poll(1)
        if msg is not None and not msg.error():
            messages.append(msg)
    return messages


def test_tick_round_trips_through_kafka(kafka_sink, simulator, validator):
    consumer = consumer_at_end(TOPIC)
    try:
        run(simulator, AnomalyInjector(rate=0.0, rng=random.Random(0)), validator, kafka_sink, ticks=1, interval=0)
        messages = consume(consumer, expected=40)
    finally:
        consumer.close()

    assert len(messages) == 40
    for msg in messages:
        payload = json.loads(msg.value())
        assert msg.key().decode() == payload["equipment_serial_number"]
        assert payload["sequence_number"] == 1


def test_contract_violation_lands_in_dlq(kafka_sink, simulator, validator):
    consumer = consumer_at_end(DLQ_TOPIC)
    injector = AnomalyInjector(rate=1.0, rng=random.Random(0))
    injector.apply = lambda event: (injector._contract_violation(dict(event)), "CONTRACT_VIOLATION")
    try:
        run(simulator, injector, validator, kafka_sink, ticks=1, interval=0)
        messages = consume(consumer, expected=40)
    finally:
        consumer.close()

    assert len(messages) == 40
    record = json.loads(messages[0].value())
    assert record["contract"] == "equipment_telemetry.v1" and record["errors"]
