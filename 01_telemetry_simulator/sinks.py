"""Output sinks for telemetry events: Kafka for real runs, stdout for dry runs."""

import json
import logging
import sys

logger = logging.getLogger(__name__)


def serialize(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class StdoutSink:
    """Writes events as JSON lines; DLQ records are prefixed so they are easy to spot."""

    def send(self, key: str, payload: dict) -> None:
        sys.stdout.write(json.dumps(payload) + "\n")

    def send_dlq(self, key: str, record: dict) -> None:
        sys.stdout.write("DLQ " + json.dumps(record) + "\n")

    def flush(self) -> None:
        sys.stdout.flush()


class KafkaSink:
    def __init__(self, bootstrap_servers: str, topic: str, dlq_topic: str):
        from confluent_kafka import Producer

        self.topic = topic
        self.dlq_topic = dlq_topic
        self.delivery_failures = 0
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "telemetry-simulator",
                # Idempotent producer: broker-side dedup of retries, no reordering per partition.
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )
        self._check_topics_exist()

    def _check_topics_exist(self) -> None:
        from confluent_kafka import KafkaException

        try:
            metadata = self.producer.list_topics(timeout=10)
        except KafkaException as exc:
            raise RuntimeError(
                "Could not reach Kafka. Start the stack with "
                "`docker compose -f docker/docker-compose.yml up -d`."
            ) from exc
        missing = {self.topic, self.dlq_topic} - set(metadata.topics)
        if missing:
            raise RuntimeError(
                f"Kafka topics not found: {sorted(missing)}. "
                "Start the stack with `docker compose -f docker/docker-compose.yml up -d`."
            )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.delivery_failures += 1
            logger.error("Delivery failed for key=%s topic=%s: %s", msg.key(), msg.topic(), err)

    def _produce(self, topic: str, key: str, payload: dict) -> None:
        while True:
            try:
                self.producer.produce(
                    topic,
                    key=key.encode("utf-8"),
                    value=serialize(payload),
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Local queue full: serve delivery callbacks to free space, then retry.
                self.producer.poll(1)
        self.producer.poll(0)

    def send(self, key: str, payload: dict) -> None:
        self._produce(self.topic, key, payload)

    def send_dlq(self, key: str, record: dict) -> None:
        self._produce(self.dlq_topic, key, record)

    def flush(self, timeout: float = 30) -> None:
        remaining = self.producer.flush(timeout)
        if remaining:
            logger.error("%d messages were not delivered before shutdown", remaining)
