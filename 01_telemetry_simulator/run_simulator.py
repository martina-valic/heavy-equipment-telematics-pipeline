"""Entry point: simulate the fleet, validate against the data contract, and publish to Kafka.

Usage:
    python run_simulator.py                   # stream to Kafka until Ctrl+C
    python run_simulator.py --dry-run --ticks 1 --interval 0
"""

import argparse
import logging
import os
import random
import time
from collections import Counter

from dotenv import load_dotenv

from anomalies import AnomalyInjector
from contract import build_dlq_record, load_validator, validate
from simulator import FleetSimulator
from sinks import KafkaSink, StdoutSink

logger = logging.getLogger("telemetry_simulator")


def run(simulator, injector, validator, sink, ticks: int | None, interval: float) -> Counter:
    """Run the emit loop and return totals: sent, dlq, and a count per anomaly kind."""
    totals = Counter()
    next_tick = time.monotonic()
    tick = 0
    try:
        while ticks is None or tick < ticks:
            tick += 1
            tick_stats = Counter()
            for event in simulator.tick():
                key = event["equipment_serial_number"]
                outgoing, anomaly = injector.apply(event)
                if anomaly:
                    tick_stats[anomaly] += 1
                for payload in outgoing:
                    errors = validate(validator, payload)
                    if errors:
                        sink.send_dlq(key, build_dlq_record(payload, errors))
                        tick_stats["dlq"] += 1
                    else:
                        sink.send(key, payload)
                        tick_stats["sent"] += 1
            totals.update(tick_stats)
            logger.info("tick=%d %s", tick, dict(sorted(tick_stats.items())))

            next_tick += interval
            if ticks is None or tick < ticks:
                time.sleep(max(0.0, next_tick - time.monotonic()))
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
    finally:
        sink.flush()
    logger.info("totals %s", dict(sorted(totals.items())))
    return totals


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="print events to stdout instead of Kafka")
    parser.add_argument("--ticks", type=int, default=None, help="stop after N ticks (default: run forever)")
    parser.add_argument("--interval", type=float, default=float(os.getenv("EMIT_INTERVAL_SECONDS", "5")))
    parser.add_argument("--anomaly-rate", type=float, default=float(os.getenv("ANOMALY_RATE", "0.075")))
    seed = os.getenv("SIMULATOR_SEED")
    parser.add_argument("--seed", type=int, default=int(seed) if seed else None)
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    rng = random.Random(args.seed)
    simulator = FleetSimulator(interval_seconds=args.interval, rng=rng)
    injector = AnomalyInjector(rate=args.anomaly_rate, rng=rng)
    validator = load_validator()

    if args.dry_run:
        sink = StdoutSink()
    else:
        sink = KafkaSink(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("TELEMETRY_TOPIC", "telematics.equipment.telemetry.v1"),
            dlq_topic=os.getenv("TELEMETRY_DLQ_TOPIC", "telematics.equipment.telemetry.dlq.v1"),
        )

    logger.info(
        "Simulating %d machines every %.1fs (anomaly rate %.1f%%, %s)",
        len(simulator.machines), args.interval, args.anomaly_rate * 100,
        "dry run" if args.dry_run else "publishing to Kafka",
    )
    run(simulator, injector, validator, sink, ticks=args.ticks, interval=args.interval)


if __name__ == "__main__":
    main()
