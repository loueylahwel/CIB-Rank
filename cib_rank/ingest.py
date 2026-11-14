"""Ingestion: read engagement events from a CSV file and publish them to Kafka.

Usage:
    python -m cib_rank.ingest data/sample_engagements.csv --bootstrap localhost:9092
"""

from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterator

from .config import KAFKA_TOPIC

EVENT_FIELDS = [
    "event_id", "timestamp", "event_type", "tweet_id", "tweet_text", "author_id",
    "actor_id", "actor_username", "actor_created_at", "actor_followers",
    "actor_following", "actor_statuses", "actor_default_avatar",
    "reply_latency_seconds",
]


def iter_csv(path: str) -> Iterator[dict]:
    """Yield one dict per CSV row. Extra columns (e.g. `is_bot_truth`) are kept."""
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = set(EVENT_FIELDS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV is missing columns: {sorted(missing)}")
        yield from reader


def main() -> None:
    from kafka import KafkaProducer  # kafka-python

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("csv_path")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    sent = 0
    for event in iter_csv(args.csv_path):
        producer.send(args.topic, event)
        sent += 1
    producer.flush()
    print(f"Sent {sent} events to topic '{args.topic}'")


if __name__ == "__main__":
    main()
