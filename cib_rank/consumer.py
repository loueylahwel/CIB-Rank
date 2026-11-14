"""Analysis: consume events from Kafka, score them, store them in HBase,
and expose Prometheus metrics.

Usage:
    python -m cib_rank.consumer --create-tables
"""

from __future__ import annotations

import argparse
import json

from prometheus_client import Gauge, start_http_server

from .config import (HBASE_HOST, HBASE_PORT, KAFKA_BOOTSTRAP, KAFKA_GROUP,
                     KAFKA_TOPIC, METRICS_PORT)
from .pipeline import DuplicateTracker, process_event
from .ranking import rank_topics
from .store import HBaseStore

SUSPICIOUS_RATIO = Gauge(
    "cib_topic_suspicious_ratio", "Share of engagements from suspicious accounts", ["topic"])
TOPIC_TOTAL = Gauge(
    "cib_topic_engagements_total", "Engagements seen per topic", ["topic"])


def run(create_tables: bool) -> None:
    from kafka import KafkaConsumer

    store = HBaseStore(HBASE_HOST, HBASE_PORT)
    if create_tables:
        store.create_tables()

    duplicates = DuplicateTracker()
    start_http_server(METRICS_PORT)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"Consuming '{KAFKA_TOPIC}', metrics on :{METRICS_PORT}")
    for message in consumer:
        process_event(message.value, store, duplicates)
        # Refresh gauges for the topics this process has seen.
        for row in rank_topics(store.topic_totals(), min_engagements=1):
            SUSPICIOUS_RATIO.labels(topic=row["topic"]).set(row["suspicious_ratio"])
            TOPIC_TOTAL.labels(topic=row["topic"]).set(row["total"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--create-tables", action="store_true",
                        help="create the HBase tables if they do not exist")
    args = parser.parse_args()
    run(create_tables=args.create_tables)


if __name__ == "__main__":
    main()
