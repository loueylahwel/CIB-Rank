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
AVG_SENTIMENT = Gauge(
    "cib_topic_avg_sentiment", "Average sentiment of engaged tweets, -1 to 1", ["topic"])

# A full HBase scan is expensive, so the gauges are refreshed every
# REFRESH_EVERY events, and also whenever the topic goes quiet.
REFRESH_EVERY = 50
POLL_TIMEOUT_MS = 1000


def refresh_metrics(store) -> None:
    for row in rank_topics(store.topic_totals(), min_engagements=1):
        SUSPICIOUS_RATIO.labels(topic=row["topic"]).set(row["suspicious_ratio"])
        TOPIC_TOTAL.labels(topic=row["topic"]).set(row["total"])
        AVG_SENTIMENT.labels(topic=row["topic"]).set(row["avg_sentiment"])


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
    print(f"Consuming '{KAFKA_TOPIC}', metrics on :{METRICS_PORT}", flush=True)
    refresh_metrics(store)  # show what is already in HBase right away

    since_refresh = 0
    processed = 0
    while True:
        batches = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
        for records in batches.values():
            for record in records:
                process_event(record.value, store, duplicates)
                processed += 1
                since_refresh += 1
                if since_refresh >= REFRESH_EVERY:
                    refresh_metrics(store)
                    since_refresh = 0
        if not batches and since_refresh:
            refresh_metrics(store)  # stream went quiet: publish the last partial batch
            since_refresh = 0
            print(f"Processed {processed} events so far", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--create-tables", action="store_true",
                        help="create the HBase tables if they do not exist")
    args = parser.parse_args()
    run(create_tables=args.create_tables)


if __name__ == "__main__":
    main()
