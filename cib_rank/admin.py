"""Small admin helpers used by scripts/demo.sh.

Usage:
    python -m cib_rank.admin wait      # wait until HBase Thrift and Kafka answer
    python -m cib_rank.admin reset     # drop + recreate HBase tables, delete the Kafka topic
    python -m cib_rank.admin settle    # wait until the topic counters stop changing
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from .config import HBASE_HOST, HBASE_PORT, KAFKA_BOOTSTRAP, KAFKA_TOPIC
from .store import TABLES, HBaseStore


def _try(fn) -> bool:
    try:
        fn()
        return True
    except Exception:  # noqa: BLE001 - any failure means "not ready yet"
        return False


def wait_ready(timeout: float = 180) -> None:
    from kafka import KafkaAdminClient

    deadline = time.time() + timeout
    hbase_ok = kafka_ok = False
    while time.time() < deadline:
        hbase_ok = hbase_ok or _try(lambda: HBaseStore(HBASE_HOST, HBASE_PORT).connection.tables())
        kafka_ok = kafka_ok or _try(
            lambda: KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP).close())
        if hbase_ok and kafka_ok:
            print("HBase and Kafka are ready")
            return
        time.sleep(3)
    sys.exit(f"Timed out: hbase_ready={hbase_ok} kafka_ready={kafka_ok}")


def reset() -> None:
    from kafka import KafkaAdminClient
    from kafka.errors import UnknownTopicOrPartitionError

    store = HBaseStore(HBASE_HOST, HBASE_PORT)
    existing = set(store.connection.tables())
    for name in TABLES:
        if name.encode() in existing:
            store.connection.delete_table(name, disable=True)
    store.create_tables()
    print("HBase tables recreated")

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        admin.delete_topics([KAFKA_TOPIC])
        print(f"Kafka topic '{KAFKA_TOPIC}' deleted")
        time.sleep(3)  # topic deletion is asynchronous
    except UnknownTopicOrPartitionError:
        print(f"Kafka topic '{KAFKA_TOPIC}' did not exist")
    finally:
        admin.close()


def settle(quiet_seconds: float = 10, timeout: float | None = None) -> None:
    """Return once the summed topic counters have not changed for `quiet_seconds`."""
    timeout = timeout or float(os.getenv("CIB_SETTLE_TIMEOUT", "1800"))
    store = HBaseStore(HBASE_HOST, HBASE_PORT)
    deadline = time.time() + timeout
    next_report = time.time() + 30
    last, last_change = -1, time.time()
    while time.time() < deadline:
        total = sum(t["total"] for t in store.topic_totals().values())
        if total != last:
            last, last_change = total, time.time()
        if time.time() >= next_report:
            print(f"  ...{total} topic engagements counted so far", flush=True)
            next_report = time.time() + 30
        if total > 0 and time.time() - last_change >= quiet_seconds:
            print(f"Processing settled at {total} topic engagements")
            return
        time.sleep(1)
    sys.exit("Timed out waiting for processing to settle")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["wait", "reset", "settle"])
    args = parser.parse_args()
    {"wait": wait_ready, "reset": reset, "settle": settle}[args.command]()


if __name__ == "__main__":
    main()
