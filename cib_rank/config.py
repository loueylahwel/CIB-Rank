"""Settings, read from environment variables with local defaults."""

import os

KAFKA_BOOTSTRAP = os.getenv("CIB_KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("CIB_KAFKA_TOPIC", "engagements")
KAFKA_GROUP = os.getenv("CIB_KAFKA_GROUP", "cib-rank-analysis")

HBASE_HOST = os.getenv("CIB_HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("CIB_HBASE_PORT", "9090"))

METRICS_PORT = int(os.getenv("CIB_METRICS_PORT", "8000"))
