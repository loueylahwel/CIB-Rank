#!/usr/bin/env bash
# One-command CIB-Rank demo:
#   1. start the Docker stack        4. publish the sample CSV to Kafka
#   2. wait for HBase and Kafka      5. wait for processing, print the ranking
#   3. reset data, start consumer    6. leave consumer + dashboards running
#
# Usage:  scripts/demo.sh [path/to/events.csv]
# Stop:   kill "$(cat .consumer.pid)" ; docker compose down
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-.venv/bin/python}"
CSV="${1:-data/sample_engagements.csv}"

if [ ! -x "$PY" ]; then
  echo "No virtualenv found. Run: python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'" >&2
  exit 1
fi

echo "==> Starting the Docker stack"
docker compose up -d

echo "==> Waiting for HBase and Kafka"
"$PY" -m cib_rank.admin wait

if [ ! -f "$CSV" ]; then
  echo "==> Generating synthetic sample data"
  "$PY" -m scripts.generate_sample --out "$CSV"
fi

echo "==> Resetting HBase tables and the Kafka topic (clean demo run)"
if [ -f .consumer.pid ] && kill -0 "$(cat .consumer.pid)" 2>/dev/null; then
  kill "$(cat .consumer.pid)"
fi
"$PY" -m cib_rank.admin reset

echo "==> Starting the consumer (log: consumer.log)"
CIB_KAFKA_GROUP="cib-rank-demo-$(date +%s)" nohup "$PY" -u -m cib_rank.consumer > consumer.log 2>&1 &
echo $! > .consumer.pid
sleep 3

echo "==> Publishing $CSV to Kafka"
"$PY" -m cib_rank.ingest "$CSV"

echo "==> Waiting for the consumer to finish"
"$PY" -m cib_rank.admin settle

echo
"$PY" -m cib_rank.report --min-engagements 20
echo
echo "Grafana:    http://localhost:${CIB_GRAFANA_PORT:-3030}/d/cib-rank   (admin / admin)"
echo "Prometheus: http://localhost:9091"
echo "HBase UI:   http://localhost:16010"
echo "The consumer keeps running (pid $(cat .consumer.pid)). Stop it with: kill \$(cat .consumer.pid)"
