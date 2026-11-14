"""Print the topic ranking from HBase.

Usage:
    python -m cib_rank.report --min-engagements 20 --top 10
"""

from __future__ import annotations

import argparse

from .config import HBASE_HOST, HBASE_PORT
from .ranking import rank_topics
from .store import HBaseStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--min-engagements", type=int, default=20)
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    store = HBaseStore(HBASE_HOST, HBASE_PORT)
    rows = rank_topics(store.topic_totals(), args.min_engagements)[: args.top]
    print(f"{'topic':<24}{'engagements':>12}{'suspicious':>12}{'ratio':>8}{'sentiment':>11}")
    for row in rows:
        print(f"#{row['topic']:<23}{row['total']:>12}{row['suspicious']:>12}"
              f"{row['suspicious_ratio']:>8.3f}{row['avg_sentiment']:>11.3f}")


if __name__ == "__main__":
    main()
