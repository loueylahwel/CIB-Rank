"""Storage backends.

`HBaseStore` is the real backend (Apache HBase through the Thrift gateway).
`MemoryStore` has the same methods and is used by the tests and for quick
local runs without a cluster.

Idempotency: `add_engagement` returns False when the engagement row already
exists, and the pipeline only increments the topic counters for new rows.
Replaying Kafka messages therefore does not double-count. (With several
consumers writing the same event at the same moment, a duplicate could still
slip through; one consumer per partition, as Kafka groups do, avoids that.)
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from . import keys
from .scoring import Account

TABLES = {
    "accounts": ["p", "s"],
    "tweets": ["t", "n"],
    "engagements": ["e"],
    "topic_stats": ["s"],
}

# Sentiment is stored as an integer counter in thousandths (HBase counters are ints).
_SENTIMENT_SCALE = 1000


def _empty_totals() -> dict:
    return {"total": 0, "suspicious": 0, "sentiment_sum": 0.0}


class MemoryStore:
    """In-process store with the same interface as HBaseStore."""

    def __init__(self) -> None:
        self.accounts: dict[str, dict] = {}
        self.tweets: dict[str, dict] = {}
        self.engagements: dict[bytes, dict] = {}
        self.topic_stats: dict[bytes, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def upsert_account(self, account: Account, score: float, coordination: float = 0.0) -> None:
        self.accounts[account.user_id] = {"username": account.username, "bot_score": score,
                                          "coordination": coordination}

    def upsert_tweet(self, tweet_id: str, text: str, author_id: str,
                     ts: datetime, topics: list[str], sentiment: float = 0.0) -> None:
        self.tweets[tweet_id] = {"text": text, "author_id": author_id,
                                 "created_at": ts.isoformat(), "topics": topics,
                                 "sentiment": sentiment}

    def add_engagement(self, topic: str, ts: datetime, event_id: str,
                       event_type: str, actor_id: str, score: float) -> bool:
        key = keys.engagement_key(topic, ts, event_id)
        if key in self.engagements:
            return False
        self.engagements[key] = {"type": event_type, "actor_id": actor_id,
                                 "actor_bot_score": score}
        return True

    def count_engagement(self, topic: str, ts: datetime, suspicious: bool,
                         sentiment: float = 0.0) -> None:
        row = self.topic_stats[keys.topic_stats_key(topic, ts)]
        row["total"] += 1
        if suspicious:
            row["suspicious"] += 1
        row["sentiment_milli"] += round(sentiment * _SENTIMENT_SCALE)

    def topic_totals(self) -> dict[str, dict]:
        totals: dict[str, dict] = defaultdict(_empty_totals)
        for key, counts in self.topic_stats.items():
            topic = key.decode().split("|")[0]
            totals[topic]["total"] += counts["total"]
            totals[topic]["suspicious"] += counts["suspicious"]
            totals[topic]["sentiment_sum"] += counts["sentiment_milli"] / _SENTIMENT_SCALE
        return dict(totals)


def _counter_value(raw: bytes | None) -> int:
    """HBase atomic counters are stored as 8-byte big-endian integers."""
    if not raw:
        return 0
    return int.from_bytes(raw, byteorder="big", signed=True)


class HBaseStore:
    """HBase backend. Needs the HBase Thrift server (see docker-compose.yml)."""

    def __init__(self, host: str = "localhost", port: int = 9090) -> None:
        import happybase  # imported here so the rest works without it installed

        self.connection = happybase.Connection(host=host, port=port)

    def create_tables(self) -> None:
        existing = set(self.connection.tables())
        for name, families in TABLES.items():
            if name.encode() not in existing:
                self.connection.create_table(name, {cf: {} for cf in families})

    def _table(self, name: str):
        return self.connection.table(name)

    def upsert_account(self, account: Account, score: float, coordination: float = 0.0) -> None:
        self._table("accounts").put(keys.account_key(account.user_id), {
            b"p:username": account.username.encode(),
            b"p:followers": str(account.followers).encode(),
            b"p:following": str(account.following).encode(),
            b"p:statuses": str(account.statuses).encode(),
            b"p:default_avatar": b"1" if account.default_avatar else b"0",
            b"s:bot_score": str(score).encode(),
            b"s:coordination": str(coordination).encode(),
        })

    def upsert_tweet(self, tweet_id: str, text: str, author_id: str,
                     ts: datetime, topics: list[str], sentiment: float = 0.0) -> None:
        self._table("tweets").put(keys.tweet_key(tweet_id), {
            b"t:text": text.encode(),
            b"t:author_id": author_id.encode(),
            b"t:created_at": ts.isoformat().encode(),
            b"n:topics": ",".join(topics).encode(),
            b"n:sentiment": str(sentiment).encode(),
        })

    def add_engagement(self, topic: str, ts: datetime, event_id: str,
                       event_type: str, actor_id: str, score: float) -> bool:
        table = self._table("engagements")
        key = keys.engagement_key(topic, ts, event_id)
        if table.row(key, columns=[b"e:type"]):
            return False  # already stored: a replayed message
        table.put(key, {
            b"e:type": event_type.encode(),
            b"e:actor_id": actor_id.encode(),
            b"e:actor_bot_score": str(score).encode(),
        })
        return True

    def count_engagement(self, topic: str, ts: datetime, suspicious: bool,
                         sentiment: float = 0.0) -> None:
        table = self._table("topic_stats")
        row = keys.topic_stats_key(topic, ts)
        table.counter_inc(row, b"s:total")
        if suspicious:
            table.counter_inc(row, b"s:suspicious")
        milli = round(sentiment * _SENTIMENT_SCALE)
        if milli:
            table.counter_inc(row, b"s:sentiment_milli", milli)

    def topic_totals(self) -> dict[str, dict]:
        totals: dict[str, dict] = defaultdict(_empty_totals)
        for key, data in self._table("topic_stats").scan():
            topic = key.decode().split("|")[0]
            totals[topic]["total"] += _counter_value(data.get(b"s:total"))
            totals[topic]["suspicious"] += _counter_value(data.get(b"s:suspicious"))
            totals[topic]["sentiment_sum"] += (
                _counter_value(data.get(b"s:sentiment_milli")) / _SENTIMENT_SCALE)
        return dict(totals)
