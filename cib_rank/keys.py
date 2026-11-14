"""HBase row-key builders. Keep all key formats in one place."""

from __future__ import annotations

from datetime import datetime, timezone

# Reversed epoch seconds: newest events sort first within a topic.
_MAX_EPOCH = 9_999_999_999


def hour_bucket(ts: datetime) -> str:
    """UTC hour bucket, e.g. '2026111412'."""
    return ts.astimezone(timezone.utc).strftime("%Y%m%d%H")


def account_key(user_id: str) -> bytes:
    return user_id.encode()


def tweet_key(tweet_id: str) -> bytes:
    return tweet_id.encode()


def engagement_key(topic: str, ts: datetime, event_id: str) -> bytes:
    """`topic|reversed_ts|event_id`: one topic's recent events are one contiguous scan."""
    epoch = int(ts.astimezone(timezone.utc).timestamp())
    reversed_ts = f"{_MAX_EPOCH - epoch:010d}"
    return f"{topic}|{reversed_ts}|{event_id}".encode()


def topic_stats_key(topic: str, ts: datetime) -> bytes:
    """`topic|yyyymmddhh`: one row per topic per hour."""
    return f"{topic}|{hour_bucket(ts)}".encode()
