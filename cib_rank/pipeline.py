"""Turns one raw engagement event into stored records.

An event is one engagement (like / reply / retweet) by an actor on a tweet.
Expected fields are listed in `EVENT_FIELDS` and match the CSV from the
ingestion step.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from .coordination import (CO_ENGAGEMENT_TYPES, COORDINATION_THRESHOLD, CoEngagementTracker,
                           coordination_score)
from .scoring import SUSPICIOUS_THRESHOLD, Account, bot_score
from .sentiment import sentiment_score
from .topics import extract_topics


@dataclass
class DuplicateTracker:
    """Counts how many distinct accounts posted the same text.

    Kept in memory per consumer process. A restart resets the counts, so the
    duplicate signal is weaker after a restart (documented in the README).
    """

    actors_by_text: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))

    def observe(self, text: str, actor_id: str) -> int:
        """Record that `actor_id` used `text`; return how many accounts used it so far."""
        digest = hashlib.sha256(" ".join(text.lower().split()).encode()).hexdigest()
        self.actors_by_text[digest].add(actor_id)
        return len(self.actors_by_text[digest]) - 1  # other accounts, not counting this one


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def process_event(event: dict, store, duplicates: DuplicateTracker,
                  coengagement: CoEngagementTracker | None = None) -> dict:
    """Score the actor, store the records, update topic counters.

    An engagement is suspicious when its account has a high profile bot score
    OR a high coordination score (many co-engagement partners). Without a
    `coengagement` tracker only the profile score is used.

    Returns a summary dict (used by the consumer for metrics and by tests).
    """
    ts = parse_time(event["timestamp"])
    account = Account(
        user_id=str(event["actor_id"]),
        username=event.get("actor_username", ""),
        created_at=parse_time(event["actor_created_at"]),
        followers=int(event["actor_followers"]),
        following=int(event["actor_following"]),
        statuses=int(event["actor_statuses"]),
        default_avatar=str(event["actor_default_avatar"]).lower() in ("1", "true", "yes"),
    )
    latency = event.get("reply_latency_seconds")
    latency = float(latency) if latency not in (None, "") else None

    other_accounts = duplicates.observe(event.get("tweet_text", ""), account.user_id)
    score = bot_score(account, ts, duplicate_count=other_accounts, reply_latency_seconds=latency)
    coordination = 0.0
    if coengagement is not None:
        if event.get("event_type") in CO_ENGAGEMENT_TYPES:
            coengagement.observe(str(event["tweet_id"]), account.user_id)
        coordination = coordination_score(coengagement.partners_of(account.user_id))
    suspicious = score >= SUSPICIOUS_THRESHOLD or coordination >= COORDINATION_THRESHOLD

    text = event.get("tweet_text", "")
    topics = extract_topics(text)
    sentiment = sentiment_score(text)
    store.upsert_account(account, score, coordination)
    store.upsert_tweet(str(event["tweet_id"]), text,
                       str(event.get("author_id", "")), ts, topics, sentiment)
    counted = 0
    for topic in topics:
        # Only new engagement rows move the counters, so replays are not double-counted.
        if store.add_engagement(topic, ts, str(event["event_id"]), event["event_type"],
                                account.user_id, score):
            store.count_engagement(topic, ts, suspicious, sentiment)
            counted += 1

    return {"actor_id": account.user_id, "bot_score": score, "coordination": coordination,
            "suspicious": suspicious,
            "topics": topics, "sentiment": sentiment, "counted": counted}
