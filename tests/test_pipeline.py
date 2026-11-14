from datetime import datetime, timezone

from cib_rank import keys
from cib_rank.pipeline import DuplicateTracker, process_event
from cib_rank.ranking import rank_topics
from cib_rank.store import MemoryStore
from cib_rank.topics import extract_topics

TS = "2026-11-14T12:00:00Z"


def event(**overrides) -> dict:
    base = {
        "event_id": "e1", "timestamp": TS, "event_type": "like", "tweet_id": "t1",
        "tweet_text": "Huge #crypto gains", "author_id": "a1",
        "actor_id": "u1", "actor_username": "sunny_day", "actor_created_at": "2019-01-01T00:00:00Z",
        "actor_followers": "400", "actor_following": "200", "actor_statuses": "3000",
        "actor_default_avatar": "0", "reply_latency_seconds": "",
    }
    base.update(overrides)
    return base


def test_extract_topics_dedupes_and_lowercases():
    assert extract_topics("#Crypto and #nft and #crypto") == ["crypto", "nft"]
    assert extract_topics("") == []


def test_row_keys_are_stable_and_sortable():
    ts = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
    assert keys.topic_stats_key("crypto", ts) == b"crypto|2026111412"
    newer = keys.engagement_key("crypto", datetime(2026, 11, 14, 13, tzinfo=timezone.utc), "e")
    older = keys.engagement_key("crypto", ts, "e")
    assert newer < older  # newest first within a topic


def test_process_event_stores_records_and_counts():
    store = MemoryStore()
    result = process_event(event(), store, DuplicateTracker())
    assert result["topics"] == ["crypto"]
    assert "u1" in store.accounts
    assert store.tweets["t1"]["topics"] == ["crypto"]
    assert store.topic_totals()["crypto"]["total"] == 1


def test_duplicate_text_across_accounts_raises_score():
    store = MemoryStore()
    dups = DuplicateTracker()
    first = process_event(event(actor_id="u1"), store, dups)
    last = None
    for i in range(2, 8):
        last = process_event(event(actor_id=f"u{i}", event_id=f"e{i}"), store, dups)
    assert last["bot_score"] >= first["bot_score"]


def test_replayed_event_is_counted_once():
    store = MemoryStore()
    dups = DuplicateTracker()
    first = process_event(event(), store, dups)
    replay = process_event(event(), store, dups)
    assert first["counted"] == 1
    assert replay["counted"] == 0
    assert store.topic_totals()["crypto"]["total"] == 1


def test_keyword_topics_without_hashtag():
    assert extract_topics("Just bought more bitcoin") == ["crypto"]
    assert extract_topics("#crypto bitcoin", use_keywords=True) == ["crypto"]
    assert extract_topics("Just bought more bitcoin", use_keywords=False) == []


def test_hashtag_text_is_not_matched_as_keyword():
    # "#recipe" is a hashtag topic, and must not also add the "cooking" keyword niche.
    assert extract_topics("#recipe tonight") == ["recipe"]


def test_sentiment_score_range_and_sign():
    from cib_rank.sentiment import sentiment_score
    assert sentiment_score("I love this, great work") > 0
    assert sentiment_score("this is a scam, terrible") < 0
    assert sentiment_score("the sky is blue") == 0.0
    assert -1.0 <= sentiment_score("love hate love") <= 1.0


def test_sentiment_is_aggregated_per_topic():
    store = MemoryStore()
    dups = DuplicateTracker()
    process_event(event(tweet_text="#crypto great gains", event_id="e1"), store, dups)
    process_event(event(tweet_text="#crypto scam", event_id="e2", actor_id="u2"), store, dups)
    ranked = rank_topics(store.topic_totals(), min_engagements=1)
    assert ranked[0]["topic"] == "crypto"
    assert ranked[0]["avg_sentiment"] == 0.0  # (+1 + -1) / 2


def test_rank_topics_orders_by_suspicious_ratio_and_filters_small_topics():
    totals = {
        "crypto": {"total": 100, "suspicious": 80},
        "cooking": {"total": 200, "suspicious": 10},
        "tiny": {"total": 3, "suspicious": 3},
    }
    ranked = rank_topics(totals, min_engagements=20)
    assert [r["topic"] for r in ranked] == ["crypto", "cooking"]
    assert ranked[0]["suspicious_ratio"] == 0.8
