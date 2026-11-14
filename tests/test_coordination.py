from cib_rank.coordination import CoEngagementTracker, coordination_score
from cib_rank.pipeline import DuplicateTracker, process_event
from cib_rank.store import MemoryStore


def test_pair_becomes_partners_after_min_shared_tweets():
    tracker = CoEngagementTracker(min_shared=3)
    for tweet in ("t1", "t2"):
        tracker.observe(tweet, "a")
        tracker.observe(tweet, "b")
    assert tracker.partners_of("a") == 0
    tracker.observe("t3", "a")
    tracker.observe("t3", "b")
    assert tracker.partners_of("a") == 1 and tracker.partners_of("b") == 1


def test_repeated_engagement_on_same_tweet_counts_once():
    tracker = CoEngagementTracker(min_shared=2)
    for _ in range(5):
        tracker.observe("t1", "a")
        tracker.observe("t1", "b")
    assert tracker.partners_of("a") == 0


def test_strangers_on_one_viral_tweet_are_not_partners():
    tracker = CoEngagementTracker(min_shared=3)
    for i in range(100):
        tracker.observe("viral", f"user{i}")
    assert all(tracker.partners_of(f"user{i}") == 0 for i in range(100))


def test_viral_tweets_beyond_cap_add_no_pairs():
    tracker = CoEngagementTracker(min_shared=1, max_actors_per_tweet=2)
    for actor in ("a", "b", "c"):
        tracker.observe("t", actor)
    assert tracker.partners_of("c") == 0
    assert tracker.partners_of("a") == 1


def test_coordination_score_saturates():
    assert coordination_score(0) == 0.0
    assert coordination_score(3, saturation=5) == 0.6
    assert coordination_score(50, saturation=5) == 1.0


def _event(i, actor, tweet, etype="retweet"):
    return {"event_id": f"{actor}-{i}", "timestamp": "2026-11-14T12:00:00Z", "event_type": etype,
            "tweet_id": tweet, "tweet_text": f"#crypto news {tweet}", "author_id": "x",
            "actor_id": actor, "actor_username": "plain_name", "actor_created_at": "2015-01-01T00:00:00Z",
            "actor_followers": "500", "actor_following": "300", "actor_statuses": "2000",
            "actor_default_avatar": "0", "reply_latency_seconds": ""}


def test_ring_with_normal_profiles_is_flagged_by_coordination():
    store, dups, co = MemoryStore(), DuplicateTracker(), CoEngagementTracker()
    ring = [f"ring{i}" for i in range(4)]
    last = {}
    for t in range(3):
        for actor in ring:
            last[actor] = process_event(_event(t, actor, f"tweet{t}"), store, dups, co)
    # Every ring member shares 3 tweets with 3 others -> 3 partners.
    assert all(co.partners_of(a) == 3 for a in ring)
    # Streaming: the last member to engage already sees the whole ring.
    assert last["ring3"]["coordination"] == 0.6
    assert last["ring3"]["suspicious"]
    assert last["ring3"]["bot_score"] < 0.6  # profile alone would miss them


def test_posts_do_not_create_co_engagement():
    store, dups, co = MemoryStore(), DuplicateTracker(), CoEngagementTracker()
    for t in range(3):
        for actor in ("a", "b"):
            process_event(_event(t, actor, f"tweet{t}", etype="post"), store, dups, co)
    assert co.partners_of("a") == 0
