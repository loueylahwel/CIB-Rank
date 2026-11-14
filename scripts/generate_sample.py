"""Generate a SYNTHETIC engagement dataset for testing the pipeline.

The accounts, tweets and timings are invented. Bot-like accounts are generated
with the patterns the scoring heuristics look for, so the output is only useful
to check that the pipeline works. Do not present its results as real findings.

Usage:
    python scripts/generate_sample.py --out data/sample_engagements.csv
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone

from cib_rank.ingest import EVENT_FIELDS

HUMAN_TOPICS = ["cooking", "gardening", "fitness", "photography"]
BOT_TOPICS = ["crypto", "nft"]
HUMAN_TEMPLATES = [
    "Trying a new recipe tonight #cooking",
    "Finally planted the tomatoes #gardening",
    "Morning run done, feeling good #fitness",
    "Golden hour shots from today #photography",
    "Any tips for a beginner with #{topic}?",
]
BOT_TEMPLATES = [
    "Huge #{topic} gains this week, join now",
    "Don't miss the next #{topic} drop!!",
    "Best #{topic} signals, follow me #{topic}",
]
NOW = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(seed: int = 7, humans: int = 60, bots: int = 40, events_per_actor: int = 25) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    tweets = {topic: [f"orig_{topic}_{i}" for i in range(15)]
              for topic in HUMAN_TOPICS + BOT_TOPICS}
    event_no = 0

    actors = []
    for i in range(humans):
        age_days = rng.randint(730, 3650)
        actors.append({
            "id": f"h{i:04d}", "is_bot": 0, "name": f"user_{rng.choice(['sun', 'blue', 'river', 'oak'])}{i}x",
            "created": NOW - timedelta(days=age_days),
            "followers": rng.randint(100, 5000), "following": rng.randint(100, 1000),
            "statuses": rng.randint(500, 20000), "default_avatar": int(rng.random() < 0.1),
        })
    for i in range(bots):
        age_days = rng.randint(3, 40)
        actors.append({
            "id": f"b{i:04d}", "is_bot": 1, "name": f"user{rng.randint(10**6, 10**9 - 1)}",
            "created": NOW - timedelta(days=age_days),
            "followers": rng.randint(0, 30), "following": rng.randint(500, 2000),
            "statuses": rng.randint(2000, 8000), "default_avatar": int(rng.random() < 0.8),
        })

    for actor in actors:
        for _ in range(events_per_actor if actor["is_bot"] else rng.randint(1, 4)):
            event_no += 1
            if actor["is_bot"]:
                topic = rng.choice(BOT_TOPICS)
                text = rng.choice(BOT_TEMPLATES).format(topic=topic)
                latency = rng.randint(1, 10)
                event_type = "reply" if rng.random() < 0.6 else "like"
                ts = NOW - timedelta(minutes=rng.randint(0, 60 * 24))
            else:
                topic = rng.choice(HUMAN_TOPICS)
                text = rng.choice(HUMAN_TEMPLATES).format(topic=topic)
                latency = rng.randint(60, 3600)
                event_type = rng.choice(["like", "reply", "retweet"])
                ts = NOW - timedelta(minutes=rng.randint(0, 60 * 24))
            rows.append({
                "event_id": f"e{event_no:07d}",
                "timestamp": _iso(ts),
                "event_type": event_type,
                "tweet_id": rng.choice(tweets[topic]),
                "tweet_text": text,
                "author_id": f"author_{topic}",
                "actor_id": actor["id"],
                "actor_username": actor["name"],
                "actor_created_at": _iso(actor["created"]),
                "actor_followers": actor["followers"],
                "actor_following": actor["following"],
                "actor_statuses": actor["statuses"],
                "actor_default_avatar": actor["default_avatar"],
                "reply_latency_seconds": latency if event_type == "reply" else "",
                "is_bot_truth": actor["is_bot"],
            })
    rng.shuffle(rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="data/sample_engagements.csv")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    rows = generate(seed=args.seed)
    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_FIELDS + ["is_bot_truth"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} synthetic events to {args.out}")


if __name__ == "__main__":
    main()
