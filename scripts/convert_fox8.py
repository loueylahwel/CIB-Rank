"""Convert the fox8-23 dataset into CIB-Rank's engagement CSV.

fox8-23 (Yang & Menczer, 2023, CC BY 4.0): 1,140 ChatGPT-powered bots from one
coordinated network ("fox8") and 1,140 human accounts, each with up to 200
tweets in Twitter API v1.1 format.
Download: https://doi.org/10.5281/zenodo.8035289  (fox8_23_dataset.ndjson.gz)

Each tweet becomes one engagement event:
- retweet -> event_type "retweet"; engaged tweet = the original;
             reply_latency_seconds = seconds between original and retweet
- reply   -> event_type "reply"; engaged tweet = the tweet replied to
             (latency unknown: the parent's timestamp is not in the data)
- other   -> event_type "post"; engaged tweet = the account's own tweet

Usage:
    python -m scripts.convert_fox8 data/raw/fox8/fox8_23_dataset.ndjson.gz \
        --out data/fox8_events.csv [--max-tweets-per-user 20]

Caveat: the human accounts come from older datasets (tweets mostly 2008-2020),
while the bots tweeted in 2022-2023. Topic rankings therefore partly reflect
the different time periods, not only bot behaviour.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from datetime import datetime

from cib_rank.ingest import EVENT_FIELDS

EXTRA_FIELDS = ["is_bot_truth", "source_dataset"]
_TWITTER_TIME = "%a %b %d %H:%M:%S %z %Y"


def _time(value: str) -> datetime:
    return datetime.strptime(value, _TWITTER_TIME)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt.utcoffset().total_seconds() == 0 \
        else dt.isoformat()


def _text_with_hashtags(tweet: dict) -> str:
    """Tweet text, plus any hashtags from `entities` that truncation cut off."""
    text = tweet.get("text") or ""
    source = tweet.get("retweeted_status") or tweet
    tags = [h["text"] for h in source.get("entities", {}).get("hashtags", [])]
    tags += [h["text"] for h in tweet.get("entities", {}).get("hashtags", [])]
    lowered = text.lower()
    missing = []
    for tag in tags:
        if f"#{tag.lower()}" not in lowered and tag not in missing:
            missing.append(tag)
    return text + "".join(f" #{tag}" for tag in missing)


def tweet_to_event(tweet: dict, label: str, source_dataset: str) -> dict:
    user = tweet["user"]
    created = _time(tweet["created_at"])
    original = tweet.get("retweeted_status")
    latency = ""
    if original:
        event_type = "retweet"
        engaged_id = original["id_str"]
        author_id = original.get("user", {}).get("id_str", "")
        latency = max(0, int((created - _time(original["created_at"])).total_seconds()))
    elif tweet.get("in_reply_to_status_id_str"):
        event_type = "reply"
        engaged_id = tweet["in_reply_to_status_id_str"]
        author_id = tweet.get("in_reply_to_user_id_str") or ""
    else:
        event_type = "post"
        engaged_id = tweet["id_str"]
        author_id = user["id_str"]

    return {
        "event_id": tweet["id_str"],
        "timestamp": _iso(created),
        "event_type": event_type,
        "tweet_id": engaged_id,
        "tweet_text": _text_with_hashtags(tweet),
        "author_id": author_id,
        "actor_id": user["id_str"],
        "actor_username": user.get("screen_name", ""),
        "actor_created_at": _iso(_time(user["created_at"])),
        "actor_followers": user.get("followers_count", 0),
        "actor_following": user.get("friends_count", 0),
        "actor_statuses": user.get("statuses_count", 0),
        "actor_default_avatar": int(bool(user.get("default_profile_image"))),
        "reply_latency_seconds": latency,
        "is_bot_truth": 1 if label == "bot" else 0,
        "source_dataset": source_dataset,
    }


def convert(src: str, out: str, max_per_user: int | None = None) -> dict:
    counts = {"accounts": 0, "events": 0, "bots": 0, "humans": 0}
    with gzip.open(src, "rt", encoding="utf-8") as handle, \
            open(out, "w", newline="", encoding="utf-8") as dest:
        writer = csv.DictWriter(dest, fieldnames=EVENT_FIELDS + EXTRA_FIELDS)
        writer.writeheader()
        for line in handle:
            record = json.loads(line)
            counts["accounts"] += 1
            counts["bots" if record["label"] == "bot" else "humans"] += 1
            tweets = record["user_tweets"]
            if max_per_user:
                tweets = tweets[:max_per_user]
            for tweet in tweets:
                writer.writerow(tweet_to_event(tweet, record["label"], record["dataset"]))
                counts["events"] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("src", help="path to fox8_23_dataset.ndjson.gz")
    parser.add_argument("--out", default="data/fox8_events.csv")
    parser.add_argument("--max-tweets-per-user", type=int, default=None,
                        help="keep only the first N tweets per account (smaller demo file)")
    args = parser.parse_args()
    counts = convert(args.src, args.out, args.max_tweets_per_user)
    print(f"Wrote {counts['events']} events from {counts['accounts']} accounts "
          f"({counts['bots']} bots, {counts['humans']} humans) to {args.out}")


if __name__ == "__main__":
    main()
