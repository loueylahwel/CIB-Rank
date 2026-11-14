"""Coordination (co-engagement) signal.

Coordinated inauthentic behaviour shows in what accounts do *together*, not in
any single profile. Two accounts become **coordination partners** once they
have engaged with (retweeted / replied to / liked) the same `min_shared`
tweets. An account's coordination score grows with its number of partners.

Why pairs and not just "busy tweets": thousands of unrelated people retweet a
viral post, but the same two strangers rarely meet again and again. Repeated
co-engagement by the same pairs is the footprint of a network.

Streaming note: the score uses the evidence seen so far, so an account's first
engagements score low and the score rises as the network shows itself. The
tracker lives in memory, per consumer process.
"""

from __future__ import annotations

from collections import defaultdict

# Engagement types that point at someone else's tweet (posts are excluded).
CO_ENGAGEMENT_TYPES = {"retweet", "reply", "like", "quote"}

MIN_SHARED_TWEETS = 3      # shared tweets needed before two accounts count as partners
PARTNER_SATURATION = 5     # partners at which the score reaches 1.0
MAX_ACTORS_PER_TWEET = 200 # beyond this a tweet is "viral": no new pairs from it
COORDINATION_THRESHOLD = 0.6  # score >= this (3+ partners) marks engagements suspicious


class CoEngagementTracker:
    def __init__(self, min_shared: int = MIN_SHARED_TWEETS,
                 max_actors_per_tweet: int = MAX_ACTORS_PER_TWEET) -> None:
        self.min_shared = min_shared
        self.max_actors_per_tweet = max_actors_per_tweet
        self.actors_by_tweet: dict[str, set[str]] = defaultdict(set)
        self.pair_counts: dict[tuple[str, str], int] = defaultdict(int)
        self.partners: dict[str, int] = defaultdict(int)

    def observe(self, tweet_id: str, actor_id: str) -> None:
        """Record that `actor_id` engaged with `tweet_id`."""
        actors = self.actors_by_tweet[tweet_id]
        if actor_id in actors:
            return
        # Viral tweets are skipped: co-engaging them says little, and pairing
        # every engager would cost O(n^2).
        if len(actors) < self.max_actors_per_tweet:
            for other in actors:
                key = (actor_id, other) if actor_id < other else (other, actor_id)
                self.pair_counts[key] += 1
                if self.pair_counts[key] == self.min_shared:
                    self.partners[actor_id] += 1
                    self.partners[other] += 1
        actors.add(actor_id)

    def partners_of(self, actor_id: str) -> int:
        return self.partners.get(actor_id, 0)


def coordination_score(partners: int, saturation: int = PARTNER_SATURATION) -> float:
    """Map a partner count to [0, 1]."""
    return round(min(1.0, partners / saturation), 3)
