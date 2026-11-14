"""Small lexicon-based sentiment score, in [-1, 1].

Deliberately dependency-free so the pipeline runs anywhere. It only counts
positive and negative words, so it misses sarcasm, negation ("not good") and
slang. A stronger drop-in replacement is VADER (`vaderSentiment` package),
which is tuned for social media text.
"""

from __future__ import annotations

import re

POSITIVE = {
    "good", "great", "love", "loved", "nice", "amazing", "awesome", "best", "happy",
    "excellent", "beautiful", "fun", "enjoy", "enjoyed", "win", "winning", "gains",
    "glad", "thanks", "thank", "wonderful", "fantastic", "perfect", "cool", "proud",
    "delicious", "tasty", "recommend", "helpful",
}
NEGATIVE = {
    "bad", "worst", "hate", "hated", "awful", "terrible", "sad", "angry", "scam",
    "fake", "fraud", "loss", "losses", "lost", "broken", "fail", "failed", "poor",
    "ugly", "boring", "disappointed", "disappointing", "horrible", "useless", "crash",
    "dump", "rug", "stupid", "wrong", "problem",
}

_WORD = re.compile(r"[a-z']+")


def sentiment_score(text: str) -> float:
    """(positive - negative) / (positive + negative) words; 0.0 when none match."""
    words = _WORD.findall((text or "").lower())
    pos = sum(w in POSITIVE for w in words)
    neg = sum(w in NEGATIVE for w in words)
    if pos + neg == 0:
        return 0.0
    return round((pos - neg) / (pos + neg), 3)
