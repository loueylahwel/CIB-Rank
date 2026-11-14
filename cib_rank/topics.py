"""Topic / niche extraction from tweet text.

Two sources, merged and de-duplicated:
1. Hashtags (lower-cased, without the '#'): the cheapest reliable niche signal.
2. Keywords mapped to a niche (`NICHE_KEYWORDS`), so a tweet that says
   "bitcoin" without a hashtag still counts towards #crypto.

Extend `NICHE_KEYWORDS` for the niches you study. Keep keywords specific:
a generic word ("shots", "run") would pull unrelated tweets into a niche.
"""

from __future__ import annotations

import re

_HASHTAG = re.compile(r"#(\w{2,50})", re.UNICODE)
_WORD = re.compile(r"(?<!#)\b[a-z0-9]{2,30}\b")

NICHE_KEYWORDS: dict[str, str] = {
    # crypto
    "bitcoin": "crypto", "btc": "crypto", "ethereum": "crypto", "eth": "crypto",
    "altcoin": "crypto", "altcoins": "crypto", "airdrop": "crypto",
    # nft
    "nfts": "nft", "opensea": "nft",
    # cooking
    "recipe": "cooking", "recipes": "cooking",
    # gardening
    "tomatoes": "gardening", "seedlings": "gardening",
    # fitness
    "workout": "fitness", "gym": "fitness",
    # photography
    "lens": "photography", "aperture": "photography",
}


def extract_topics(text: str, use_keywords: bool = True) -> list[str]:
    """Unique topics in `text`, hashtags first, then keyword niches, lower-cased."""
    topics: list[str] = []
    text = text or ""
    for match in _HASHTAG.finditer(text):
        tag = match.group(1).lower()
        if tag not in topics:
            topics.append(tag)
    if use_keywords:
        for word in _WORD.findall(text.lower()):
            niche = NICHE_KEYWORDS.get(word)
            if niche and niche not in topics:
                topics.append(niche)
    return topics
