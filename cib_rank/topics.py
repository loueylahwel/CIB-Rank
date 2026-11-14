"""Topic / niche extraction from tweet text.

For now a topic is a hashtag (lower-cased, without the '#'). Hashtags are the
cheapest reliable niche signal on Twitter; keyword clustering can be added later.
"""

from __future__ import annotations

import re

_HASHTAG = re.compile(r"#(\w{2,50})", re.UNICODE)


def extract_topics(text: str) -> list[str]:
    """Return the unique hashtags in `text`, in order of appearance, lower-cased."""
    seen: list[str] = []
    for match in _HASHTAG.finditer(text or ""):
        tag = match.group(1).lower()
        if tag not in seen:
            seen.append(tag)
    return seen
