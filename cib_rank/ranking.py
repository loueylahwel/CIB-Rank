"""Ranking of topics by suspicious-engagement ratio."""

from __future__ import annotations


def rank_topics(totals: dict[str, dict], min_engagements: int = 20) -> list[dict]:
    """Sort topics by suspicious ratio (highest first).

    Topics with fewer than `min_engagements` events are left out, so one bot
    on a tiny topic cannot top the list.
    """
    rows = []
    for topic, counts in totals.items():
        total = counts["total"]
        if total < min_engagements or total == 0:
            continue
        suspicious = counts["suspicious"]
        rows.append({
            "topic": topic,
            "total": total,
            "suspicious": suspicious,
            "suspicious_ratio": round(suspicious / total, 3),
            "avg_sentiment": round(counts.get("sentiment_sum", 0.0) / total, 3),
        })
    rows.sort(key=lambda r: (r["suspicious_ratio"], r["total"]), reverse=True)
    return rows
