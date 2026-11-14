"""Heuristic bot / fake-account scoring.

Every signal is mapped to [0, 1] (1 = very suspicious) and combined as a
weighted average. The result is a heuristic score, not proof that an account
is fake: measure it with `cib_rank.evaluate` before drawing conclusions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

WEIGHTS: dict[str, float] = {
    "new_account": 0.25,
    "follow_ratio": 0.20,
    "posting_rate": 0.20,
    "default_avatar": 0.10,
    "username_digits": 0.10,
    "duplicate_text": 0.10,
    "fast_reply": 0.05,
}

# An engagement is counted as "suspicious" when its actor score reaches this.
SUSPICIOUS_THRESHOLD = 0.6


@dataclass(frozen=True)
class Account:
    user_id: str
    username: str
    created_at: datetime
    followers: int
    following: int
    statuses: int
    default_avatar: bool


def _age_days(account: Account, now: datetime) -> float:
    return max((now - account.created_at).total_seconds() / 86400.0, 0.0)


def signal_scores(
    account: Account,
    now: datetime,
    duplicate_count: int = 0,
    reply_latency_seconds: float | None = None,
) -> dict[str, float]:
    """Return each signal's suspiciousness in [0, 1]."""
    age = _age_days(account, now)

    # Brand-new accounts are the classic sock-puppet pattern.
    new_account = max(0.0, 1.0 - age / 30.0)

    # Accounts that follow many people but have almost no followers.
    ratio = account.followers / max(account.following, 1)
    if ratio < 0.1:
        follow_ratio = 1.0
    elif ratio < 0.5:
        follow_ratio = 0.5
    else:
        follow_ratio = 0.0

    # Average posts per day over the account's life (at least one day).
    per_day = account.statuses / max(age, 1.0)
    if per_day > 50:
        posting_rate = 1.0
    elif per_day > 10:
        posting_rate = 0.5
    else:
        posting_rate = 0.0

    default_avatar = 1.0 if account.default_avatar else 0.0

    # Share of digits in the username, e.g. "user84729301".
    name = account.username or ""
    digit_ratio = sum(c.isdigit() for c in name) / len(name) if name else 0.0
    if digit_ratio >= 0.4:
        username_digits = 1.0
    elif digit_ratio >= 0.2:
        username_digits = 0.5
    else:
        username_digits = 0.0

    # The same text posted by many different accounts is coordination.
    duplicate_text = min(1.0, duplicate_count / 5.0)

    # Replies within seconds of the original tweet are rarely human.
    if reply_latency_seconds is None:
        fast_reply = 0.0
    elif reply_latency_seconds < 5:
        fast_reply = 1.0
    elif reply_latency_seconds < 30:
        fast_reply = 0.5
    else:
        fast_reply = 0.0

    return {
        "new_account": new_account,
        "follow_ratio": follow_ratio,
        "posting_rate": posting_rate,
        "default_avatar": default_avatar,
        "username_digits": username_digits,
        "duplicate_text": duplicate_text,
        "fast_reply": fast_reply,
    }


def bot_score(
    account: Account,
    now: datetime,
    duplicate_count: int = 0,
    reply_latency_seconds: float | None = None,
) -> float:
    """Weighted average of the signals, rounded to 3 decimals, in [0, 1]."""
    signals = signal_scores(account, now, duplicate_count, reply_latency_seconds)
    total = sum(WEIGHTS[name] * value for name, value in signals.items())
    return round(total / sum(WEIGHTS.values()), 3)
