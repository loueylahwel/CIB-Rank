from datetime import datetime, timedelta, timezone

from cib_rank.scoring import SUSPICIOUS_THRESHOLD, WEIGHTS, Account, bot_score, signal_scores

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


def human() -> Account:
    return Account("h1", "maria_cooks", NOW - timedelta(days=2000), 800, 300, 5000, False)


def bot() -> Account:
    return Account("b1", "user84729301", NOW - timedelta(days=5), 3, 1500, 6000, True)


def test_weights_sum_to_one():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


def test_human_scores_low():
    assert bot_score(human(), NOW, duplicate_count=0, reply_latency_seconds=600) < 0.2


def test_bot_scores_high():
    score = bot_score(bot(), NOW, duplicate_count=5, reply_latency_seconds=2)
    assert score >= SUSPICIOUS_THRESHOLD


def test_score_is_within_unit_interval():
    for account in (human(), bot()):
        for dups in (0, 3, 50):
            score = bot_score(account, NOW, duplicate_count=dups)
            assert 0.0 <= score <= 1.0


def test_new_account_signal_is_zero_for_old_accounts():
    assert signal_scores(human(), NOW)["new_account"] == 0.0


def test_fast_reply_signal():
    assert signal_scores(human(), NOW, reply_latency_seconds=2)["fast_reply"] == 1.0
    assert signal_scores(human(), NOW, reply_latency_seconds=20)["fast_reply"] == 0.5
    assert signal_scores(human(), NOW, reply_latency_seconds=None)["fast_reply"] == 0.0


def test_empty_username_does_not_crash():
    account = Account("x", "", NOW - timedelta(days=100), 10, 10, 10, False)
    assert signal_scores(account, NOW)["username_digits"] == 0.0
