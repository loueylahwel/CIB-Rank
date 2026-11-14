"""Measure how well the detectors match labelled data.

The CSV must contain an `is_bot_truth` column (1 = bot, 0 = human), as in
data/sample_engagements.csv. Labels are used only here, never by the pipeline.

Three detectors are reported per account:
- profile:      bot score (last value seen) >= threshold
- coordination: coordination score (after all events) >= COORDINATION_THRESHOLD
- combined:     either of the two (what the pipeline uses)

Usage:
    python -m cib_rank.evaluate data/sample_engagements.csv
"""

from __future__ import annotations

import argparse

from .coordination import COORDINATION_THRESHOLD, CoEngagementTracker, coordination_score
from .ingest import iter_csv
from .pipeline import DuplicateTracker, process_event
from .scoring import SUSPICIOUS_THRESHOLD
from .store import MemoryStore


def _metrics(flags: dict[str, bool], labels: dict[str, int]) -> dict:
    tp = sum(1 for a, f in flags.items() if f and labels[a])
    fp = sum(1 for a, f in flags.items() if f and not labels[a])
    fn = sum(1 for a, f in flags.items() if not f and labels[a])
    tn = len(flags) - tp - fp - fn
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": round(precision, 3), "recall": round(recall, 3)}


def evaluate(path: str, threshold: float = SUSPICIOUS_THRESHOLD) -> dict:
    store = MemoryStore()
    duplicates = DuplicateTracker()
    coengagement = CoEngagementTracker()
    labels: dict[str, int] = {}
    profile: dict[str, float] = {}
    for event in iter_csv(path):
        result = process_event(event, store, duplicates, coengagement)
        labels[result["actor_id"]] = int(event["is_bot_truth"])
        profile[result["actor_id"]] = result["bot_score"]  # last score seen for the actor

    coordination = {a: coordination_score(coengagement.partners_of(a)) for a in labels}
    profile_flags = {a: profile[a] >= threshold for a in labels}
    coord_flags = {a: coordination[a] >= COORDINATION_THRESHOLD for a in labels}
    combined_flags = {a: profile_flags[a] or coord_flags[a] for a in labels}
    return {
        "accounts": len(labels),
        "bots": sum(labels.values()),
        "threshold": threshold,
        "coordination_threshold": COORDINATION_THRESHOLD,
        "profile": _metrics(profile_flags, labels),
        "coordination": _metrics(coord_flags, labels),
        "combined": _metrics(combined_flags, labels),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("csv_path")
    parser.add_argument("--threshold", type=float, default=SUSPICIOUS_THRESHOLD)
    args = parser.parse_args()
    result = evaluate(args.csv_path, args.threshold)
    print(f"accounts: {result['accounts']} ({result['bots']} bots)  "
          f"profile threshold: {result['threshold']}  "
          f"coordination threshold: {result['coordination_threshold']}")
    print(f"{'detector':<14}{'precision':>10}{'recall':>8}{'tp':>7}{'fp':>7}{'fn':>7}{'tn':>7}")
    for name in ("profile", "coordination", "combined"):
        m = result[name]
        print(f"{name:<14}{m['precision']:>10.3f}{m['recall']:>8.3f}"
              f"{m['tp']:>7}{m['fp']:>7}{m['fn']:>7}{m['tn']:>7}")


if __name__ == "__main__":
    main()
