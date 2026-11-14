"""Measure how well the bot score matches labelled data.

The CSV must contain an `is_bot_truth` column (1 = bot, 0 = human), as in
data/sample_engagements.csv. Labels are used only here, never by the pipeline.

Usage:
    python -m cib_rank.evaluate data/sample_engagements.csv
"""

from __future__ import annotations

import argparse

from .ingest import iter_csv
from .pipeline import DuplicateTracker, process_event
from .scoring import SUSPICIOUS_THRESHOLD
from .store import MemoryStore


def evaluate(path: str, threshold: float = SUSPICIOUS_THRESHOLD) -> dict:
    store = MemoryStore()
    duplicates = DuplicateTracker()
    labels: dict[str, int] = {}
    scores: dict[str, float] = {}
    for event in iter_csv(path):
        result = process_event(event, store, duplicates)
        labels[result["actor_id"]] = int(event["is_bot_truth"])
        scores[result["actor_id"]] = result["bot_score"]  # last score seen for the actor

    tp = fp = fn = tn = 0
    for actor, label in labels.items():
        flagged = scores[actor] >= threshold
        if flagged and label:
            tp += 1
        elif flagged and not label:
            fp += 1
        elif not flagged and label:
            fn += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"accounts": len(labels), "threshold": threshold, "tp": tp, "fp": fp,
            "fn": fn, "tn": tn, "precision": round(precision, 3), "recall": round(recall, 3)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("csv_path")
    parser.add_argument("--threshold", type=float, default=SUSPICIOUS_THRESHOLD)
    args = parser.parse_args()
    for key, value in evaluate(args.csv_path, args.threshold).items():
        print(f"{key:>10}: {value}")


if __name__ == "__main__":
    main()
