import csv

from cib_rank.evaluate import evaluate
from cib_rank.ingest import EVENT_FIELDS
from scripts.generate_sample import generate


def test_synthetic_dataset_is_separable(tmp_path):
    rows = generate(seed=7)
    path = tmp_path / "sample.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_FIELDS + ["is_bot_truth"])
        writer.writeheader()
        writer.writerows(rows)

    result = evaluate(str(path))
    assert result["accounts"] == 100
    assert result["precision"] >= 0.9
    assert result["recall"] >= 0.9
