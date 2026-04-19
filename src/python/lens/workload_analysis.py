import csv
from collections import Counter
from pathlib import Path


SAMPLE_WORKLOAD = [
    ("users", "SELECT", 12.5),
    ("users", "SELECT", 13.1),
    ("orders", "JOIN", 32.4),
    ("orders", "JOIN", 29.8),
    ("orders", "AGGREGATE", 41.2),
    ("events", "SELECT", 55.0),
]


def main() -> None:
    results_dir = Path("results/lens")
    results_dir.mkdir(parents=True, exist_ok=True)
    counts = Counter(table for table, _, _ in SAMPLE_WORKLOAD)

    with (results_dir / "workload_analysis.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["table", "hotness", "slowest_pattern", "avg_latency_ms", "recommended_index"])
        for table, hotness in counts.items():
            matching = [row for row in SAMPLE_WORKLOAD if row[0] == table]
            slowest = max(matching, key=lambda row: row[2])
            avg_latency = sum(row[2] for row in matching) / len(matching)
            writer.writerow([table, hotness, slowest[1], round(avg_latency, 2), f"idx_{table}_hot"])


if __name__ == "__main__":
    main()
