#!/usr/bin/env python3
"""
Convert ClickBench result JSON files into the same CSV schema as benchmark.py.

Usage:
  uv run python clickbench/convert_clickbench.py
"""

import csv
import json
from pathlib import Path

CLICKBENCH_REPO = Path("/Users/tyler/code/sandbox/clickbench")
OUTPUT = Path(__file__).parent / "results" / "clickbench_results.csv"

# Map: (directory, result_filename) → display name for benchmark_id
SYSTEMS = [
    ("pg_clickhouse", "c8g.metal-48xl.json"),
    ("pg_clickhouse", "c7a.metal-48xl.json"),
    ("pg_clickhouse", "c6a.metal.json"),
    ("pg_clickhouse", "c8g.4xlarge.json"),
    ("pg_clickhouse", "c6a.4xlarge.json"),
    ("pg_clickhouse", "c6a.2xlarge.json"),
    ("pg_clickhouse", "c6a.xlarge.json"),
    ("pg_duckdb-motherduck", "motherduck.json"),
    ("hydra", "hydra.json"),
    ("crunchy-bridge-for-analytics", "crunchy-bridge-analytics-256.json"),
    ("pg_ducklake", "c6a.4xlarge.json"),
    ("pg_ducklake", "c6a.xlarge.json"),
    ("pg_duckdb-parquet", "c8g.4xlarge.json"),
    ("pg_duckdb-parquet", "c6a.4xlarge.json"),
    ("pg_duckdb-parquet", "c6a.2xlarge.json"),
    ("pg_duckdb-parquet", "c6a.xlarge.json"),
    ("pg_duckdb-indexed", "c6a.4xlarge.json"),
    ("pg_duckdb", "c8g.4xlarge.json"),
    ("pg_duckdb", "c6a.4xlarge.json"),
    ("pg_duckdb", "c6a.2xlarge.json"),
    ("pg_duckdb", "c6a.xlarge.json"),
    ("timescaledb", "c8g.4xlarge.json"),
    ("timescaledb", "c6a.4xlarge.json"),
    ("timescale-cloud", "16cpu.json"),
    ("timescale-cloud", "8cpu.json"),
    ("timescale-cloud", "4cpu.json"),
    ("timescaledb-no-columnstore", "c8g.4xlarge.json"),
    ("timescaledb-no-columnstore", "c6a.4xlarge.json"),
    ("citus", "c6a.4xlarge.json"),
    ("citus", "c6a.2xlarge.json"),
    ("citus", "c6a.xlarge.json"),
    ("citus", "c6a.large.json"),
    ("supabase", "supabase.json"),
    ("postgresql", "c6a.4xlarge.json"),
    ("postgresql-orioledb", "c6a.4xlarge.json"),
]

# Machine specs (vCPUs, memory_gb) for known instance types
MACHINE_SPECS = {
    "c8g.metal-48xl": (192, 384),
    "c7a.metal-48xl": (192, 384),
    "c6a.metal":      (192, 384),
    "c8g.4xlarge":    (16, 32),
    "c6a.4xlarge":    (16, 32),
    "c6a.2xlarge":    (8, 16),
    "c6a.xlarge":     (4, 8),
    "c6a.large":      (2, 4),
}

FIELDNAMES = [
    "benchmark_id", "timestamp", "instance_size", "vcpus", "memory_gb",
    "table_name", "query_number", "run_number", "time_seconds", "query_text",
]


def convert():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for dirname, filename in SYSTEMS:
        path = CLICKBENCH_REPO / dirname / "results" / filename
        if not path.exists():
            print(f"  skip (not found): {path}")
            continue

        data = json.loads(path.read_text())
        system = data.get("system", dirname)
        machine = data.get("machine", filename.replace(".json", ""))
        date = data.get("date", "")
        result = data.get("result", [])

        benchmark_id = f"{system} ({machine})"
        specs = MACHINE_SPECS.get(machine, (0, 0))

        for query_num, timings in enumerate(result):
            for run_num, time_s in enumerate(timings, 1):
                rows.append({
                    "benchmark_id": benchmark_id,
                    "timestamp": date,
                    "instance_size": machine,
                    "vcpus": specs[0],
                    "memory_gb": specs[1],
                    "table_name": f"{dirname}_hits",
                    "query_number": f"Q{query_num}",
                    "run_number": run_num,
                    "time_seconds": time_s if time_s is not None else "",
                    "query_text": "",
                })

        print(f"  {benchmark_id}: {len(result)} queries")

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {OUTPUT}")


if __name__ == "__main__":
    convert()
