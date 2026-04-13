#!/usr/bin/env python3
"""
ClickBench benchmark runner for DuckLake on Supabase.

Wraps run.sh (psql against pg_duckdb), parses timings, and writes
denormalized CSV results with instance metadata.

Usage:
  uv run python clickbench/benchmark.py --instance small --table hits_13gb
  uv run python clickbench/benchmark.py --instance 4xl --table hits_13gb_partitioned_eventdate_sorted_counterid
"""

import argparse
import csv
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Supabase instance specs (all ARM / AWS Graviton)
# ---------------------------------------------------------------------------

INSTANCE_SPECS = {
    "nano":  {"vcpus": 0,  "memory_gb": 0.5},
    "micro": {"vcpus": 2,  "memory_gb": 1},
    "small": {"vcpus": 2,  "memory_gb": 2},
    "medium":{"vcpus": 2,  "memory_gb": 4},
    "large": {"vcpus": 2,  "memory_gb": 8},
    "xl":    {"vcpus": 4,  "memory_gb": 16},
    "2xl":   {"vcpus": 8,  "memory_gb": 32},
    "4xl":   {"vcpus": 16, "memory_gb": 64},
    "8xl":   {"vcpus": 32, "memory_gb": 128},
    "12xl":  {"vcpus": 48, "memory_gb": 192},
    "16xl":  {"vcpus": 64, "memory_gb": 256},
}

TRIES = 3
SCRIPT_DIR = Path(__file__).parent
QUERIES_FILE = SCRIPT_DIR / "queries.sql"

# Regex to extract the inner DuckDB SQL from the psql wrapper
_INNER_SQL_RE = re.compile(
    r"SELECT duckdb\.raw_query\(\$\$(.+?)\$\$\);?", re.IGNORECASE
)
_TIMING_RE = re.compile(r"Time:\s+([\d.]+)\s+ms")


def _load_queries() -> list[str]:
    """Load query texts from queries.sql (the inner DuckDB SQL)."""
    queries = []
    for line in QUERIES_FILE.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        m = _INNER_SQL_RE.search(line)
        queries.append(m.group(1) if m else line)
    return queries


def _run_benchmark(table_name: str, memory_limit_mb: int, num_queries: int) -> str:
    """Execute run.sh streaming progress, return full output for parsing."""
    env = os.environ.copy()
    env["TABLE_NAME"] = table_name
    env["MEMORY_LIMIT_MB"] = str(memory_limit_mb)
    proc = subprocess.Popen(
        ["bash", "run.sh"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(SCRIPT_DIR),
        env=env,
    )

    lines = []
    query_num = 0
    for line in proc.stdout:
        lines.append(line)
        # Detect query start
        if line.startswith("SELECT duckdb.raw_query("):
            query_num += 1
            inner = _INNER_SQL_RE.search(line)
            short = inner.group(1)[:80] if inner else line[:80]
            print(f"  [{query_num}/{num_queries}] {short}…", flush=True)
        # Show timing inline
        m = _TIMING_RE.search(line)
        if m:
            ms = float(m.group(1))
            if ms >= 1000:
                print(f"           {ms / 1000:.3f}s", flush=True)
            else:
                print(f"           {ms:.1f}ms", flush=True)

    proc.wait()
    return "".join(lines)


def _parse_timings(output: str) -> list[dict]:
    """
    Parse run.sh output into a list of {query_num, run_num, time_seconds}.

    Each query is echoed, then 3 runs with `Time: NNN.NNN ms` lines.
    ERROR lines before a Time line mark that run as null.
    """
    results = []
    query_num = -1
    run_num = 0
    saw_error = False

    for line in output.splitlines():
        # Detect query echo line (starts with SELECT duckdb.raw_query)
        if line.startswith("SELECT duckdb.raw_query("):
            query_num += 1
            run_num = 0
            saw_error = False
            continue

        if "ERROR:" in line:
            saw_error = True
            continue

        m = _TIMING_RE.search(line)
        if m:
            run_num += 1
            if saw_error:
                results.append({"query_num": query_num, "run_num": run_num, "time_seconds": None})
                saw_error = False
            else:
                time_s = round(float(m.group(1)) / 1000, 6)
                results.append({"query_num": query_num, "run_num": run_num, "time_seconds": time_s})

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="ClickBench benchmark runner for DuckLake on Supabase")
    parser.add_argument("--instance", required=True, choices=list(INSTANCE_SPECS.keys()), help="Supabase instance size")
    parser.add_argument("--table", required=True, help="DuckLake table name (e.g. hits_13gb_sorted_counterid)")
    parser.add_argument("--output", default=str(SCRIPT_DIR / "results"), help="Output directory for CSV (default: clickbench/results/)")
    args = parser.parse_args()

    specs = INSTANCE_SPECS[args.instance]
    benchmark_id = f"{args.instance}__{args.table}"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    queries = _load_queries()

    # pg_duckdb defaults to 4GB. Use 75% of instance RAM for DuckDB,
    # leave the rest for Postgres and OS.
    memory_limit_mb = int(specs["memory_gb"] * 1024 * 0.75)

    print(f"Benchmark: {benchmark_id}")
    print(f"Instance:  {args.instance} ({specs['vcpus']} vCPUs, {specs['memory_gb']} GB RAM, ARM)")
    print(f"Memory:    duckdb.memory_limit = {memory_limit_mb} MB (75% of instance RAM)")
    print(f"Table:     {args.table}")
    print(f"Queries:   {len(queries)}")
    print()

    # Run the benchmark
    print("Running benchmark via run.sh …\n")
    output = _run_benchmark(args.table, memory_limit_mb, len(queries))

    # Parse timings
    timings = _parse_timings(output)
    expected = len(queries) * TRIES
    if len(timings) != expected:
        print(f"Warning: expected {expected} timing entries, got {len(timings)}")

    # Write CSV
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "clickbench_results.csv"

    file_exists = csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "benchmark_id", "timestamp", "instance_size", "vcpus", "memory_gb",
            "table_name", "query_number", "run_number", "time_seconds", "query_text",
        ])
        if not file_exists:
            writer.writeheader()

        for t in timings:
            qnum = t["query_num"]
            query_text = queries[qnum] if qnum < len(queries) else ""
            writer.writerow({
                "benchmark_id": benchmark_id,
                "timestamp": timestamp,
                "instance_size": args.instance,
                "vcpus": specs["vcpus"],
                "memory_gb": specs["memory_gb"],
                "table_name": args.table,
                "query_number": f"Q{qnum}",
                "run_number": t["run_num"],
                "time_seconds": t["time_seconds"] if t["time_seconds"] is not None else "",
                "query_text": query_text,
            })

    print(f"\nResults appended to {csv_path}")
    print(f"  {len(timings)} rows ({len(timings) // TRIES} queries x {TRIES} runs)")


if __name__ == "__main__":
    main()
