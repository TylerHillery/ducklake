# DuckLake Benchmark

> [!WARNING]
> This repository is primarily AI-generated and is intended only for testing, debugging, and learning purposes.

Performance benchmarking tool for [DuckLake](https://ducklake.select/) using Supabase (Postgres metadata + S3-compatible storage) or AWS S3.

## Setup

```bash
cp .env.example .env   # fill in your credentials
mise run install       # install python deps via uv
```

For AWS S3, also fill in `.env.aws`.

---

## Environment

| File | Purpose |
|---|---|
| `.env` | Supabase Postgres + Supabase Storage credentials |
| `.env.aws` | AWS SSO profile, region, and S3 bucket name |

`.env.aws` is only loaded when `--storage s3` is passed.

---

## Commands

### Default — SELECT + file counts
```bash
uv run main.py                    # Supabase storage (default)
uv run main.py --storage s3       # AWS S3 (requires: aws sso login --profile tyler-dev)
```
Prints a table with row count, scan time, file counts (data + delete files), size, and pending cleanup files.

---

### `--churn` — Generate DML transactions
Runs 333 transactions of the specified type, each affecting ~120 rows. Each transaction produces a separate DuckLake file — simulates CDC ingestion.

```bash
uv run main.py --churn            # all: 333 inserts + 333 updates + 333 deletes
uv run main.py --churn insert     # inserts only
uv run main.py --churn update     # updates only
uv run main.py --churn delete     # deletes only
uv run main.py --storage s3 --churn
```

---

### `--maintenance` — Full compaction + cleanup pipeline
Runs all maintenance ops in order, benchmarking scan time and file counts at each step:

1. `merge_adjacent_files` — tier 0→1: merge files `< 1MB` into `~5MB`
2. `merge_adjacent_files` — tier 1→2: merge files `1MB–10MB` into `~32MB`
3. `rewrite_data_files` — rewrite files with `> 50%` deleted rows into clean data files
4. `expire_snapshots` + `cleanup_old_files` + `delete_orphaned_files`

```bash
uv run main.py --maintenance
uv run main.py --storage s3 --maintenance
```

> **Note:** Requires a direct Postgres connection (not session pooler). The cleanup step holds an open Postgres transaction while making one HEAD request per S3 file — the session pooler will kill the connection before it finishes with large file counts.

---

### `--cleanup` — Expire snapshots and delete old/orphaned files
```bash
uv run main.py --cleanup                      # run full cleanup
uv run main.py --cleanup --dry-run            # count pending files without deleting
uv run main.py --storage s3 --cleanup
```

> **Note:** Makes one HTTP HEAD request per file to S3 before deleting. 2000+ files will take several minutes. Requires direct Postgres connection — see note above.

---

## Storage backends

| Flag | Backend | Metadata schema | Credentials |
|---|---|---|---|
| `--storage supa` (default) | Supabase Storage | `ducklake` | `.env` |
| `--storage s3` | AWS S3 | `ducklake_aws` | `.env.aws` (SSO) |

---

## Postgres connection

`.env` supports both direct and session pooler connections — comment/uncomment to switch:

```bash
# Direct connection (IPv4 addon required) — required for --cleanup and --maintenance
POSTGRES_HOST="db.<project-ref>.supabase.red"

# Session pooler (no IPv4 addon) — cleanup/maintenance will fail with large file counts
# POSTGRES_HOST="aws-0-<region>.pooler.supabase.green"
```

---

## mise tasks

```bash
mise run install          # install dependencies
mise run run              # SELECT + file count (Supabase)
mise run run-s3           # SELECT + file count (AWS S3)
mise run churn            # all churn (Supabase)
mise run churn-s3         # all churn (AWS S3)
mise run maintenance      # full maintenance pipeline (Supabase)
mise run maintenance-s3   # full maintenance pipeline (AWS S3)
mise run cleanup          # cleanup (Supabase)
mise run cleanup-dry-run  # dry run cleanup (Supabase)
```
