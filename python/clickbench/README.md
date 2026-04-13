# ClickBench — pg_duckdb + DuckLake on Supabase

> [!WARNING]
> **Object storage — not comparable to local-disk benchmarks.**
> All query data is read from Supabase Storage (S3-compatible object storage) at query time. The vast majority of ClickBench submissions read from local NVMe/SSD. Object storage reads are orders of magnitude slower due to network latency and throughput limits, so these results **cannot be fairly compared** against any benchmark that stores data on local disk.

Standard [ClickBench](https://github.com/ClickHouse/ClickBench) query suite (43 queries) run against a **DuckLake** table accessed via the **pg_duckdb** extension on a Supabase Postgres instance.

The `hits` dataset (~100M rows) lives in Supabase Storage (S3-compatible object storage) as DuckLake-managed Parquet files. Every query goes through pg_duckdb → DuckDB → DuckLake → S3.

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- `psql` installed (`brew install postgresql`)
- A Supabase project with:
  - **IPv4 add-on** enabled (required for direct connection)
  - Superuser / SSH access to complete the one-time admin setup below

---

## One-time Supabase Admin Setup

These steps require a superuser connection (e.g. SSH into the Postgres host and run as the `postgres` OS user, or connect as `supabase_admin`). They only need to be done once per project.

All statements are in [`admin.sql`](admin.sql) — you can run the whole file at once:

```bash
sudo -u postgres psql -f admin.sql
```

Or run statement by statement:

**1. Install pg_duckdb**

```sql
CREATE EXTENSION IF NOT EXISTS pg_duckdb SCHEMA extensions;
```

**2. Grant the duckdb FDW to the postgres role**

Without this the seed will fail with `permission denied for foreign-data wrapper duckdb`.

```sql
GRANT USAGE ON FOREIGN DATA WRAPPER duckdb TO postgres;
```

**3. Grant file access roles to postgres**

pg_duckdb disables `LocalFileSystem` for any role that lacks both `pg_read_server_files` and `pg_write_server_files`. DuckLake's extension loader must traverse local directories to find the `.duckdb_extension` file even when the extension is already installed — without these grants the ATTACH will fail with `File system LocalFileSystem has been disabled by configuration`.

```sql
GRANT pg_read_server_files  TO postgres;
GRANT pg_write_server_files TO postgres;
```

**4. Enable community extensions and install ducklake**

DuckLake is a community extension and must be unlocked before it can be installed. These GUCs are session-local so run them in the same `psql` session:

```sql
SET duckdb.allow_community_extensions = true;
SET duckdb.allow_unsigned_extensions  = true;
SELECT duckdb.install_extension('ducklake');
```

Verify everything is in place:

```sql
-- Should show pg_duckdb
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_duckdb';

-- Should show duckdb FDW
SELECT fdwname, fdwowner::regrole FROM pg_foreign_data_wrapper WHERE fdwname = 'duckdb';

-- Should show ducklake listed
SELECT * FROM duckdb.extensions WHERE extension_name = 'ducklake';
```

Before each benchmark you will also be prompted to set the `duckdb.memory_limit` to 75% of available memory of the instnace used.

---

## Step 1 — Fill in credentials

Edit `../.env.staging` (one level up from this directory):

S3 credentials come from **Supabase Dashboard → Storage → S3 Access**.

---

## Step 2 — Download the dataset

```bash
cd clickbench
wget --continue --progress=dot:giga \
  https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet
```

~14 GB download. Run from inside this directory — the seed expects `hits.parquet` here.

---

## Step 3 — Seed

### Base table

Creates `hits_14gb` from the raw parquet file. Uploads to Supabase Storage, sets up FDW + user mappings, and creates the DuckLake table.

```bash
uv run python clickbench/seed.py seed
```

### Table variants

Create tables with partitioning, sorting, or both. All variants read from the base `hits_14gb` table.

```bash
# Partitioned by EventDate
uv run python clickbench/seed.py seed --partitioned eventdate

# Sorted by CounterID
uv run python clickbench/seed.py seed --sorted counterid

# Both partitioned and sorted
uv run python clickbench/seed.py seed --partitioned eventdate --sorted counterid
```

### Scaled tables

Use `--replicate N` to insert the base data N times. The data size in the table name is computed as 13 * N.

```bash
# 2x replication → hits_26gb
uv run python clickbench/seed.py seed --replicate 2

# 5x replication + partitioned + sorted → hits_65gb_partitioned_eventdate_sorted_counterid
uv run python clickbench/seed.py seed --replicate 5 --partitioned eventdate --sorted counterid
```

### Table naming convention

Table names encode all configuration (always lowercase):

```
hits_{size}gb[_partitioned_{column}][_sorted_{column}]
```

| Flags | Table name |
|---|---|
| (none) | `hits_14gb` |
| `--replicate 2` | `hits_26gb` |
| `--partitioned eventdate` | `hits_14gb_partitioned_eventdate` |
| `--sorted counterid` | `hits_14gb_sorted_counterid` |
| `--replicate 4 --partitioned eventdate --sorted counterid` | `hits_52gb_partitioned_eventdate_sorted_counterid` |

The seed is **idempotent** — re-running skips tables that already have data.

---

## Step 4 — Run the benchmark

### Quick run (JSON output)

```bash
# Default: runs against hits_14gb
cd clickbench && ./benchmark.sh

# Specific table:
TABLE_NAME=hits_14gb_sorted_counterid ./benchmark.sh
```

Prints results as JSON arrays (one per query), raw output saved to `log.txt`.

### Full run with CSV results

```bash
uv run python clickbench/benchmark.py --instance small --table hits_14gb
uv run python clickbench/benchmark.py --instance small --table hits_14gb_sorted_counterid
uv run python clickbench/benchmark.py --instance 4xl --table hits_26gb_partitioned_eventdate
```

Results are appended to `results/clickbench_results.csv` with columns:

| Column | Example |
|---|---|
| `benchmark_id` | `small__hits_14gb_sorted_counterid` |
| `timestamp` | `2026-04-10T14:30:00Z` |
| `instance_size` | `small` |
| `vcpus` | `2` |
| `memory_gb` | `2` |
| `table_name` | `hits_14gb_sorted_counterid` |
| `query_number` | `Q0` |
| `run_number` | `1` |
| `time_seconds` | `0.155694` |
| `query_text` | `SELECT COUNT(*) FROM clickbench.main.hits_14gb_sorted_counterid` |

Each run produces 129 rows (43 queries x 3 runs). The CSV is append-friendly — run benchmarks with different instances/tables and all results accumulate in the same file.

---

## Maintenance

Run the DuckLake compaction and cleanup pipeline on any table:

```bash
uv run python clickbench/seed.py maintenance
uv run python clickbench/seed.py maintenance --sorted counterid
uv run python clickbench/seed.py maintenance --replicate 2 --partitioned eventdate
```

Steps: merge small files (tiered), rewrite files with deleted rows, expire snapshots, cleanup old/orphaned files.

---

## Teardown

Drop all DuckLake data and metadata (all tables, all variants):

```bash
uv run python clickbench/seed.py teardown
```

---

## Notes

**Cold vs. hot runs** — Because we can't restart Supabase's managed Postgres between queries, this is a *lukewarm cold run* in ClickBench's terminology (OS page cache is not cleared either). Results should be tagged `lukewarm-cold-run` if submitted to the ClickBench repo.

**Query execution path** — Queries use `duckdb.raw_query($$...$$)` which executes inside DuckDB and discards the result rows (returns void). This measures full execution time without requiring Postgres-typed column declarations.

**DuckLake naming** — The DuckLake catalog is attached as `clickbench`, metadata lives in the `clickbench_ducklake` Postgres schema, and data files are at `s3://<BUCKET>/clickbench/ducklake/`.

**pg_duckdb memory limit** — pg_duckdb defaults to 4 GB regardless of instance RAM. `benchmark.py` automatically sets `duckdb.memory_limit` to 75% of the instance's RAM. When running `run.sh` directly, pass `MEMORY_LIMIT_MB=0` to let DuckDB auto-detect (80% of RAM).

### Test matrix

Combinations to benchmark, based on query analysis of the 43 ClickBench queries:

**Sorting candidates** — sort improves file-level min/max pruning (WHERE filters) and ordered aggregation (GROUP BY):

| Sort column | Rationale |
|---|---|
| `userid` | Highest cardinality. 5 GROUP BYs (Q16-19, Q9), 1 point lookup (Q20: `WHERE UserID = ...`) |
| `counterid` | 7 WHERE equality filters (Q37-43: `CounterID = 62`), enables file skipping |

**Partitioning candidates** — partition pruning skips entire file groups on range filters:

| Partition column | Rationale |
|---|---|
| `eventdate` | 7 date-range filters (Q37-43: `EventDate >= ... AND EventDate <= ...`) |

**Tables to create and benchmark:**

```bash
# Baseline
uv run python clickbench/seed.py seed

# Sort only — compare UserID vs CounterID
uv run python clickbench/seed.py seed --sorted userid
uv run python clickbench/seed.py seed --sorted counterid

# Partition only
uv run python clickbench/seed.py seed --partitioned eventdate

# Partition + sort
uv run python clickbench/seed.py seed --partitioned eventdate --sorted userid
uv run python clickbench/seed.py seed --partitioned eventdate --sorted counterid
```

**Expected outcomes:**
- `sorted_userid` — faster GROUP BY UserID queries (Q16-19), near-instant point lookup (Q20), tighter min/max stats across the board due to high cardinality
- `sorted_counterid` — faster Q37-43 (file pruning on `CounterID = 62`), but less benefit for GROUP BY queries
- `partitioned_eventdate` — prunes files for Q37-43 date ranges, but may hurt full-scan queries (Q0-Q36) due to many small files
- `partitioned_eventdate_sorted_userid` — best of both: date pruning + high-cardinality sort
