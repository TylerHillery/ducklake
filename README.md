# Overview

> [!WARNING]
> This repository is primarily AI-generated and is intended only for testing, debugging, and learning purposes.

Exploratory sandbox for running [DuckLake](https://ducklake.select/) on the [Supabase](https://supabase.com) platform. The goal is to understand how DuckLake behaves with Supabase Postgres as the metadata catalog and Supabase Storage (S3-compatible) as the data layer.

DuckDB can be embedded in many different runtimes — this repo experiments with several of them in the context of Supabase:

- **Python** — DuckDB process running locally, connecting directly to Supabase Postgres + Storage
- **Deno / Edge Functions** — DuckDB via node-api or WASM running inside a Supabase Edge Function
- **pg_duckdb** — DuckDB embedded inside Postgres itself, accessed through SQL

```
python/          Python tooling — benchmarks, seeding, maintenance
  main.py          Interactive DuckLake CLI (attach, query, inspect)
  pyproject.toml   uv project (Python 3.13+)
  clickbench/      ClickBench suite adapted for DuckLake on Supabase

deno/            TypeScript / Deno tooling — edge functions, WASM experiments
  supabase/        Supabase Edge Functions running DuckDB (node-api + WASM)
  test_duckdb.ts   Smoke test via @duckdb/node-api
  test_duckdb_wasm.ts  Smoke test using custom duckdb-wasm build
  deno.json        Import map + task runner
```

## Configuration

Copy `.env.example` to `.env` and fill in your Supabase credentials:

```bash
cp .env.example .env
# edit .env with your POSTGRES_*, AWS_*, ENDPOINT_URL, BUCKET_NAME values
```

## Python (`python/`)

```bash
cd python
uv sync

# Interactive CLI — attach DuckLake, run queries, inspect catalog
uv run main.py

# ClickBench — seed, benchmark, maintain
uv run clickbench/seed.py          # zero-copy load of hits.parquet → DuckLake
uv run clickbench/benchmark.py --instance small --table hits_14gb
```

See [`python/clickbench/README.md`](python/clickbench/README.md) for the full ClickBench workflow including partitioning, sorting, replication, and maintenance.

## Deno (`deno/`)

```bash
cd deno

# Smoke tests
deno run --allow-all test_duckdb.ts       # DuckDB via node-api
deno run --allow-all test_duckdb_wasm.ts  # DuckDB via WASM (custom build)

# Deploy edge functions
deno task supabase functions deploy hello-world
deno task supabase functions deploy hello-world-wasm
```

The WASM build is sourced from a local custom build of `@duckdb/duckdb-wasm` rather than the published npm package.
