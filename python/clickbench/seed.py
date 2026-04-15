#!/usr/bin/env python3
"""
Seed script for ClickBench pg_duckdb + DuckLake on Supabase.

Steps:
  1. Create S3 bucket if it doesn't exist
  2. Upload hits.parquet to S3 (idempotent — skip if already uploaded)
  3. Set up pg_duckdb foreign server + user mappings via DuckDB postgres extension
  4. Attach DuckLake locally and CREATE TABLE clickbench.main.hits (idempotent)

Run from the repo root:
  uv run python clickbench/seed.py
"""

import argparse
import os
import time
from pathlib import Path

import boto3
import duckdb
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from rich import box
from rich.console import Console
from rich.table import Table

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

_env_path = Path(__file__).parent.parent.parent / ".env.staging"
load_dotenv(_env_path)

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_DATABASE = os.environ["POSTGRES_DATABASE"]
POSTGRES_USERNAME = os.environ["POSTGRES_USERNAME"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
ENDPOINT_URL = os.environ["ENDPOINT_URL"]  # host only, no scheme
BUCKET_NAME = os.environ["BUCKET_NAME"]

RAW_KEY = "clickbench/raw/hits.parquet"
S3_RAW_PATH = f"s3://{BUCKET_NAME}/{RAW_KEY}"
DUCKLAKE_DATA_PATH = f"s3://{BUCKET_NAME}/clickbench/ducklake/"
DUCKLAKE_ALIAS = "clickbench"
METADATA_SCHEMA = "clickbench_ducklake"
LOCAL_PARQUET = Path(__file__).parent / "hits.parquet"
REMOTE_PARQUET = "https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet"
BASE_SIZE_GB = 14
BASE_TABLE = "hits_14gb"

console = Console()


def _table_name(
    replicate: int = 1, partitioned: str | None = None, sorted: str | None = None
) -> str:
    size_gb = BASE_SIZE_GB * replicate
    name = f"hits_{size_gb}gb"
    if partitioned:
        name += f"_partitioned_{partitioned.lower()}"
    if sorted:
        name += f"_sorted_{sorted.lower()}"
    return name


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ENDPOINT_URL}",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3) -> None:
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        console.print(f"[green]Created bucket:[/green] {BUCKET_NAME}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou", "409"):
            console.print(f"[dim]Bucket already exists:[/dim] {BUCKET_NAME}")
        else:
            raise


def parquet_uploaded(s3) -> bool:
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=RAW_KEY)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def upload_parquet(_s3) -> None:
    """Read local hits.parquet, convert timestamp columns, write single file to S3."""
    if not LOCAL_PARQUET.exists():
        console.print(f"[red]Error:[/red] {LOCAL_PARQUET} not found.")
        console.print("Download it first:")
        console.print(
            "  wget --continue --progress=dot:giga "
            "https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet "
            f"-O {LOCAL_PARQUET}"
        )
        raise SystemExit(1)

    file_size = LOCAL_PARQUET.stat().st_size
    console.print(
        f"Converting + uploading [bold]{LOCAL_PARQUET.name}[/bold] "
        f"({file_size / 1e9:.1f} GB) → {S3_RAW_PATH}"
    )
    console.print(
        "  [dim]EventDate → DATE, EventTime/ClientEventTime/LocalEventTime → TIMESTAMP[/dim]"
    )
    console.print("  [dim](this will take a while)[/dim]")

    with duckdb.connect() as conn:
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"""
            CREATE SECRET supabase_storage (
                TYPE S3,
                KEY_ID '{AWS_ACCESS_KEY_ID}',
                SECRET '{AWS_SECRET_ACCESS_KEY}',
                ENDPOINT '{ENDPOINT_URL}',
                REGION '{AWS_REGION}',
                URL_STYLE 'path',
                USE_SSL true,
                SCOPE 's3://{BUCKET_NAME}'
            )
        """)
        # Supabase Storage sits behind Cloudflare with a 100s origin timeout.
        # Single-threaded upload avoids concurrent-part 502s on large files.
        conn.execute("SET s3_uploader_max_filesize='100gb';")
        conn.execute("SET s3_uploader_thread_limit=1;")

        # Writing to a specific .parquet path (not a directory) produces a single file.
        conn.execute(f"""
            COPY (
                SELECT * REPLACE (
                    (DATE '1970-01-01' + EventDate::INTEGER) AS EventDate,
                    to_timestamp(EventTime)       AS EventTime,
                    to_timestamp(ClientEventTime) AS ClientEventTime,
                    to_timestamp(LocalEventTime)  AS LocalEventTime
                )
                FROM read_parquet({str(LOCAL_PARQUET)!r}, binary_as_string => true)
            ) TO {S3_RAW_PATH!r} (FORMAT PARQUET)
        """)

    console.print("[green]✓[/green] Upload complete.")


# ---------------------------------------------------------------------------
# FDW setup via DuckDB postgres extension
# ---------------------------------------------------------------------------


def setup_fdw() -> None:
    console.print("Setting up pg_duckdb foreign server + user mappings…")

    pg_conn = (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DATABASE} user={POSTGRES_USERNAME} password={POSTGRES_PASSWORD}"
    )

    with duckdb.connect() as conn:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

        # Foreign server
        conn.execute(f"""
            CALL postgres_execute('pg', $$
                CREATE SERVER IF NOT EXISTS duckdb_supabase_storage_foreign_server
                TYPE 's3'
                FOREIGN DATA WRAPPER duckdb
                OPTIONS (
                    endpoint '{ENDPOINT_URL}',
                    region '{AWS_REGION}',
                    url_style 'path',
                    use_ssl 'true',
                    scope 's3://{BUCKET_NAME}/'
                )
            $$)
        """)
        console.print("[green]✓[/green] Foreign server ready")

        # User mappings for the three roles that may run pg_duckdb queries
        for role in ("postgres", "service_role", "supabase_admin"):
            conn.execute(f"""
                CALL postgres_execute('pg', $$
                    CREATE USER MAPPING IF NOT EXISTS FOR {role}
                    SERVER duckdb_supabase_storage_foreign_server
                    OPTIONS (
                        KEY_ID '{AWS_ACCESS_KEY_ID}',
                        SECRET '{AWS_SECRET_ACCESS_KEY}'
                    )
                $$)
            """)
        console.print("[green]✓[/green] User mappings ready (postgres, service_role)")


# ---------------------------------------------------------------------------
# DuckLake ATTACH + CREATE TABLE (runs DuckDB locally, connects to Supabase PG)
# ---------------------------------------------------------------------------

def _attach_ducklake(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure S3 + DuckLake on an open connection."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    # Use a secret so all S3 operations (including ducklake_delete_orphaned_files'
    # internal read_blob calls) pick up the right credentials and region.
    conn.execute(f"""
        CREATE SECRET supabase_storage (
            TYPE S3,
            KEY_ID '{AWS_ACCESS_KEY_ID}',
            SECRET '{AWS_SECRET_ACCESS_KEY}',
            ENDPOINT '{ENDPOINT_URL}',
            REGION '{AWS_REGION}',
            URL_STYLE 'path',
            USE_SSL true,
            SCOPE 's3://{BUCKET_NAME}'
        )
    """)
    # Supabase Storage sits behind Cloudflare with a 100s origin timeout.
    # Default part size (~80 MB = 800 GB / 10000 parts) can exceed that,
    # and concurrent uploads can 502 the origin.  Smaller parts + single
    # upload thread keeps it reliable.
    conn.execute("SET s3_uploader_max_filesize='100gb';")
    conn.execute("SET s3_uploader_thread_limit=1;")

    attach_str = (
        f"ducklake:postgres:host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DATABASE} user={POSTGRES_USERNAME} password={POSTGRES_PASSWORD}"
    )
    conn.execute(f"""
        ATTACH '{attach_str}'
        AS {DUCKLAKE_ALIAS} (
            DATA_PATH       '{DUCKLAKE_DATA_PATH}',
            METADATA_SCHEMA '{METADATA_SCHEMA}'
        )
    """)


def _ensure_base_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Make sure hits_14gb exists (zero-copy: registers parquet without copying data)."""
    fq = f"{DUCKLAKE_ALIAS}.main.{BASE_TABLE}"
    try:
        row_count = conn.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        if row_count > 0:
            console.print(f"[dim]Base table {fq} has {row_count:,} rows.[/dim]")
            return
        conn.execute(f"DROP TABLE {fq}")
    except Exception:
        pass

    console.print(f"Creating {fq} (zero-copy from {S3_RAW_PATH})…")
    # Both schema inference and file registration use the S3 path so that
    # pg_duckdb on the remote Postgres server can read the data.
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {fq} AS "
        f"SELECT * FROM read_parquet({S3_RAW_PATH!r}, binary_as_string => true) WHERE 1=0"
    )
    conn.execute(
        f"CALL ducklake_add_data_files('{DUCKLAKE_ALIAS}', '{BASE_TABLE}', {S3_RAW_PATH!r})"
    )
    console.print(f"[green]✓[/green] {fq} ready (zero-copy)")


def create_ducklake_table(
    replicate: int = 1,
    partitioned: str | None = None,
    sorted: str | None = None,
) -> None:
    """Create a DuckLake table variant from the base hits_14gb table."""
    table = _table_name(replicate, partitioned, sorted)
    fq = f"{DUCKLAKE_ALIAS}.main.{table}"
    base_fq = f"{DUCKLAKE_ALIAS}.main.{BASE_TABLE}"

    with duckdb.connect() as conn:
        _attach_ducklake(conn)
        _ensure_base_table(conn)

        # If the requested table IS the base table and it already exists, we're done
        if table == BASE_TABLE:
            return

        # Idempotency: skip if table already has data
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
            if row_count > 0:
                console.print(
                    f"[dim]Table {fq} already has {row_count:,} rows — skipping.[/dim]"
                )
                return
            console.print(
                f"[yellow]Table {fq} exists but is empty — dropping and re-creating.[/yellow]"
            )
            conn.execute(f"DROP TABLE {fq}")
        except Exception:
            pass

        console.print(f"Creating {fq}…")
        order_clause = f" ORDER BY {sorted}" if sorted else ""

        if partitioned:
            # Create empty table with schema, set partition key, then insert
            conn.execute(f"CREATE TABLE {fq} AS SELECT * FROM {base_fq} WHERE 1=0")
            conn.execute(f"ALTER TABLE {fq} SET PARTITIONED BY ({partitioned})")
            console.print(f"  Partitioned by {partitioned}")
            conn.execute(f"INSERT INTO {fq} SELECT * FROM {base_fq}{order_clause}")
        else:
            conn.execute(f"CREATE TABLE {fq} AS SELECT * FROM {base_fq}{order_clause}")

        if sorted:
            console.print(f"  Sorted by {sorted}")

        # Replication: insert from base table (replicate-1) more times
        for i in range(replicate - 1):
            console.print(f"  Replicating ({i + 2}/{replicate})…")
            conn.execute(f"INSERT INTO {fq} SELECT * FROM {base_fq}{order_clause}")

        final_count = conn.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        console.print(f"[green]✓[/green] {fq} ready — {final_count:,} rows")


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


def maintenance(
    replicate: int = 1,
    partitioned: str | None = None,
    sorted: str | None = None,
) -> None:
    """Run the full DuckLake maintenance pipeline with benchmarks at each step."""
    table = _table_name(replicate, partitioned, sorted)
    console.rule(f"[bold]ClickBench Maintenance — {table}[/bold]")
    alias = DUCKLAKE_ALIAS
    fq_table = f"{alias}.main.{table}"

    with duckdb.connect() as conn:
        _attach_ducklake(conn)

        def file_stats():
            row = conn.execute(f"""
                SELECT
                    count(*) AS total,
                    count(*) FILTER (WHERE delete_file IS NULL) AS data_files,
                    count(*) FILTER (WHERE delete_file IS NOT NULL) AS delete_files,
                    round(sum(data_file_size_bytes) / 1024.0 / 1024.0, 2) AS total_mb
                FROM ducklake_list_files('{alias}', '{table}')
            """).fetchone()
            return row

        def pending_counts():
            scheduled = conn.execute(f"""
                SELECT count(*)
                FROM __ducklake_metadata_{alias}.{METADATA_SCHEMA}.ducklake_files_scheduled_for_deletion
            """).fetchone()[0]
            orphaned = conn.execute(f"""
                SELECT count(*)
                FROM ducklake_delete_orphaned_files('{alias}', dry_run => true, older_than => now() - INTERVAL '0 seconds')
            """).fetchone()[0]
            return scheduled, orphaned

        def bench(label, maint_note=""):
            total, data, deletes, total_mb = file_stats()
            scheduled, orphaned = pending_counts()
            t0 = time.perf_counter()
            row_count = conn.execute(f"SELECT count(*) FROM {fq_table}").fetchone()[0]
            scan = time.perf_counter() - t0
            results.add_row(
                label,
                f"{row_count:,}",
                f"{scan:.3f}s",
                str(total),
                str(data),
                str(deletes),
                f"{total_mb:.1f}MB",
                str(scheduled),
                str(orphaned),
                maint_note,
            )

        results = Table(
            box=box.SIMPLE_HEAVY, show_header=True, header_style="bold cyan"
        )
        results.add_column("Step", style="bold white", no_wrap=True)
        results.add_column("Rows", justify="right", style="green")
        results.add_column("Scan", justify="right", style="yellow")
        results.add_column("Files", justify="right")
        results.add_column("Data", justify="right", style="cyan")
        results.add_column("Deletes", justify="right", style="red")
        results.add_column("Size", justify="right", style="magenta")
        results.add_column("Scheduled", justify="right", style="dim")
        results.add_column("Orphaned", justify="right", style="dim")
        results.add_column("Maintenance", style="dim white", no_wrap=False)

        bench("baseline")

        # tier 0→1: merge tiny files (<1 MB) into ~5 MB
        conn.execute(f"CALL ducklake_set_option('{alias}', 'target_file_size', '5MB')")
        t0 = time.perf_counter()
        conn.execute(
            f"CALL ducklake_merge_adjacent_files('{alias}', max_file_size => 1048576)"
        )
        t = time.perf_counter() - t0
        bench("after merge", f"merge_adjacent_files(<1MB->5MB) {t:.3f}s")

        # tier 1→2: merge small files (1–10 MB) into ~32 MB
        conn.execute(f"CALL ducklake_set_option('{alias}', 'target_file_size', '32MB')")
        t0 = time.perf_counter()
        conn.execute(
            f"CALL ducklake_merge_adjacent_files('{alias}', min_file_size => 1048576, max_file_size => 10485760)"
        )
        t = time.perf_counter() - t0
        bench("after merge", f"merge_adjacent_files(1MB-10MB->32MB) {t:.3f}s")

        # tier 2→3: merge medium files (10–64 MB) into ~128 MB
        conn.execute(
            f"CALL ducklake_set_option('{alias}', 'target_file_size', '128MB')"
        )
        t0 = time.perf_counter()
        conn.execute(
            f"CALL ducklake_merge_adjacent_files('{alias}', min_file_size => 10485760, max_file_size => 67108864)"
        )
        t = time.perf_counter() - t0
        bench("after merge", f"merge_adjacent_files(10MB-64MB->128MB) {t:.3f}s")

        # rewrite files with deleted rows
        t0 = time.perf_counter()
        conn.execute(
            f"CALL ducklake_rewrite_data_files('{alias}', '{table}', delete_threshold => 0.0)"
        )
        t = time.perf_counter() - t0
        bench("after rewrite", f"rewrite_data_files(delete_threshold=0.0) {t:.3f}s")

        # expire snapshots + cleanup old files + delete orphaned files
        t0 = time.perf_counter()
        conn.execute(f"CALL ducklake_expire_snapshots('{alias}', older_than => now())")
        conn.execute(
            f"CALL ducklake_cleanup_old_files('{alias}', older_than => now() - INTERVAL '0 seconds')"
        )
        conn.execute(
            f"CALL ducklake_delete_orphaned_files('{alias}', older_than => now() - INTERVAL '0 seconds')"
        )
        t = time.perf_counter() - t0
        bench("after cleanup", f"expire + cleanup + delete_orphaned {t:.3f}s")

        console.print(results)

    console.rule("[bold green]Maintenance complete[/bold green]")


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------


def teardown() -> None:
    """Delete all DuckLake data files from S3 and drop the metadata schema."""
    console.rule("[bold red]ClickBench Teardown[/bold red]")

    # 1. Delete all objects under the DuckLake data prefix in S3
    prefix = "clickbench/ducklake/"
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    # Supabase Storage doesn't support batch DeleteObjects — delete one at a time.
    total = 0
    for page in pages:
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            total += 1
            if total % 50 == 0:
                console.print(f"  deleted {total} objects…")

    if total == 0:
        console.print(f"[dim]No objects found under s3://{BUCKET_NAME}/{prefix}[/dim]")
    else:
        console.print(
            f"[green]✓[/green] Deleted {total} objects from s3://{BUCKET_NAME}/{prefix}"
        )

    # 2. Drop the DuckLake metadata schema in Postgres
    pg_conn = (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DATABASE} user={POSTGRES_USERNAME} password={POSTGRES_PASSWORD}"
    )
    with duckdb.connect() as conn:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")
        conn.execute(
            f"CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS {METADATA_SCHEMA} CASCADE')"
        )
    console.print(f"[green]✓[/green] Dropped schema {METADATA_SCHEMA}")

    console.rule("[bold red]Teardown complete[/bold red]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _add_variant_args(p: argparse.ArgumentParser) -> None:
    """Add --replicate, --partitioned, --sorted to a subparser."""
    p.add_argument(
        "--replicate",
        type=int,
        default=1,
        help="Number of times to insert base data (default: 1 = ~14gb)",
    )
    p.add_argument(
        "--partitioned",
        type=str,
        default=None,
        metavar="COLUMN",
        help="Partition column (e.g. eventdate)",
    )
    p.add_argument(
        "--sorted",
        type=str,
        default=None,
        metavar="COLUMN",
        help="Sort column (e.g. counterid)",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ClickBench seed / maintenance / teardown for DuckLake"
    )
    sub = parser.add_subparsers(dest="command")

    seed_p = sub.add_parser("seed", help="Seed the base table and/or a variant table")
    _add_variant_args(seed_p)

    maint_p = sub.add_parser("maintenance", help="Run maintenance pipeline on a table")
    _add_variant_args(maint_p)

    args = parser.parse_args()
    # Default to seed when no subcommand given
    if args.command is None:
        args.command = "seed"
        args.replicate = 1
        args.partitioned = None
        args.sorted = None

    if args.command == "teardown":
        teardown()
        return

    if args.command == "maintenance":
        maintenance(args.replicate, args.partitioned, args.sorted)
        return

    # seed
    table = _table_name(args.replicate, args.partitioned, args.sorted)
    console.rule(f"[bold]ClickBench Seed — {table}[/bold]")

    s3 = _s3_client()

    ensure_bucket(s3)

    if parquet_uploaded(s3):
        console.print(
            f"[dim]Parquet already in S3, skipping upload:[/dim] {S3_RAW_PATH}"
        )
    else:
        upload_parquet(s3)

    setup_fdw()
    create_ducklake_table(args.replicate, args.partitioned, args.sorted)

    console.rule("[green bold]Seed complete[/green bold]")


if __name__ == "__main__":
    main()
