import argparse
import os
import platform
import subprocess
import time
from pathlib import Path

import duckdb
import psutil
from dotenv import load_dotenv
from tqdm import tqdm
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from rich import box

console = Console()
results_table = None


def make_table():
    t = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold cyan")
    t.add_column("Step", style="bold white", no_wrap=True)
    t.add_column("Rows", justify="right", style="green")
    t.add_column("Scan", justify="right", style="yellow")
    t.add_column("Files", justify="right")
    t.add_column("Data", justify="right", style="cyan")
    t.add_column("Deletes", justify="right", style="red")
    t.add_column("Size", justify="right", style="magenta")
    t.add_column("Scheduled", justify="right", style="dim")
    t.add_column("Orphaned", justify="right", style="dim")
    t.add_column("Maintenance", style="dim white", no_wrap=False)
    return t


parser = argparse.ArgumentParser()
parser.add_argument("--storage", choices=["supa", "s3"], default="supa")
parser.add_argument("--log", action="store_true", help="Enable DuckDB debug logging (query duckdb_logs after run)")
parser.add_argument("--maintenance", action="store_true", help="Run maintenance ops")
parser.add_argument(
    "--cleanup",
    action="store_true",
    help="Run expire snapshots + cleanup old files + delete orphaned files",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Use with --cleanup to count files pending deletion without deleting them",
)
parser.add_argument(
    "--churn",
    nargs="?",
    const="all",
    choices=["all", "insert", "update", "delete"],
    help="Run churn transactions: all (default), insert, update, or delete (~120 rows each, 333 tx)",
)
args = parser.parse_args()

load_dotenv(Path(__file__).parent.parent / ".env")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")

AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

duckdb.sql("INSTALL postgres")

duckdb.sql("INSTALL ducklake")


def _cpu_name():
    try:
        return subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True
        ).stdout.strip()
    except Exception:
        return platform.processor() or platform.machine()


cpu = _cpu_name()
ram = round(psutil.virtual_memory().total / 1024**3, 1)
cores = psutil.cpu_count(logical=False)
duckdb_version = duckdb.__version__

PG = f"ducklake:postgres:dbname={POSTGRES_DATABASE} user={POSTGRES_USERNAME} host={POSTGRES_HOST} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}"

if args.storage == "s3":
    load_dotenv(Path(__file__).parent.parent / ".env.aws", override=True)
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION")
    STORAGE = f"AWS S3 (s3://{BUCKET_NAME}/)"
    duckdb.sql(f"""
DROP SECRET IF EXISTS aws_s3;
CREATE SECRET aws_s3 (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'sso;config',
    REGION '{AWS_REGION}',
    SCOPE 's3://{BUCKET_NAME}'
)
""")
    duckdb.sql(
        f"ATTACH '{PG}' AS my_ducklake (DATA_PATH 's3://{BUCKET_NAME}/', METADATA_SCHEMA 'ducklake_aws'); USE my_ducklake;"
    )
else:
    STORAGE = f"Supabase Storage (s3://{BUCKET_NAME}/)"
    duckdb.sql(f"""
DROP SECRET IF EXISTS supabase_storage;
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
    duckdb.sql(
        f"ATTACH '{PG}' AS my_ducklake (DATA_PATH 's3://{BUCKET_NAME}/', METADATA_SCHEMA 'ducklake'); USE my_ducklake;"
    )

if args.log:
    duckdb.sql("CALL enable_logging(level => 'debug')")

pg_host = POSTGRES_HOST
pg_user = POSTGRES_USERNAME
connection_method = "direct" if "pooler" not in pg_host else "session pooler"

console.print(
    f"[bold]Client[/bold]   cpu: {cpu}  cores: {cores}  ram: {ram}GB  duckdb: v{duckdb_version}"
)
console.print(
    f"[bold]Postgres[/bold] Supabase small (2GB RAM, 2-core ARM)  user: {pg_user}  host: {pg_host}  connection: {connection_method}  region: {AWS_REGION}"
)
console.print(f"[bold]Storage[/bold]  {STORAGE}\n")

duckdb.sql("""
CREATE TABLE IF NOT EXISTS yellow_tripdata AS
FROM '/Users/tyler/code/work/demos/supabase-pg-duckdb/data/yellow_tripdata_2023-01.parquet';
""")


def file_count():
    row = duckdb.sql("""
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE delete_file IS NULL) AS data_files,
            count(*) FILTER (WHERE delete_file IS NOT NULL) AS delete_files,
            round(sum(data_file_size_bytes) / 1024.0 / 1024.0, 2) AS total_mb
        FROM ducklake_list_files('my_ducklake', 'yellow_tripdata')
    """).fetchone()
    return row[0], row[1], row[2], row[3]


def pending_counts():
    scheduled = duckdb.sql("""
        SELECT count(*) FROM __ducklake_metadata_my_ducklake.ducklake.ducklake_files_scheduled_for_deletion
    """).fetchone()[0]
    orphaned = duckdb.sql("""
        SELECT count(*) FROM ducklake_delete_orphaned_files('my_ducklake', dry_run => true, older_than => now() - INTERVAL '0 seconds')
    """).fetchone()[0]
    return scheduled, orphaned


def bench(label, maintenance=None):
    global results_table
    total, data, deletes, total_mb = file_count()
    scheduled, orphaned = pending_counts()
    t0 = time.perf_counter()
    row_count = duckdb.sql("SELECT count(*) FROM yellow_tripdata").fetchone()[0]
    scan = time.perf_counter() - t0
    results_table.add_row(
        label,
        f"{row_count:,}",
        f"{scan:.3f}s",
        str(total),
        str(data),
        str(deletes),
        f"{total_mb:.1f}MB",
        str(scheduled),
        str(orphaned),
        maintenance or "",
    )


def print_pg_logs():
    sql_rows = duckdb.sql("""
        SELECT timestamp::VARCHAR, message
        FROM duckdb_logs
        WHERE log_level = 'INFO'
          AND message NOT ILIKE '%duckdb_logs%'
        ORDER BY timestamp
    """).fetchall()

    try:
        http_rows = duckdb.sql("""
            SELECT timestamp::VARCHAR,
                   request.type AS method,
                   request.url AS url,
                   request.duration_ms AS duration_ms,
                   response.status AS status
            FROM duckdb_logs_parsed('HTTP')
            ORDER BY timestamp
        """).fetchall()
    except Exception:
        http_rows = []

    tagged = [(ts, "sql", msg) for ts, msg in sql_rows] + [
        (ts, "http", (method, url, dur, status))
        for ts, method, url, dur, status in http_rows
    ]
    tagged.sort(key=lambda r: r[0])

    if not tagged:
        console.print("\n[dim]No log entries captured.[/dim]")
        return

    console.print("\n[bold cyan]Postgres Queries + S3 Requests[/bold cyan]")
    for ts, kind, data in tagged:
        time_part = ts[11:23]
        if kind == "sql":
            query = data.strip()
            if "\n" in query:
                console.print(f"[dim]{time_part}[/dim] [green]SQL[/green]")
                console.print(Syntax(query, "sql", theme="one-dark", word_wrap=True, padding=(0, 2)))
            else:
                console.print(f"[dim]{time_part}[/dim] [green]SQL[/green]  {query}")
        else:
            method, url, dur, status = data
            path = url.split("?")[0]
            code = status.split("_")[-1] if "_" in status else status
            code_style = "green" if code.startswith("2") else "yellow" if code.startswith("4") else "red"
            console.print(
                f"[dim]{time_part}[/dim] [blue]S3 [/blue]  "
                f"[cyan]{method}[/cyan] {path}  "
                f"[{code_style}]{code}[/{code_style}]  [dim]{dur}ms[/dim]"
            )


def run_maintenance(label, sql, set_sql=None):
    if set_sql:
        duckdb.sql(set_sql)
    t0 = time.perf_counter()
    duckdb.sql(sql)
    elapsed = time.perf_counter() - t0
    return elapsed


if args.maintenance:
    results_table = make_table()
    console.print("\n[bold]MAINTENANCE MODE[/bold]\n")

    bench("baseline")

    t = run_maintenance(
        "tier 0→1",
        "CALL ducklake_merge_adjacent_files('my_ducklake', max_file_size => 1048576)",
        "CALL ducklake_set_option('my_ducklake', 'target_file_size', '5MB')",
    )
    bench("after merge", maintenance=f"merge_adjacent_files(<1MB→5MB) {t:.3f}s")

    t = run_maintenance(
        "tier 1→2",
        "CALL ducklake_merge_adjacent_files('my_ducklake', min_file_size => 1048576, max_file_size => 10485760)",
        "CALL ducklake_set_option('my_ducklake', 'target_file_size', '32MB')",
    )
    bench("after merge", maintenance=f"merge_adjacent_files(1MB–10MB→32MB) {t:.3f}s")

    t = run_maintenance(
        "tier 2→3",
        "CALL ducklake_merge_adjacent_files('my_ducklake', min_file_size => 10485760, max_file_size => 67108864)",
        "CALL ducklake_set_option('my_ducklake', 'target_file_size', '128MB')",
    )
    bench("after merge", maintenance=f"merge_adjacent_files(10MB–64MB→128MB) {t:.3f}s")

    t = run_maintenance(
        "rewrite",
        "CALL ducklake_rewrite_data_files('my_ducklake', 'yellow_tripdata', delete_threshold => 0.0)",
    )
    bench(
        "after rewrite",
        maintenance=f"rewrite_data_files(delete_threshold=0.0) {t:.3f}s",
    )

    t0 = time.perf_counter()
    duckdb.sql("CALL ducklake_expire_snapshots('my_ducklake', older_than => now())")
    duckdb.sql(
        "CALL ducklake_cleanup_old_files('my_ducklake', older_than => now() - INTERVAL '0 seconds')"
    )
    duckdb.sql(
        "CALL ducklake_delete_orphaned_files('my_ducklake', older_than => now() - INTERVAL '0 seconds')"
    )
    t = time.perf_counter() - t0
    bench(
        "after cleanup",
        maintenance=f"expire_snapshots + cleanup_old_files + delete_orphaned_files {t:.3f}s",
    )
    console.print(results_table)
    if args.log:
        print_pg_logs()

elif args.cleanup:
    results_table = make_table()
    if args.dry_run:
        scheduled, orphaned = pending_counts()
        console.print(
            f"[bold]Dry run[/bold]  scheduled: [red]{scheduled}[/red]  orphaned: [red]{orphaned}[/red]"
        )
    else:
        bench("before cleanup")
        t0 = time.perf_counter()
        duckdb.sql("CALL ducklake_expire_snapshots('my_ducklake', older_than => now())")
        duckdb.sql(
            "CALL ducklake_cleanup_old_files('my_ducklake', older_than => now() - INTERVAL '0 seconds')"
        )
        duckdb.sql(
            "CALL ducklake_delete_orphaned_files('my_ducklake', older_than => now() - INTERVAL '0 seconds')"
        )
        t = time.perf_counter() - t0
        bench(
            "after cleanup",
            maintenance=f"expire_snapshots + cleanup_old_files + delete_orphaned_files {t:.3f}s",
        )
        console.print(results_table)
        if args.log:
            print_pg_logs()

elif args.churn:
    results_table = make_table()
    N = 333
    ops = args.churn  # "all", "insert", "update", or "delete"
    bench(f"before churn ({ops})")

    if ops in ("all", "insert"):
        for i in tqdm(range(N), desc="inserts", unit="tx"):
            duckdb.sql(f"""
                INSERT INTO yellow_tripdata
                SELECT * FROM yellow_tripdata WHERE rowid % {N} = {i} LIMIT 120
            """)

    if ops in ("all", "update"):
        for i in tqdm(range(N), desc="updates", unit="tx"):
            duckdb.sql(f"""
                UPDATE yellow_tripdata SET passenger_count = passenger_count
                WHERE rowid IN (
                    SELECT rowid FROM yellow_tripdata
                    WHERE passenger_count IS NOT NULL
                    QUALIFY row_number() OVER (ORDER BY rowid) BETWEEN {i * 120 + 1} AND {(i + 1) * 120}
                )
            """)

    if ops in ("all", "delete"):
        for i in tqdm(range(N), desc="deletes", unit="tx"):
            duckdb.sql(f"""
                DELETE FROM yellow_tripdata
                WHERE rowid IN (
                    SELECT rowid FROM yellow_tripdata
                    QUALIFY row_number() OVER (ORDER BY rowid) BETWEEN {i * 120 + 1} AND {(i + 1) * 120}
                )
            """)

    bench("after churn")
    console.print(results_table)
    if args.log:
        print_pg_logs()

else:
    results_table = make_table()
    bench("select only")
    console.print(results_table)
    if args.log:
        print_pg_logs()
