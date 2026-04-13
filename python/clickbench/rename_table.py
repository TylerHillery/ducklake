#!/usr/bin/env python3
"""One-off: rename clickbench.main.hits_13gb → hits_14gb"""

import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

load_dotenv(Path(__file__).parent.parent.parent / ".env.staging")

PG = (
    f"ducklake:postgres:host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']} "
    f"dbname={os.environ['POSTGRES_DATABASE']} user={os.environ['POSTGRES_USERNAME']} password={os.environ['POSTGRES_PASSWORD']}"
)
ENDPOINT = os.environ["ENDPOINT_URL"]
KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]
REGION = os.environ["AWS_REGION"]
BUCKET = os.environ["BUCKET_NAME"]

with duckdb.connect() as conn:
    conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        CREATE SECRET s (TYPE S3, KEY_ID '{KEY}', SECRET '{SECRET}',
        ENDPOINT '{ENDPOINT}', REGION '{REGION}', URL_STYLE 'path',
        USE_SSL true, SCOPE 's3://{BUCKET}')
    """)
    conn.execute(f"""
        ATTACH '{PG}' AS clickbench (
            DATA_PATH 's3://{BUCKET}/clickbench/ducklake/',
            METADATA_SCHEMA 'clickbench_ducklake'
        )
    """)
    conn.execute("ALTER TABLE clickbench.main.hits_13gb RENAME TO hits_14gb")
    print("Done: clickbench.main.hits_13gb → clickbench.main.hits_14gb")
