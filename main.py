import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

# Postgres credentials
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Supabase storage credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_URL = os.getenv("BUCKET_URL")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")

duckdb.sql(f"""
DROP SECRET IF EXISTS supabase_storage;
CREATE SECRET supabase_storage (
    TYPE S3,
    KEY_ID '{AWS_ACCESS_KEY_ID}',
    SECRET '{AWS_SECRET_ACCESS_KEY}', 
    ENDPOINT '{ENDPOINT_URL}', 
    REGION '{AWS_REGION}',
    URL_STYLE 'path'
)
""")

duckdb.sql("INSTALL postgres")

duckdb.sql(f"""
ATTACH 
    'ducklake:postgres:dbname={POSTGRES_DATABASE}
    user={POSTGRES_USERNAME} 
    host={POSTGRES_HOST} 
    password={POSTGRES_PASSWORD} 
    port={POSTGRES_PORT}' 
AS my_ducklake (DATA_PATH 's3://data-lake');
USE my_ducklake;
"""
)

duckdb.sql("""
CREATE TABLE nl_train_stations AS
FROM 'https://blobs.duckdb.org/nl_stations.csv';        
""")

duckdb.sql("""
SELECT * FROM nl_train_stations;
""").show()

duckdb.sql("""
UPDATE nl_train_stations SET name_long='Johan Cruijff ArenA' WHERE code = 'ASB';
SELECT name_long FROM nl_train_stations WHERE code = 'ASB';
""").show()

duckdb.sql("""
FROM glob('s3://data-lake/*');         
""")
