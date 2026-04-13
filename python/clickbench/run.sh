#!/usr/bin/env bash

TRIES=3
cd "$(dirname "$0")"
set -a; source ../../.env.staging; set +a

TABLE_NAME="${TABLE_NAME:-hits_14gb}"
MEMORY_LIMIT_MB="${MEMORY_LIMIT_MB:-0}"
PG_URL="postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE}"

# Build the ATTACH statement once — \$\$ becomes $$ in the string value.
# DuckDB connects back to Postgres via localhost (same host).
ATTACH_SQL="SELECT duckdb.raw_query(\$\$ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=${POSTGRES_DATABASE} user=${POSTGRES_USERNAME} host=localhost password=${POSTGRES_PASSWORD} port=5432' AS clickbench (DATA_PATH 's3://${BUCKET_NAME}/clickbench/ducklake/', METADATA_SCHEMA 'clickbench_ducklake')\$\$);"

while read -r query; do
    # Substitute the table name in the query
    query="${query//clickbench.main.hits/clickbench.main.${TABLE_NAME}}"
    echo "$query"
    (
        echo "$ATTACH_SQL"
        echo '\timing'
        yes "$query" | head -n $TRIES
    ) | psql --no-psqlrc --tuples-only "$PG_URL" 2>&1
done < queries.sql
