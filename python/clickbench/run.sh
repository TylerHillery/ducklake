#!/usr/bin/env bash

TRIES=3
cd "$(dirname "$0")"
trap 'echo; echo "Interrupted — exiting."; exit 130' INT TERM

# Source .env.staging if present (local dev). On the DB host, set PG_URL directly.
ENV_FILE="$(dirname "$0")/../../.env.staging"
if [ -f "$ENV_FILE" ]; then
    set -a; source "$ENV_FILE"; set +a
fi

TABLE_NAME="${TABLE_NAME:-hits_14gb}"

# PG_URL can be passed in directly (e.g. on the DB host without .env.staging).
# Falls back to constructing from individual POSTGRES_* vars loaded above.
PG_URL="${PG_URL:-postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE}}"

# For DuckLake ATTACH on the DB host, pg_duckdb connects back via localhost.
# Pass ATTACH_DB / ATTACH_USER / ATTACH_PASSWORD / BUCKET_NAME to override.
ATTACH_DB="${ATTACH_DB:-${POSTGRES_DATABASE}}"
ATTACH_USER="${ATTACH_USER:-${POSTGRES_USERNAME}}"
ATTACH_PASSWORD="${ATTACH_PASSWORD:-${POSTGRES_PASSWORD}}"

# Build the ATTACH statement once — \$\$ becomes $$ in the string value.
ATTACH_SQL="SELECT duckdb.raw_query(\$\$ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=${ATTACH_DB} user=${ATTACH_USER} host=localhost password=${ATTACH_PASSWORD} port=5432' AS clickbench (DATA_PATH 's3://${BUCKET_NAME}/clickbench/ducklake/', METADATA_SCHEMA 'clickbench_ducklake')\$\$);"

while read -r query; do
    # Substitute the table name in the query
    query="${query//clickbench.main.hits/clickbench.main.${TABLE_NAME}}"
    echo "$query"
    (
        echo "SET statement_timeout = 0;"
        echo "$ATTACH_SQL"
        echo '\timing'
        yes "$query" | head -n $TRIES
    ) | psql --no-psqlrc --tuples-only "$PG_URL" 2>&1
done < queries.sql
