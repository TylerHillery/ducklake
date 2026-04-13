-- admin.sql
-- One-time setup that must be run as a superuser (e.g. supabase_admin or postgres OS user).
-- Run via SSH: sudo -u postgres psql -f admin.sql

-- 1. Install pg_duckdb
CREATE EXTENSION IF NOT EXISTS pg_duckdb SCHEMA extensions;

-- 2. Grant the duckdb FDW to the postgres role so it can CREATE SERVER
GRANT USAGE ON FOREIGN DATA WRAPPER duckdb TO postgres;

-- 3. Grant file access roles so pg_duckdb allows LocalFileSystem access for postgres.
--    Without these, DuckLake's extension loader cannot find the .duckdb_extension file
--    and ATTACH fails with "File system LocalFileSystem has been disabled by configuration".
GRANT pg_read_server_files  TO postgres;
GRANT pg_write_server_files TO postgres;

-- 4. Enable community extensions and install ducklake.
--    These GUCs are session-local — run them in the same session as the INSTALL.
SET duckdb.allow_community_extensions = true;
SET duckdb.allow_unsigned_extensions  = true;
SELECT duckdb.install_extension('ducklake');

-- Verify
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_duckdb';
SELECT fdwname, fdwowner::regrole FROM pg_foreign_data_wrapper WHERE fdwname = 'duckdb';
SELECT * FROM duckdb.extensions WHERE extension_name = 'ducklake';
