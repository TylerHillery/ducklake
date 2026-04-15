-- admin.sql
-- One-time setup that must be run as a superuser.
-- Run via SSH: sudo -u postgres psql -f admin.sql

-- 1. Install pg_duckdb
create extension if not exists pg_duckdb schema extensions;

-- 2. Grant the duckdb FDW to the postgres role so it can CREATE SERVER
grant usage on foreign data wrapper duckdb to postgres;

-- 3. Grant file access roles so pg_duckdb allows LocalFileSystem access for postgres.
--    Without these, DuckLake's extension loader cannot find the .duckdb_extension file
--    and ATTACH fails with "File system LocalFileSystem has been disabled by configuration".
grant pg_read_server_files  to postgres;
grant pg_write_server_files to postgres;

select duckdb.install_extension('postgres');
select duckdb.install_extension('ducklake');

-- Verify
select extname, extversion from pg_extension where extname = 'pg_duckdb';
select fdwname, fdwowner::regrole from pg_foreign_data_wrapper where fdwname = 'duckdb';
select * from duckdb.extensions where extension_name = 'ducklake';
