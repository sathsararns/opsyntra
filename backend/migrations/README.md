# Analytics migrations and external database provisioning

These SQL migrations require PostgreSQL 16+ and an administrator-provisioned
owner login plus reader/writer groups. The local WSL launcher provisions these
automatically. On another PostgreSQL server, have its administrator run the
following in `psql` (once, on a dedicated database server/namespace):

```sql
CREATE ROLE opsyntra_analytics_reader NOLOGIN;
CREATE ROLE opsyntra_analytics_writer NOLOGIN;
CREATE ROLE opsyntra_analytics_owner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE opsyntra_analytics_ingest LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE opsyntra_analytics_api LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
\password opsyntra_analytics_owner
\password opsyntra_analytics_ingest
\password opsyntra_analytics_api
GRANT opsyntra_analytics_writer TO opsyntra_analytics_ingest;
GRANT opsyntra_analytics_reader TO opsyntra_analytics_api;
CREATE DATABASE opsyntra_analytics OWNER opsyntra_analytics_owner;
CREATE DATABASE opsyntra_analytics_test OWNER opsyntra_analytics_owner;
```

For **each** database, connect as administrator and restrict default privileges:

```sql
\connect opsyntra_analytics
REVOKE ALL ON DATABASE opsyntra_analytics FROM PUBLIC;
GRANT CONNECT ON DATABASE opsyntra_analytics TO opsyntra_analytics_owner, opsyntra_analytics_ingest, opsyntra_analytics_api;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

\connect opsyntra_analytics_test
REVOKE ALL ON DATABASE opsyntra_analytics_test FROM PUBLIC;
GRANT CONNECT ON DATABASE opsyntra_analytics_test TO opsyntra_analytics_owner, opsyntra_analytics_ingest, opsyntra_analytics_api;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
```

Set server-side URLs from [`.env.example`](../.env.example), using encrypted
connections for a remote server (`sslmode=verify-full` with a trusted CA).
Never put passwords in committed SQL files or terminal command arguments.
From `backend/`, install `requirements/analytics.txt` then run:

```text
python -m pipelines.analytics_db migrate
python -m pipelines.analytics_check
```

Only the owner connection applies migrations. Reader/writer groups receive
explicit grants from each migration. Add new versioned files for changes;
editing applied SQL fails checksum verification (CRLF/LF differences alone are
normalized). The migration runner locks and
rolls back on failure. Schema version history is not writable by application
roles. Fixtures are kept in the separate test database.

See [storage contracts and metrics](../../docs/ANALYTICS_STORAGE.md) and
[verified results](../reports/analytics_verification.md).
