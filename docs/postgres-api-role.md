# Least-privilege Postgres role for the API server

The API connects with the database owner by default (`DATABASE_URL` in
`runAddressApiServer.ts`; it prefers `API_DATABASE_URL` when that is set —
see "Switch the API over"). Queries are parameterized, but defense in depth
says a future SQL mistake — or a compromised dependency — running as the
owner could `DROP TABLE`; running as a restricted role it cannot touch
schema, and cannot write pipeline tables the API never writes.

This runbook creates `voteapp_api`: read anything, write only the tables the
API actually writes. Migrations keep running as the owner role (the
`db:migrate` scripts), so DDL stays fully privileged.

## Apply (one time, as the owner role, e.g. via Render's psql shell)

```sql
-- 1. Role. Generate a long random password from URL-safe characters only
--    (letters + digits — it gets embedded in a connection URI, where @ : /
--    # ? % would need percent-encoding). It lives only in the API service's
--    API_DATABASE_URL env var.
CREATE ROLE voteapp_api LOGIN PASSWORD '<generate-a-long-random-password>';

-- 2. Read everything (candidates, elections, finance, districts, ...).
--    Sequence USAGE covers nextval on any serial/identity columns; the
--    schema is UUID-keyed so this is precautionary, and USAGE on a sequence
--    exposes no row data.
GRANT USAGE ON SCHEMA public TO voteapp_api;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO voteapp_api;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO voteapp_api;

-- 3. Write only what the API writes today.
--    Enumerated from the modules wired into runAddressApiServer.ts
--    (auth, /api/me/*, content reports, address-resolve side effects).
GRANT INSERT, UPDATE, DELETE ON
  public.users,                            -- register, profile, account delete
  public.user_auth_tokens,                 -- verify/reset/change-email tokens
  public.content_reports,                  -- report-a-problem insert + delete-time anonymize
  public.user_candidate_follows,
  public.user_districts,
  public.user_ballot_preferences,
  public.user_research_area_preferences,   -- email prefs are columns on users
  public.user_election_choices,
  public.user_push_tokens,
  public.user_push_notification_receipts,  -- cleared on account delete
  public.manual_district_research_requests,-- address resolve enqueues research
  public.staging_items                     -- autoDistrictResearch upsert
TO voteapp_api;

-- Append-only acceptance history and idempotent pick-card shares need narrower
-- grants than the general read/write tables above.
GRANT INSERT ON public.user_terms_acceptances TO voteapp_api;
GRANT INSERT, UPDATE ON public.user_pick_card_shares TO voteapp_api;

-- PostgreSQL row-locking SELECTs (FOR UPDATE/FOR SHARE/etc.) require UPDATE
-- privilege. API queries only row-lock tables already listed as writable;
-- reference/catalog tables such as elections, candidate_elections, and
-- candidates remain SELECT-only.

-- 4. Future tables created by migrations (which run as the owner) are
--    readable automatically; new write tables need an explicit GRANT in
--    their migration.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO voteapp_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE ON SEQUENCES TO voteapp_api;
```

If any listed table does not exist in your schema revision, drop that line
and re-run.

## Switch the API over

Do NOT edit `DATABASE_URL` — render.yaml pins it to the owner connection
string via `fromDatabase`, so a manual edit would be silently reverted by
the next Blueprint sync. The API instead prefers `API_DATABASE_URL` when
set (`runAddressApiServer.ts`), and that variable is deliberately absent
from render.yaml: dashboard-only vars survive Blueprint syncs untouched.

1. On Render, add **`API_DATABASE_URL`** to the **API service** in the
   dashboard: the same URL as `DATABASE_URL` but with
   `voteapp_api:<password>` as the credentials. Leave `DATABASE_URL` itself
   alone everywhere (owner role — used by migrations and workers).
2. Deploy/restart the API service.
3. Verify the API is on the restricted role, then smoke test: register +
   login, load a ballot, follow a candidate, submit a content report,
   delete a test account.

   ```sql
   -- as owner, while the API is serving traffic
   SELECT usename, count(*) FROM pg_stat_activity
   WHERE datname = current_database() GROUP BY usename;
   -- expect the API's connections under voteapp_api
   ```

## If something 500s

A missed grant fails loudly and specifically:
`permission denied for table <name>` in the API logs. Fix is one statement:

```sql
GRANT INSERT, UPDATE, DELETE ON public.<name> TO voteapp_api;
```

Rollback is instant: delete `API_DATABASE_URL` from the dashboard and the
API falls back to `DATABASE_URL` (owner) on the next restart.

## Ongoing rule

When a migration adds a table the **API** must write (not just read), the
migration must include the `GRANT INSERT, UPDATE, DELETE ... TO voteapp_api`
itself. Read-only tables need nothing — the default privileges cover SELECT.

Both the grants and the default privileges above are scoped to schema
`public`. A migration that creates a **new schema** must grant everything the
API needs there explicitly — `GRANT USAGE ON SCHEMA <name>` first, or the API
cannot even read it — and set its own `ALTER DEFAULT PRIVILEGES IN SCHEMA
<name>` for tables added later.
