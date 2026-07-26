# Least-privilege Postgres role for the API server

The API currently connects with the database owner (`DATABASE_URL` in
`runAddressApiServer.ts`). Queries are parameterized, but defense in depth
says a future SQL mistake — or a compromised dependency — running as the
owner could `DROP TABLE`; running as a restricted role it cannot touch
schema, and cannot write pipeline tables the API never writes.

This runbook creates `voteapp_api`: read anything, write only the tables the
API actually writes. Migrations keep running as the owner role (the
`db:migrate` scripts), so DDL stays fully privileged.

## Apply (one time, as the owner role, e.g. via Render's psql shell)

```sql
-- 1. Role. Generate a long random password; it lives only in the API
--    service's DATABASE_URL env var.
CREATE ROLE voteapp_api LOGIN PASSWORD '<generate-a-long-random-password>';

-- 2. Read everything (candidates, elections, finance, districts, ...).
GRANT USAGE ON SCHEMA public TO voteapp_api;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO voteapp_api;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO voteapp_api;

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
  public.user_push_tokens,
  public.user_push_notification_receipts,  -- cleared on account delete
  public.manual_district_research_requests,-- address resolve enqueues research
  public.staging_items                     -- autoDistrictResearch upsert
TO voteapp_api;

-- 4. Future tables created by migrations (which run as the owner) are
--    readable automatically; new write tables need an explicit GRANT in
--    their migration.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO voteapp_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO voteapp_api;
```

If any listed table does not exist in your schema revision, drop that line
and re-run.

## Switch the API over

1. On Render, edit the **API service's** `DATABASE_URL` to the same URL with
   `voteapp_api:<password>` as the credentials. Leave the workers' and
   migration `DATABASE_URL` on the owner role.
2. Deploy/restart the API service.
3. Smoke test: register + login, load a ballot, follow a candidate, submit a
   content report, delete a test account.

## If something 500s

A missed grant fails loudly and specifically:
`permission denied for table <name>` in the API logs. Fix is one statement:

```sql
GRANT INSERT, UPDATE, DELETE ON public.<name> TO voteapp_api;
```

Rollback is instant: point `DATABASE_URL` back at the owner credentials.

## Ongoing rule

When a migration adds a table the **API** must write (not just read), the
migration must include the `GRANT INSERT, UPDATE, DELETE ... TO voteapp_api`
itself. Read-only tables need nothing — the default privileges cover SELECT.
