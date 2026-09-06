# Code improvements backlog — 2026-09-06

Status: tracking. Merged from a full-repo read (Claude, main session) and two
external review rounds (Codex, 2026-09-06). Every item was re-verified against
source in this checkout (commit 39be1bd3f). Claims that did not hold up, or
that proposed more machinery than the bug needs, were cut or narrowed (see
"Dropped or narrowed" at the end).

Ordering = severity, then cost. Tick the box when merged; note the PR.
Regression tests are part of each item, not a separate deliverable. The
Postgres-backed CI job (`.github/workflows/backend.yml`, second job) runs
NAMED spec files — every new DB-backed spec must be added to that job.

Baseline (this checkout): backend typecheck clean, 10,856 tests pass (24
skipped); frontend typecheck clean, lint = 0 errors / 30 warnings, 928 tests
pass; frontend also passes `tsc --strict` with 0 errors. No `@ts-ignore`; one
`TODO` (`stateResourcesValidator.ts:1180`, a column-rename note). Reviewed
API/auth paths use parameterized values; SQL identifiers that are
interpolated are validated at construction (e.g. the finance due-list
builder) — no injection found in the reviewed paths (not a proof for the
whole tree).

---

## P1 — security

### [x] 1. Void outstanding auth tokens when ownership or credentials change; fix lock order — PR #1195

Files: `backend/src/auth/authService.ts`, `backend/src/auth/authTokenStore.ts`

Problem. `user_auth_tokens` rows (`email_verify`, `password_reset`,
`email_change`) are only voided by a new issuance of the SAME purpose
(`issueUserAuthToken`) or by consumption. `session_epoch` bumps revoke
sessions, never tokens. Confirmed paths:

- Pre-registrant takeover. `POST /api/me/email` needs a session but not a
  verified email (`apiServer.ts` ME_EMAIL_PATH). Attacker registers
  victim@x (unverified), logs in, requests an email change to attacker@y →
  `email_change` token in attacker's inbox (24 h). Victim re-registers
  (`createOrRefreshAuthUser` replaces the password and bumps the epoch —
  tokens untouched) and verifies. Attacker opens the old link:
  `verifyEmailChange` (`authService.ts:1204`) swaps `users.email` to
  attacker@y with no ownership check → forgot-password to attacker@y →
  attacker owns the victim's account.
- `changePassword` / `resetPassword` do not void `password_reset` or
  `email_change`; `verifyEmail` / `verifyEmailChange` do not void the other
  purposes; Google takeover (`resolveUserOnce`) voids nothing.
- Lock order. All three consumers — `verifyEmail` (`:836`), `resetPassword`,
  `verifyEmailChange` (`:1204`) — consume the token row FIRST, then update
  the user row. Issuance and the fix below lock the user row first. Adding
  a "void tokens" UPDATE to the user-first paths without fixing the
  consumers creates an AB/BA deadlock between e.g. re-registration and a
  concurrent verify.

Fix (small, no new columns, no token epoch):

1. `voidUserAuthTokens(client, userId, purposes?)` in `authTokenStore.ts`:
   one `UPDATE … SET consumed_at = now() WHERE user_id = $1 AND consumed_at
   IS NULL [AND purpose = ANY($2)]`.
2. Rule: every ownership/credential event voids ALL outstanding tokens of
   the OTHER purposes, inside the existing transaction, after the user row
   is locked (`FOR UPDATE`):
   - `createOrRefreshAuthUser` existing-unverified branch → void
     `password_reset`, `email_change`.
   - `verifyEmail` → void `password_reset`, `email_change`.
   - `resetPassword`, `changePassword` → void `email_change` (+
     `password_reset` for `changePassword`).
   - `verifyEmailChange` → void `password_reset`, `email_verify` (an old
     mailbox's verify link must not stay actionable after the address
     changes; today its only leftover effect is an epoch bump).
   - Google takeover → void all purposes.
3. Consumers: look the token up by hash WITHOUT consuming (new
   `findUserAuthToken`), `SELECT … FOR UPDATE` the user row, then run the
   existing atomic consume (which re-checks hash/purpose/expiry/consumed_at
   after the wait). Same order everywhere.
4. Keep `POST /api/me/email` reachable for unverified users (typo-fix flow).

Tests (Postgres-backed, wire into the DB CI job): takeover sequence above →
old link 400; old reset link after `changePassword` → 400; Google takeover
voids all; legitimate unverified typo-fix still works; concurrent
re-register vs verify does not deadlock (two clients, interleaved).

---

## P2 — correctness / data integrity

### [x] 2. Manual finance-link protection: missing or incomplete in 24 bespoke writers — PR #1196

Files: `backend/src/pipeline/<state>Finance/<state>FinanceWriter.ts`;
reference guard in `pipeline/finance/standardStateFinanceSnapshotWriter.ts:396`
("M" in `docs/finance-module-capability-matrix.md`).

Problem, verified per writer:

- No guard at all (20): alaska, california, colorado, connecticut, florida,
  hawaii, kentucky, louisiana, massachusetts, nebraska, newJersey,
  newMexico, newYork, oklahoma, tennessee, utah, vermont, virginia,
  washington, wisconsin — `ON CONFLICT … DO UPDATE SET link_status =
  EXCLUDED.link_status, link_source = EXCLUDED.link_source`. The
  "no auto-linker" exemption was wrong on two counts: the ORDINARY sync
  re-upserts the link with the bulk source every run (utah
  `utahCandidateFinanceSync.ts:135`), erasing `manual` provenance even for
  the same identity; and vermont (auto-link inside
  `vermontCandidateFinanceBatchSync.ts:242`) and newJersey (due query
  starts from `candidate_elections`, LEFT JOINs active links, resolves via
  ELEC search) DO auto-link. Auto-link selects "no ACTIVE link" →
  operator's `inactive/manual` rejection is resurrected as `active/<bulk>`.
- Reactivates an operator-disabled manual row (2): indiana
  (`indianaFinanceWriter.ts:192` guards `link_source` only), newYorkCity
  (`newYorkCityFinanceWriter.ts:131` sets `link_status = 'active'`
  unconditionally; its due query `…BatchSync.ts:121` LEFT JOINs active
  links only, so the rejected identity is re-selected).
- Pre-INSERT "retire other identities" ignores manual (2): newYork
  (`newYorkFinanceWriter.ts:213`) and louisiana
  (`louisianaFinanceWriter.ts:281`) `UPDATE … SET link_status='inactive'
  WHERE … <> identity AND link_status='active'` — an automatic identity B
  disables an operator's active/manual identity A. illinois, michigan,
  pennsylvania, newYorkCity already exclude manual here; a CASE in the
  later ON CONFLICT cannot protect this other row.
- Factory + guarded bespoke writers (illinois, michigan, minnesota,
  pennsylvania) block DELIBERATE manual updates: `CASE WHEN links.link_source
  = 'manual' THEN links.link_status` ignores `EXCLUDED` even when the
  incoming write is itself manual, so a manual active→inactive write via
  the writer silently stays active. Latent today (no script writes
  `linkStatus: "inactive"` through a writer; operators use SQL) — fix while
  in the file.

Fix — one rule, applied per writer (no factory migration):

- Automatic writes (`link_source <> 'manual'`) hitting a manual row: keep
  `link_status` and `link_source`; may refresh metadata columns
  (`election_year`, names, office, district, `source_url`,
  `last_verified_at`) — same as factory M today. Manual writes
  (`EXCLUDED.link_source = 'manual'`) apply in full. SQL:
  `CASE WHEN links.link_source = 'manual' AND EXCLUDED.link_source <> 'manual'
  THEN links.link_status ELSE EXCLUDED.link_status END` (same for
  `link_source`).
- Every pre-INSERT retire step adds `AND link_source IS DISTINCT FROM
  'manual'` unless the incoming write is manual (NYC pattern).
- Auto-link and sync callers: a write that was blocked/kept must not be
  counted as "linked" — return the existing id and a `protected: true`
  flag, or throw as the factory does; pick one and use it in the count.

Tests: table-driven over all 24 writers with real Postgres (wire into the
DB CI job): inactive/manual + automatic same identity → stays
inactive/manual; manual→manual status change both ways applies; automatic
different identity cannot disable an active manual link; metadata refresh
matches the rule; blocked write is not reported as linked.

### [x] 3. `TypeError`/`SyntaxError` are mapped to HTTP 400 — internal bugs hide as client errors — PR #1197

File: `backend/src/api/apiErrors.ts:30-35`

Problem. 216 `throw new TypeError(...)` sites are used as validation
errors, so `mapErrorToResponse` returns 400 + `error.message` for ANY
`TypeError`; likewise ANY `SyntaxError` → 400 `invalid_json`. A runtime
`TypeError` (`undefined.foo` in ballotLookup) or a `JSON.parse` failure on
a stored column becomes a 400 with the internal message in the body, never
logged, never captured (the middleware captures mapped 500s only).
Malformed request bodies are already handled separately via
`entity.parse.failed` (`apiServer.ts:2635`).

Fix:
- `class RequestValidationError extends Error` in a small,
  dependency-free module (NOT `apiValidation.ts` — auth stores and
  `pipeline/users` must not import the API layer): e.g.
  `backend/src/utils/requestValidationError.ts`. Map it to 400
  `invalid_request`; drop the `TypeError` and `SyntaxError` branches.
- Classify sites, do not replace mechanically. Client errors →
  `RequestValidationError`: all of `apiValidation.ts` (132), request-shaped
  checks in `authService.ts`, `pipeline/users/*`, `membership`,
  `authApiRateLimiter.ts`, `usage/events.ts`. Internal invariants stay
  plain `Error` (→ 500 + capture): unsupported token purpose / bad hash in
  `authTokenStore.ts`, bad configured TTL/epoch in `authSessionStore.ts`,
  bad internal timestamps, mailer template checks in `authMailer.ts`.
- Keep the explicit body-parser `invalid_json` path; an unclassified
  internal `SyntaxError` becomes a captured 500.

Tests: API-level — malformed JSON body still 400 `invalid_json`; a bad
field still 400 `invalid_request` with the same message; an injected
internal `TypeError` and `SyntaxError` from a handler → 500 `internal_error`
with a scrubbed body and exactly one capture.

### [x] 4. API pool has no deadlines or error handler; auth flows hold a pool client during SES sends — PR #1198

Files: `backend/src/scripts/runAddressApiServer.ts:221`, `:990`;
`backend/src/auth/authService.ts:793`, `:823`

Problem.
- `new Pool({ connectionString })` → pg defaults: `connectionTimeoutMillis
  0` (wait forever), no server-side `statement_timeout`. A stuck Postgres
  pins every request. (pg's default `max` is already 10 — not a change.)
- No `pool.on("error")`. An idle client's connection error is emitted on
  the pool; unhandled, it reaches `process.on("uncaughtException")` at
  `:990`, which exits the process. pg already discards the broken client;
  the API should log/capture and continue.
- `register` / `resendVerification` `await mailer.send…` INSIDE the
  try/finally that owns the pool client, after `COMMIT`: a slow SES call
  holds a pool slot; a mailer throw runs a pointless `ROLLBACK` on a
  committed transaction and returns 500 for an account that exists.
  `forgotPassword` already sends after `finally`.

Fix:
- Pool: `connectionTimeoutMillis` (bounded acquisition) +
  `statement_timeout` (server-side cancel) — values from measured API p99,
  not copied from scripts. If `query_timeout` is set at all it must be
  LARGER than `statement_timeout`: pg's client-side read timeout errors
  without sending a cancel, so the server deadline must fire first.
- `pool.on("error", (err) => captureError(err, { source: "pg-pool" }))`.
- Move both mail sends after `finally`. Decide the post-commit mail-failure
  UX explicitly: recommended — capture the error and still return success
  (the account exists; the UI's "resend verification" covers delivery),
  rather than 500 on a committed registration.

Not in scope: HTTP keep-alive tuning. Node already sets
`keepAliveTimeout=5000`, `headersTimeout=60000`, `requestTimeout=300000`;
the earlier "65/66 s vs proxy idle" claim has no verified Render proxy
value behind it and no observed 502s. Measure before touching.

Tests: exhausted pool / blocked query fail within bounds and the pool stays
usable; deferred mail send observes the client already released; an
emitted pool error is captured and the process stays up.

Note: 242 `new Pool(` sites repo-wide; 4 set timeouts. A `createPool(kind)`
helper is welcome when touching them — not a sweep now.

### [x] 5. `logoutAll` epoch bump and push-token revoke are not one transaction — PR #1199

File: `backend/src/auth/authService.ts:1393`

Problem. Epoch bump via `options.db.query`, then `revokeAllUserPushTokens`
separately. If the second fails, the caller's session is already dead
(retry → 401) and push tokens stay active — a device the user meant to
sign out keeps receiving personalized pushes.

Fix: check out ONE client; `BEGIN` → epoch UPDATE →
`revokeAllUserPushTokens(client, …)` → `COMMIT` → release; then the Redis
sweep best-effort (as now). `pool.query` between `BEGIN`/`COMMIT` is not a
transaction. Tests: injected push-UPDATE failure rolls back both; success
changes both; Redis failure after commit still returns success; existing
API cookie-clearing test unchanged.

### [x] 6. Cross-tab account switch leaves the previous account's private queries cached — PR #1200

Files: `packages/api-client/src/useMe.ts` (only)

Problem. Private keys are account-independent (`["me","districts"]`,
`["me","election-choices"]`, `["me","follows"]`, `["me","ballot"]`,
`["me","research-area-preferences"]`, …; 79 key usages across
frontend/mobile). Purge runs only in this tab's login/logout callbacks.
Another tab logs out and in as B; this tab's `/api/me` refetch returns B
while the other entries keep serving A's picks/districts. `staleTime` does
not expire data, so there is no 60 s bound; disabled hooks keep returning
cached `query.data` too (`useMyAccountDistricts.ts:38`). Shared-browser
privacy issue, web only in practice (mobile has no second tab).

Fix (no key changes, no new hook): purge INSIDE `useMe`'s `queryFn`,
before the new identity reaches the cache — compare
`queryClient.getQueryData(["me"])?.email` with the response (defined →
different, or defined → 401/null) and call `purgeAccountScopedQueries`
first. Because the cache update for `["me"]` happens after the purge, no
render ever sees B's identity with A's data (a `useEffect` purge would
paint one frame of B + A). Keep the existing login/logout purges. `Me` has
no `id`; email is the only identity field exposed — an email change on the
same account causes one harmless refetch.

Residual (accepted): a mutation started by A that completes after the
switch can `setQueryData` under a static key. Rare; not worth a boundary.

Tests (hook tests, existing runner): A→B, A→null, verified→unverified;
assert render history never contains B's identity with A's districts.

---

## P3 — correctness, cheap

### [x] 7. Bespoke finance writers run `BEGIN/COMMIT` on a caller-supplied client — PR #1201

Files: 25 `with<State>FinanceTransaction` helpers in
`pipeline/*Finance/*Writer.ts`.

Problem (corrected after reading every helper — the earlier "16 writers
commit the caller's transaction" claim was wrong):
- 15 writers already reject a `PoolClient` (alaska, florida, hawaii,
  kentucky, louisiana, massachusetts, newJersey, newYork, pennsylvania,
  vermont, washington, wisconsin, illinois, michigan, minnesota).
- newMexico detects "pool" as `connect && !release`; a real `PoolClient`
  has `release`, so it takes the direct branch and issues `BEGIN/COMMIT`
  on the caller's client — the only corrupting case.
- 9 writers (connecticut, indiana, nebraska, oklahoma, utah, california,
  colorado, tennessee, virginia) detect on `connect` only; a `PoolClient`
  also has `connect`, so they call it and throw "Client has already been
  connected" — loud, not corrupting.
No current production caller passes a client — latent contract hazard.

Fix (shipped): shared `assertSnapshotDbIsNotPoolClient` runs first in the
ten helpers and fails closed on any `release`-bearing object before any
statement. Query-only test doubles keep their behaviour (as illinois/
michigan/minnesota already do); link-upsert helpers still accept a
transaction client. No SAVEPOINT support. Postgres-backed New Mexico spec
proves a caller's open transaction survives the rejection.

### [x] 8. Finance sync workers never capture degraded or failed runs — PR #1202

Files: 42 `backend/src/scripts/run*FinanceSyncSchedulerWorker.ts` (all
near-identical to `runNewMexicoCandidateFinanceSyncSchedulerWorker.ts`);
`backend/src/observability/sentry.ts:86` (`captureError`).

Problem. None of the 42 finance worker entrypoints uses
`runSchedulerWorker` (only the 3 notification workers do) and none
initializes Sentry or captures anything: `completed` logs `failed=N`,
`failed`/`error` log to stderr. A run that fails every candidate is
invisible. `failedCandidateCount` alone is incomplete: vermont has
`autoLinkFailedCount`; newMexico can skip an unavailable outside-spending
artifact and report zero failures.

Fix: one shared `runFinanceSchedulerWorker({ label, isEnabled,
createWorker })` in `scheduler/` that owns the ready/active/completed/
failed/error handlers and the bounded shutdown (exactly what the 42 files
repeat), and captures: `failed`/`error` events, and `completed` results
where `failedCandidateCount > 0 || autoLinkFailedCount > 0 ||
partialSourceFailure`. Migrate scripts by replacing their body; preserve
each feature-flag gate. Tests: the runner with a fake worker — success,
degraded-completed, auto-link-only failure, failed event.

Retry note: the standard due list (`standardStateFinanceDueListQuery.ts:214`)
orders `last_synced_at ASC NULLS FIRST … LIMIT`, and failed rows keep a
stale/NULL `last_synced_at`, so they ARE retried next run — but if
persistent failures fill `maxCandidates`, healthy rows behind them starve.
Not observed in production; log `totalDueRows`/attempted/failed per run
first, add bounded deferral only if a queue shows it. Never advance
`last_synced_at` for failed rows. NYC already has attempt tracking; leave
it.

### [x] 9. Small fixes (one PR) — PR #1203

- `frontend/tsconfig.app.json`: add `"strict": true` (already passes with
  0 errors; the flag locks it in).
- `backend/src/pipeline/address/ballotLookup.ts:96`: delete the empty
  `import {} from "../../config/featureFlags.js"` (module has no
  side-effect initialization).
- `backend/src/api/inMemoryRateLimiter.ts:63`: on a hit, `delete` then
  `set` the bucket (LRU order) as `authApiRateLimiter.ts:125` does; update
  the "insertion order is sufficient" comment; add a cap-pressure test.
- `backend/src/api/membership/membershipService.ts:1062`: after the customer
  lookup, subscription/payments/total are independent → `Promise.all`.
  Latency only; the later amount-change read stays behind the subscription.

---

## P4 — structure (no bug; pays off over time)

### [ ] 10. Tests mock `db.query` by call order, which dictates production query order

2,111 `mockResolvedValueOnce` across 211 backend test files; source
comments preserve mock slots ("issued last so ordered test mocks keep their
slots", `ballotLookup.ts:1134` etc.), so independent loads run
sequentially.

Fix: a test fake that dispatches on SQL text with NARROW patterns or named
fixtures and rejects unexpected/ambiguous queries (broad `/FROM districts/`
matches would hide duplicate queries); keep call-count/parameter asserts.
Migrate `ballotLookup.test.ts` first, then parallelize only the loads that
are truly independent (candidate-dependent reads stay behind candidate
loading; measure tags behind measure ids). Profile per scenario and
compare latency AND pool contention under concurrent requests before
committing to it — no gain has been benchmarked yet.

### [ ] 11. `apiServer.ts` dispatcher (2,729 lines)

Route knowledge is triplicated (`isKnownApiPath`, JSON-body path list,
dispatch if-chain); the dispatch tail implicitly handles address resolve.
No advertised route falls through today.

Fix, smallest first: make address resolve an explicit branch and give the
dispatch tail a controlled no-match response; add a route-contract test
matrix (preflight/unknown before rate limits, limits before parsing, JSON
content-type as CSRF guard, raw Stripe bytes, unsubscribe's per-method
bodies, search before loose detail prefixes, session vs verified). Then a
descriptor table and route-group files, only if it simplifies. "Existing
tests pass" is necessary, not sufficient — add the matrix cases first.

### [ ] 12. Scheduler and env-reader duplication

64 files in `backend/src/scheduler/`; 5 states use
`createStateCandidateFinanceSyncScheduler`, the rest hand-roll the same
shape with real differences (raw cache dir, `includeOutside`, auto-link
options, master vs sync flag, recurrence removal, forced jobs, pass caps,
disabled-result shapes). Migrate demonstrably matching states first with
contract tests pinning queue names, scheduler ids, cron/timezone, job ids,
gating, input propagation and shutdown; add factory options only for
repeated needs. `readBooleanEnv` ×4 / `readPositiveIntegerEnv` ×13 differ
beyond `y`/`n` (`config/env.ts:106` uses `parseInt`, accepting `1.5` and
`10ms`; others validate the whole string) → one `config/envReaders.ts`
with a stated grammar; tightening accepted values is a deliberate change,
not a cleanup.

### [ ] 13. 466 npm scripts in `backend/package.json` — DEFERRED

A `src/cli.ts` dispatcher would not remove the per-invocation `tsx` boot,
and would add routing/help/compat work across hundreds of operational
commands still cited by runbooks and deploy configs. First: grouped help or
a script index; prototype a dispatcher for one command family only if the
UX is actually causing friction.

---

## Dropped or narrowed from the Codex reviews

- "Bind tokens to a credential version / token epoch" → in-transaction
  voiding under a common lock order is enough (item 1).
- "Account cache identifier in every private key + central account
  boundary that gates private rendering" → 79 key usages; purge inside the
  identity `queryFn` gives the strict no-leak render property without
  either (item 6).
- "Schedule bounded retries for failed sync items" → already retried by
  the next due-list run; add reporting and per-run counts first, deferral
  only if starvation is observed (item 8).
- "SAVEPOINT for nested rollback" → Pool-only signature (item 7).
- "Browser account-flow smoke tests + native lifecycle CI jobs" →
  DB-backed and hook-level specs inside existing jobs, one per fixed item.
- "Share/validate backend↔client DTOs" → no defect found; not scheduled.
- "65/66 s keep-alive vs proxy idle" → unverified diagnosis; Node has
  defaults; measure first (item 4).
- "`STATE_FINANCE_LOOKUP_ADAPTERS[].state` is never read" → wrong; it feeds
  `reportFinanceSummarySourceFailure` (`ballotLookup.ts:1151`). Removed.
- "`max: 10` on the API pool" → already pg's default; removed.
- "Add degraded-run capture to `runSchedulerWorker`" → finance workers do
  not use it; replaced by the shared finance runner (item 8).
- Codex evidence scripts live in a separate worktree
  (`~/.codex/worktrees/a438/…/scratch/code-review-2026-09-06/`) and assert
  that the bugs are PRESENT; they are not in this repo and must not be
  copied into CI unchanged. Each finding above was confirmed by reading
  this checkout's source.
