# Plan: highest-value gaps, September 2026

Status: planned 2026-09-06, revised the same day after a second review;
nothing built. Sources: a codebase gap survey plus an outside code review.
Every claim below was re-verified against this checkout and the local
database unless marked "unverified". Ranked by value divided by risk. Each
code item is its own PR. No item needs a schema migration.

Items that write or delete data: 1 (prune deletes old notification events,
reminders send email), 3 (saved picks), 6 (copies production data), 7
(writes resolutions). Every one has a dry-run or a scratch-copy step.

## 1. Turn the notification workers on (operator task plus two small fixes)

Verified:
- `render.yaml:368-436` has `voteapp-notification-workers` and
  `voteapp-notifications-prune` commented out "until billing is set up".
- Billing exists since 2026-08-08 (`voteapp-db` on `basic-1gb`, both web
  services on `starter` since 2026-08-21, `docs/deploy-render.md:232-250`).
- SES production mode was confirmed in the console on 2026-07-10 (memory);
  not re-checked this session.
- Settings toggles (`frontend/src/components/EmailPreferenceToggles.tsx`)
  are live in prod with no "not available yet" gating.
- The prune script is dry-run by default (`pruneNotificationEvents.ts:90`);
  the commented cron command has no `--live`, so uncommenting it as-is
  schedules a nightly no-op.
- Reminders: the sender emails first, then records the send
  (`sendElectionReminders.ts:227-240`). The advisory lock stops overlapping
  runs; a crash between send and record can still double-send one user.
  Accept that; do not build a delivery framework.
- The reminder job fails itself on per-user failures so BullMQ retries
  (`electionReminderScheduler.ts:165-178`). Digest and new-election jobs
  complete with a `failures` list and rely on their backlog persisting to
  the next run. The worker only reports `failed`/`error` events to Sentry
  (`runNotificationSchedulerWorkers.ts:65-71`), so a completed-with-failures
  digest is invisible.
- Election-result emails are NOT part of the worker. They are drained by
  hand with `npm run notifications:election-results -- --live` after each
  results sweep (skill step 7). The digest toggle copy promises them.
- The reminder dry run only looks at tomorrow's elections (no `--date`
  flag), so it cannot preview November's recipient count.

Steps:
1. Code, one small PR:
   a. `runNotificationSchedulerWorkers.ts`: in the `completed` handler, if
      `result.failures?.length > 0`, `captureError` with the count and job
      name. No retry logic.
   b. `pruneNotificationEvents.ts`: `initSentryFromEnv()` at start;
      `main().catch` does `captureError` then `flushSentry` before setting
      the exit code.
   c. `render.yaml`: uncomment the worker block and the prune cron; change
      the prune command to `npm run notifications:prune -- --live`; add
      `CANDIDATE_FOLLOW_DIGEST_DAILY_CRON=0 10 1 * *` and
      `CANDIDATE_FOLLOW_DIGEST_DAILY_TZ=America/New_York` to the worker.
      Keep the SF finance cron commented.
2. User: merge, sync the blueprint (manual Approve), watch the worker's first
   boot for the three "upserted" lines.
3. User, before Nov 2: count opt-ins with one read-only query
   (`users.email_election_reminders` where verified and not deleted) so
   the first live send has an expected number. On Nov 1 run the reminder
   script dry-run once to see the real recipient list for Nov 2.
4. Add to `docs/deploy-checklist.md`: results emails stay manual
   (`notifications:election-results -- --live` after each sweep).

Cost: `starter` worker about $7/month, cron about $1/month minimum.

## 2. Show the "Register to vote" link (design approved, build on request)

Verified:
- `voter_registration_url` is stored (`backend/src/types/stateResource.ts:29`)
  and nonempty for all 51 local state rows, but not returned by
  `/api/state-resources` (`backend/src/api/stateVotingResources.ts`), not in
  `packages/api-client/src/types.ts`, and not rendered in
  `frontend/src/components/HowToVoteControl.tsx`.
- The click event value is validated server-side: `how_to_vote_link` values
  are `mail` and `polling` only (`backend/src/usage/events.ts:165-166`).

User decision 2026-09-06: one new block at the top of the panel, above
"Vote by mail". Heading text exactly `Register to vote`, same class as the
other headings. No tint, no badge, no "first" wording. Link row reuses
`OfficialLink` with `kind: "registration"`.

Steps: expose the field in the API serializer; add it to the api-client type
as optional (an older SSR build can meet a newer API for a few minutes and
vice versa; render the block only when the field is a nonempty string); add
`"registration"` to the events allowlist; add the block plus tests for the
API field and the UI. Then check the mobile parity list before porting.

## 3. Let voters say "No" on judicial retention

Verified:
- Retention races are `race_type = 'office'` with the judge as the single
  candidate. `BallotPreview.tsx:122` says "the app has no 'vote no'
  mechanic".
- `userElectionChoices.ts:479` rejects `measure_position` on office races.
- Scope: 76 upcoming local retention races across 17 districts (UT 19,
  AZ 18, IN 14, NM 12, KS 5, CO 3, ID 2, WY 2, FL 1). Zero existing
  candidate picks on retention races locally; the outside review reports
  zero in production too.
- No migration: `user_election_choices` already allows `candidate_id NULL`
  + `measure_position`. But the two unique indexes are partial and
  independent, so a candidate row and a measure row for the same election
  can coexist. The measure insert (`userElectionChoices.ts:503-512`) does
  not delete candidate rows and the candidate insert does not delete
  measure rows.
- Guest draft `setDraftMeasureChoice` hardcodes `race_type: "ballot_measure"`
  (`ballotDraft.ts:364`). Retention rows must stay `office`.
- Candidate detail pages also offer pick controls (`CandidatePage.tsx`).
- Shared pick cards print "Yes/No on this measure"
  (`PublicPickCardPage.tsx:189`).
- Auto-pick branches on `race_type` for computation, persistence, and copy
  (`packages/api-client/src/autoPick.ts:90`).
- `MeasureChoiceButtons` exists (`ElectionChoiceControls.tsx:465`);
  `isJudicialRetentionTitle` exists (`electionPartisanshipPolicy.ts:325`);
  the frontend copy is `isRetentionTitle` (`BallotPreview.tsx:43`).

Design: a retention race is answered with `measure_position` only. "Yes" =
keep the judge, "No" = remove. Manual Yes/No ships first. Auto-pick leaves
retention races open (no answer, with the existing "no answer" copy);
whether an issue match should ever mean "retain" is a separate decision.

Steps:
1. Shared: move `isRetentionTitle` into `packages/api-client` so web,
   mobile, and the backend copy agree on one regex. Add `is_retention` to
   the election payload only if the client cannot get the title; today it
   can, so probably not.
2. Backend `userElectionChoices.ts`: when the election is an office race
   that passes `isJudicialRetentionTitle`:
   - accept `measure_position` and, in the same transaction, delete any
     candidate rows for that user+election before the insert;
   - accept a `candidate_id` pick from older clients and guest-draft
     sync, but store it as `measure_position = 'yes'` (that is what the
     preview already shows for it), deleting any candidate rows. Never
     reject old clients.
   - `measure_position: null` clears both kinds.
   Tests for all three, plus one proving a candidate row and a measure row
   cannot both survive a write.
3. Auto-pick: retention races return the "no answer" outcome and write
   nothing. Test.
4. Web: `ElectionCard`, `ElectionPage`, `DraftPage`, `SavedBallotPage`,
   `CandidatePage` show `MeasureChoiceButtons` for retention races and hide
   the candidate pick button. `BallotPreview` reads `measure_position`
   directly. `PublicPickCardPage` says "Yes/No on retaining <judge>" for
   these. Guest draft: `setDraftMeasureChoice` takes the race type from the
   caller instead of hardcoding it.
5. Mobile: same swap in `ElectionChoiceControls.tsx` / `ElectionCard.tsx`.
6. Before deploy: re-run the count of candidate picks on retention races in
   prod. If still zero, nothing to convert. If not, step 2's translation
   handles reads, and a one-off UPDATE (dry-run first) normalises the rows.

## 4. Add a real `/api/healthz`

Verified:
- `render.yaml:45` points `healthCheckPath` at `/api/research-areas`, a
  public, rate-limited, full-payload endpoint (`apiServer.ts:656`). The
  comment wants a DB-backed check; that part is right.
- Render's probe deadline is 5 s; it pulls traffic after sustained failures
  and restarts after about 60 s. The API pool allows 10 s to acquire a
  connection and 30 s per statement (`runAddressApiServer.ts:203-210`), so a
  slow probe can exceed the deadline and count as a failure.

Steps: one route, `SELECT 1` on the existing pool raced against a 3 s
deadline, `{"ok":true}` 200 or `{"ok":false}` 503, `cache-control:
no-store`, exempt from every rate limiter and from CORS rejection logging.
Point `healthCheckPath` at it. Tests: 200 with a live pool; 503 when the
query throws; 503 when the query hangs past the deadline.

## 5. Revive the Playwright suite, then one deterministic journey in CI

Verified by running it on 2026-09-06: all three specs fail.
- `voter-loop.spec.ts:15` expects an h1 named exactly "Elections"; the page
  renders "My elections:" (`BallotPage.tsx:215`).
- `account-loop.spec.ts:14` fills `getByLabel(/First name/)`; the label is
  now "First Name (optional)" (`RegisterPage.tsx:201`, case differs). It
  also never fills the required "Confirm password" field
  (`RegisterPage.tsx:239`), so the fill times out.
- `personalization.spec.ts` looks for UI text that changed.
- The local DB holds 20 orphan `e2e-*@example.com` users from 2026-07-09
  (cleanup only runs when the spec gets that far).

Steps:
1. Fix the three specs against the current UI; keep them data-dependent
   (skip when the local DB has no upcoming elections). Make the account
   spec's cleanup run in `afterEach` with its own short timeout.
2. Delete the 20 orphan local e2e users by hand (local DB only).
3. Follow-up PR, concrete scope: one seeded fixture (one district, one
   election, two candidates) loaded by a small script, and one CI job with
   its own Postgres service (GitHub Actions services are per job; the
   `migrate` job's instance cannot be shared) running the voter journey:
   ballot → election → candidate → pick → reload. No fixture framework.

## 6. Rehearse a database restore and write the runbook

Verified:
- 282 forward-only migrations. That is normal, not a gap; do not add
  rollback scripts.
- The only recovery mention is `docs/deploy-render.md:240` (Render
  point-in-time recovery, 3-day window). No documented rehearsal exists.
- Local Postgres is 16.14. The outside review reports production on
  Postgres 18 (unverified here). Check the prod major version first and
  use matching `pg_dump`/`pg_restore` binaries and the `vector` extension.

Steps:
1. Render PITR into a NEW database (never over the live one). Validate:
   migrations table at the expected head, row counts on `users`,
   `elections`, `candidate_records`, and one finance link table, the
   least-privilege API role recreated (`docs/postgres-api-role.md`), one
   API instance pointed at it with mailers, Stripe webhooks, and workers
   off. Delete the copy.
2. Second path, later: `pg_dump` of prod into a scratch database with the
   matching major version; same checks.
3. Write `docs/restore-runbook.md`: exact commands, timings, what
   "restored correctly" means, how to switch a service's `DATABASE_URL`,
   what must stay disabled during validation. Link it from
   `docs/deploy-checklist.md`.

This is an operational rehearsal with a doc, not a docs-only task. It
copies production data; keep the copy short-lived.

## 7. Content reports: tiny CLI plus a bounded operator summary

Verified:
- `contentReports.ts` only inserts and counts. Migration 155 already has
  `status`, `claimed_by`, `resolution`, `finished_at`, and a constraint
  mapping resolutions to terminal status: `fixed`, `no_change_needed`,
  `duplicate` → `resolved`; `unverifiable`, `spam` → `dismissed`.
- Nothing tells anyone a report arrived.
- Local: 11 open, oldest 2026-07-19. The outside review reports 15 open in
  production (unverified here).
- `authMailer.ts` exposes only auth-message methods; the SES transport used
  by `memberNewsletterMailer.ts` is the right thing to copy for a new
  message type.

Scope: one operator, low volume. No claim, ownership, lease, overdue view,
or admin UI.

Steps:
1. `npm run content-reports -- list [--status new|investigating]` (oldest
   first, entity label, message preview) and
   `-- resolve --id <uuid> --resolution fixed|no_change_needed|unverifiable|duplicate|spam --summary <text>`.
   `resolve` derives the terminal status from the constraint mapping and
   refuses to touch a row that is already `resolved`/`dismissed`. It takes
   `DATABASE_URL` explicitly (the real backlog is in prod).
2. `-- summary`: if any report is open, send ONE plain-text email to
   `contact@` listing entity type, entity id, and age. Never include the
   reporter's message or URL (attacker-controlled; mail clients auto-link
   URLs). Run it from the prune cron by chaining the command:
   `npm run notifications:prune -- --live && npm run content-reports -- summary`.
   No per-report email, no new cron.

## 8. Three small SEO correctness fixes (one PR)

Verified:
- The splat route has no loader, so unknown URLs return HTTP 200 with the
  not-found page. Google can treat these as soft 404s.
- `HomePage.tsx`, `BallotPage.tsx`, `DraftPage.tsx` export no `meta`; their
  server-rendered HTML carries the root's generic title. Client-side
  `useDocumentTitle` fixes the tab title after hydration, not the HTML.
- No `<link rel="canonical">` anywhere.
- The root error boundary (`RouteError.tsx`) renders "Something went wrong"
  and reports to Sentry for any thrown response, so throwing a 404 from
  the splat loader would be wrong.

Steps: the splat route gets a loader that returns
`data(null, { status: 404 })` and keeps rendering `NotFoundPage`; add
`meta` exports to the three pages via `pageMeta`; add `canonical` to
`pageMeta` for public content routes (home, mission, support, legal pages,
election and candidate detail). Do not add canonical to `/ballot` or
`/draft`: whether personalised and guest ballots should be indexed at all
is a separate decision.

## Order

1 → 2 (on your go) → 7 → 4 → 5 steps 1-2 → 3 → 8 → 5 step 3 → 6
(6 needs a quiet hour and prod dashboard access; schedule it separately).

## Deferred, with reasons

- Redis-backed rate limiter: `voteapp-api` runs one instance, so the
  in-memory limiter is only wrong across restarts. Do it with the second
  instance.
- Spanish / i18n: real product gap for §203 jurisdictions, but a large
  cross-cutting change with a content plan. Own plan.
- Structured logging, metrics, tracing: not at this scale. Sentry covers
  errors, with item 1a closing the completed-with-failures hole.
- Dependabot / `npm audit` in CI: two separate decisions (scanning vs
  automatic update PRs); each needs an owner for the stream.
- SES bounce automation: the SNS topic emails the operator on bounces
  today. That is not a suppression list; revisit when volume justifies it.
- Mobile tests and OTA updates: the parity plan accepted manual-only
  testing; OTA (`expo-updates`) is its own decision before the next store
  release.
- Root README and moving the 40 root `plan-*.md` files: housekeeping.
