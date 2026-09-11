# Error Monitoring Plan

*Written 2026-07-05 after auditing the actual error paths. Goal: know about
crashes and silent 500s before users report them (they won't). Not a goal:
performance tracing, session replay, log aggregation — all skipped
deliberately.*

## What the audit found

| Surface | Today | Risk |
|---|---|---|
| API unexpected errors | `createApiErrorMiddleware` (apiServer.ts) maps any thrown error to a 500 response and **logs nothing** | Prod 500s are completely invisible — the single biggest gap |
| API process | no `uncaughtException` / `unhandledRejection` handlers beyond Node defaults | crash reason lost unless the process manager captures stderr |
| Frontend render crashes | `RouteError` errorElement shows the fallback page; `console.error` in dev only | user sees "Something went wrong", we never do |
| Frontend API/query errors | `ApiError` envelope → `Status.tsx` copy | handled by design; only 5xx bursts are signal |
| BullMQ workers (digest, alerts, reminders) | `console.error` + job failure | invisible unless someone tails worker logs |
| CLI scripts (broadcast, prune, syncs) | print + `process.exitCode = 1` | operator-run, failure is seen — lowest priority |

## Decision

**Instrument with the Sentry SDK; start on Sentry SaaS free tier; keep the
exit open.** Reasons:

- GlitchTip (open-source, self-hostable) accepts Sentry SDKs unchanged — if
  data-residency concerns grow post-launch, swapping the DSN moves all data
  in-house with zero code change. The SDK choice is not vendor lock-in.
- Self-hosting a monitoring stack before the app itself has a prod box is
  over-engineering. SaaS free tier (5k events/mo, email alerts on new
  issues) is plenty at launch scale.
- Errors only. `tracesSampleRate: 0`, no session replay, no profiling —
  each adds cost, config, and privacy surface for no launch value.

**Setup shape:**

- Two Sentry projects — `voteapp-backend`, `voteapp-frontend` — so browser
  noise never buries a real backend failure. Workers report into the
  backend project tagged `component:worker` (a third project is premature).
- `release` = git SHA in both inits (and in the source-map upload), so
  every event answers "which deploy caused this."
- Alert config (one-time, in Sentry): email on new issue and on regression
  (defaults), plus one spike rule (same issue ≥ N times in 10 minutes).
- Create the org/projects early (free, no-regret); DSNs live in the deploy
  secret store, never the repo. Everything stays dark until a DSN is set.

**Privacy posture (this app holds addresses, emails, political interests).
Explicit config, not trust-the-defaults:**

- `sendDefaultPii: false` (SDK default — keep it, but treat as one layer).
- Never capture request bodies (addresses travel in POST bodies) — set the
  SDK's request-body capture off explicitly.
- Request headers: allowlist only (method, path, status); explicitly drop
  `Authorization`, `Cookie`, and any user-identifying header.
- `beforeSend` (shared scrubber, used by both SDKs): strip URL query
  strings (`?d=<district-ids>` is location-adjacent), drop anything shaped
  like an email from contexts/tags/extras, drop `user.ip_address`.
- No user identification in events (no `setUser`; anonymous events only).
- Privacy Policy: add an "Error monitoring provider" processor entry, same
  conditional pattern as the analytics entry ("when error monitoring is
  enabled…"), named before enabling. Sentry SaaS = US processing, already
  consistent with the US-processing disclosure.
- **Hard rule: the scrubber and the privacy-policy entry land in (or
  before) the same PR that could enable production — the prod DSN never
  flips on before a staging scrub test passes.**

## Phase 1 — backend visibility floor (no vendor, do regardless)

1. `createApiErrorMiddleware`: log unexpected errors (the ones mapped to
   500) with `console.error` including method + path — never the body.
   Today's silence is a bug independent of any monitoring vendor.
2. Request id on failures: the middleware generates a short id, includes it
   in the log line, and returns it as `request_id` in the 500 error
   envelope (additive field). A user email saying "I saw an error" becomes
   a findable log line — and later a findable Sentry event (Phase 2 tags
   it).
3. `runAddressApiServer`: `process.on("unhandledRejection")` /
   `("uncaughtException")` → log and exit nonzero (let the process manager
   restart; matches the deploy-checklist runbook).
4. Tests for the middleware logging and the `request_id` field.

## Phase 2 — Sentry SDK, backend

1. `@sentry/node` in backend. `Sentry.init` gated on `SENTRY_DSN` env (unset
   = disabled; local dev and tests never send). `environment` from a new
   `DEPLOY_ENV` var; `release` from the build's git SHA.
2. Capture points, smallest set:
   - the Phase-1 middleware log line → `Sentry.captureException`, tagged
     with the Phase-1 `request_id`;
   - the process-level handlers;
   - scheduler workers: BullMQ `failed` handler + the existing catch blocks
     in the three notification workers, tagged `component:worker`.
3. `beforeSend` scrubber (shared util) per the privacy posture above:
   query strings, emails anywhere, IP, header allowlist, no bodies; cap
   breadcrumbs.
4. Env docs: `SENTRY_DSN` + `DEPLOY_ENV` rows in docs/deploy-checklist.md.

## Phase 3 — Sentry SDK, frontend

1. `@sentry/react` in frontend. Init in `main.tsx` gated on
   `import.meta.env.VITE_SENTRY_DSN` (unset locally = disabled), errors
   only, same scrubbing rules, `release` = git SHA (must match the
   source-map upload).
2. Capture points:
   - `RouteError`: `captureException(useRouteError())` — the crash boundary
     built in Phase 5;
   - QueryClient `queryCache.onError` for `ApiError.status >= 500` only
     (4xx are expected product states, not defects).
3. Source maps: `@sentry/vite-plugin` upload on prod build (auth token via
   env, never committed) — stacks are useless minified. Vite's prod build
   already defaults `sourcemap: false`, so maps go to Sentry without being
   publicly served; keep it that way.
4. Privacy Policy edit (same PR): error-monitoring processor entry,
   conditional wording, provider named.
5. Verify live before any prod DSN: enable in a staging/prod-like build,
   throw a test error behind a temporary route, confirm the event arrives
   scrubbed (no query strings, no email, no cookies/headers, no body, no
   IP) and carries the right release, then remove the test route.

## Explicitly not doing

- Session replay / performance tracing / profiling (cost + privacy, no
  launch value).
- Self-hosted GlitchTip now (ops before the app has ops; revisit
  post-launch if data residency matters — DSN swap, no code change).
- Sentry tunnel endpoint to dodge ad-blockers (over-engineering; accept the
  loss).
- User feedback widget (the disclaimer already routes error reports to
  contact@impactperdollar.com; revisit if report volume justifies it).
- CLI script instrumentation (operator sees failures directly).

## Order and rationale

Phase 1 is vendor-free and fixes an actual bug (silent 500s) — do it now.
The Sentry org + two projects can be created any time (free, no-regret);
Phases 2 and 3 can also be implemented any time because everything is dark
without a DSN. The only hard gate is enablement: prod DSN flips on only
after the staging scrub test and the privacy-policy entry are in. One
branch/PR per phase, live verification before push, as usual.
