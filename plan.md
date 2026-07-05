# VoteApp Frontend Implementation Plan

*Written 2026-07-03 against main `30f2ba9a`. Backend is frontend-ready; this
plan consumes it without backend changes unless explicitly listed.*

## 1. What the backend provides today (fresh audit)

### Anonymous (the whole core product works logged-out)

| Endpoint | Notes |
|---|---|
| `POST /api/address/autocomplete` / `.../retrieve` | Google proxy. Complete frontend contract in [docs/address-autocomplete-frontend.md](docs/address-autocomplete-frontend.md) — session tokens, debounce, ARIA, billing, "powered by Google". |
| `POST /api/address/resolve` `{address}` | → `districts[]` + `district_ids`. 422 `address_not_found` for bad addresses. |
| `GET /api/ballot?district_ids=…&sort=…&followed_first=…` | Election summary cards: `official_ballot_title`, `election_date`, `district`, `office`, `research_areas`, `candidate_count`, `historical_competitiveness`, `vote_power`, `has_results`. Sorts: `vote_power` (default), `soonest`, `district_size`, `district_size_smallest`. Max 50 district ids. |
| `GET /api/elections/:id` | Full detail: candidates (profiles, finance summaries incl. industry backing), ballot measure (`what_yes_means`/`what_no_means`), results, sources. |
| `GET /api/candidates/:id` | Profile, records with `source_url` + dates + research-area tags/stances, election history, `is_following`/`follow` when a session exists. |
| `GET /api/research-areas` | Catalog for preference picking and tag display. |

### Auth (cookie session `voteapp_auth_session`, httpOnly, SameSite=Lax, 30d)

| Endpoint | Frontend notes |
|---|---|
| `POST /api/auth/register` | **Requires `accepted_terms_version` matching the backend's `CURRENT_TERMS_VERSION` ("1.0")**; 400 names the current version, so the client can re-fetch and re-prompt. Returns `{status:"ok"}` — **no auto-login**; show "check your email", then user logs in. |
| `POST /api/auth/login` | Sets session cookie. Response body is only `{status:"ok"}` — call `GET /api/me` after. |
| `POST /api/auth/logout` / `logout-all` | Clear cookie. Both require JSON content-type (CSRF guard) — always send `Content-Type: application/json` with `{}`. |
| `POST /api/auth/verify-email` `{token}` | Email links land on **frontend route `/verify-email?token=…`**, which POSTs this. |
| `POST /api/auth/forgot-password` / `reset-password` | Reset links land on **frontend route `/reset-password?token=…`**. |
| `POST /api/auth/verify-email-change` `{token}` | Change-email links land on **frontend route `/verify-email-change?token=…`**. |
| `POST /api/auth/resend-verification` `{email}` | For the unverified interstitial. |
| Rate limits | Per-IP and per-email/account, `429` + `retry-after` header on all auth and password-verifying routes. Honor the header. |

`AUTH_PUBLIC_BASE_URL` must be set to the frontend origin so email links hit
the three routes above.

### Authenticated (all `/api/me/*` except `GET/PUT/DELETE /api/me` require verified email; unverified → 403 `"Email verification is required"`)

| Endpoint | Notes |
|---|---|
| `GET /api/me` | `{user:{email, first_name, email_verified}}`. Session check + "Hi {first_name}" + unverified detection. Never 403s. |
| `PUT /api/me` `{first_name}` | Name edit (1–80 chars). |
| `DELETE /api/me` `{password}` | Delete account; clears cookie. |
| `POST /api/me/password` `{current_password, new_password}` | Rotates **all** sessions, returns fresh cookie automatically — other devices get logged out, current one stays. Password policy: 12–1024 chars (surface in form). |
| `POST /api/me/email` `{new_email, password}` | Always returns ok (no enumeration); confirmation goes to the NEW inbox. |
| `PUT /api/me/address` `{address}` | Replaces districts, returns updated districts + ballot. |
| `GET /api/me/ballot` | Ballot from saved districts; includes `districts[]` (use for "your districts" display). Query params override saved preferences. |
| `POST /api/me/districts/initialize` `{district_ids}` | Anon→account handoff: pass the ids from the last anonymous resolve right after first login. |
| `GET/PUT /api/me/ballot-preferences` | `{sort, followed_first}`. |
| `GET/PUT /api/me/candidate-follows` | List includes `display_name` + record/election previews. PUT input: `{candidate_id, following, notify_elections?, notify_updates?}`. |
| `GET/PUT /api/me/research-area-preferences` | Ranked picks from the catalog. |
| `GET/PUT /api/me/email-preferences` | `{email_digest, email_election_reminders, email_new_election_alerts}`. |

Unsubscribe pages are backend-rendered (`/api/email/unsubscribe`) — no frontend work.

### Legal (must ship WITH the first public page, not after)

Texts live in [docs/legal/](docs/legal/checkbox-copy.md) and are versioned; the
frontend renders them **verbatim**:

- Pre-search clickwrap checkbox (unchecked by default, Search disabled until
  checked, visible links, sits directly above the button). Anonymous
  acceptance is frontend-only; remember per-version in localStorage.
- Signup clickwrap checkbox (18+, electronic consent) + send
  `accepted_terms_version: "1.0"` in the register call.
- Privacy notice line under the address input.
- AI-research banner on every ballot/election/candidate view.
- Per-record `Source: [link] · researched [date]` lines.
- `/disclaimer` route rendering docs/legal/disclaimer.md.

## 2. Stack (unchanged decision, still right-sized)

`frontend/` directory in this repo. **Vite + React + TypeScript + React
Router + TanStack Query + Tailwind.** Hand-written TS types mirroring the
snake_case JSON (no codegen). Vitest + React Testing Library.

- Dev: Vite proxy `/api` → `http://127.0.0.1:3001` — same-origin, zero
  CORS/cookie configuration.
- Prod (later): same-site subdomains (`app.` + `api.impactperdollar.com`) or a
  reverse proxy. SameSite=Lax cookies work across same-site subdomains;
  `ADDRESS_API_ALLOWED_ORIGINS` + `Secure` cookie flags at deploy time.
- No Next.js/SSR: no SEO requirement yet, SPA is the correct size. Revisit
  only if organic search matters later.

### Cross-cutting conventions (Phase 1 establishes, everything reuses)

- **API client:** thin `fetch` wrapper — JSON in/out, `credentials:
  "same-origin"`, parses the `{error:{code,message}}` envelope into a typed
  `ApiError`, surfaces `retry-after` on 429.
- **Auth state:** one TanStack Query `["me"]` on `GET /api/me`. 401 → logged
  out; `email_verified:false` → verified-gate interstitial for personalized
  pages. 403 from any `/api/me/*` → same interstitial (resend button).
- **Session semantics:** login/password-change set cookies automatically —
  after either, invalidate `["me"]` and refetch.
- **Legal gate component:** one reusable clickwrap checkbox component fed by
  the strings from docs/legal/checkbox-copy.md, localStorage-keyed by version
  for the anonymous flow.

## 3. Phases

### Phase 1 — scaffold, legal gate, anonymous core loop *(shippable product)*

1. Scaffold `frontend/` (Vite react-ts template), Tailwind, Router, Query,
   ESLint/vitest; `.claude/launch.json` entry; dev proxy to `:3001`.
2. API client + error envelope + types for address/ballot/election/candidate
   payloads; shared formatters (dates, money, district names, source links).
3. `/disclaimer` page (renders disclaimer.md) + legal-gate checkbox component.
4. Home page: address form (plain input), privacy notice line, clickwrap gate
   → `POST /api/address/resolve` → district list → navigate to ballot.
5. Ballot page: election summary cards (title, date, district, office,
   candidate count, research-area chips), default `vote_power` order,
   AI-research banner. District ids in the URL (`/ballot?d=…`) so results are
   shareable/reloadable.
6. Election detail page: candidates w/ finance summaries, ballot measure
   yes/no explanations, results when present, sources w/ dates.
7. Candidate detail page: profile, records grouped by research area w/
   source + researched-date lines, election history.
8. Loading/error/empty states everywhere; 422 address-not-found messaging;
   429 retry messaging.

*Definition of done:* stranger with the URL can go address → ballot →
candidate without an account, sees legal gate once, every AI-content view
carries the banner and source lines. Verified live against local backend.

### Phase 2 — address autocomplete + ballot polish

1. Autocomplete per the contract doc: session tokens, 250–300ms debounce,
   3-char minimum, AbortController, ARIA combobox, "powered by Google",
   plain-input fallback on any autocomplete error (never block manual entry).
2. Sort switcher (4 sorts) wired to query params.
3. Competitiveness + vote-power display w/ short "what is this" explainers
   (methodology honesty per the disclaimer).
4. Ballot-measure result badges, election result panels.

### Phase 3 — accounts

1. Register page: signup clickwrap (18+, electronic consent),
   `accepted_terms_version`, 12-char password hint, "check your email" state.
2. Login/logout; header shows "Hi {first_name}" from `["me"]`.
3. Email-link routes: `/verify-email`, `/reset-password`, `/verify-email-change`
   — each POSTs its token, shows success/failure, links onward. Set
   `AUTH_PUBLIC_BASE_URL` to the frontend origin.
4. Forgot-password flow; unverified interstitial w/ resend button (drives all
   403s).
5. Anon→account handoff: keep anonymous district ids in storage and call
   `POST /api/me/districts/initialize` only once `GET /api/me` reports
   `email_verified: true` — the endpoint is verified-gated, and login works
   while still unverified, so initializing straight after login would 403.
   Then land on `GET /api/me/ballot`.
6. `PUT /api/me/address` for address changes while logged in.
7. Returning-user home: logged-in + verified loads the saved ballot
   (`GET /api/me/ballot`); an empty saved ballot routes to address entry.

### Phase 4 — personalization + account settings

1. Follow buttons on candidate + election pages (`is_following` already in
   detail payloads); follows manager page w/ per-follow notify toggles.
2. Persisted ballot preferences (sort + followed-first) — settings +
   automatic via ballot page controls.
3. Research-area picker from the catalog.
4. Email preferences page (3 toggles — closes the loop with digest +
   new-election alert emails).
5. Account settings: name edit, change password, change email, logout
   everywhere, delete account (type-password confirm, destructive styling).

### Phase 5 — launch hardening *(before any public deploy)*

*Re-audited 2026-07-04 after reminders + research-area phases A–E landed.
Error copy is already centralized in `Status.tsx` (422/429/5xx, used by all
pages) and `VerifyPrompt` covers 403s; the combobox is Headless UI and the
rank editor ships a KeyboardSensor — so the old error-copy and a11y items
shrank, while two gaps the original plan missed were found in code.*

1. Quick wins (found by audit, trivial, launch-embarrassing): replace the
   scaffold `<title>frontend</title>` with per-route titles; add a NotFound
   catch-all route + router `errorElement` (today an unknown URL or render
   error is a blank page); add 404 copy to `Status.tsx` (bad election/
   candidate id currently reads "Something went wrong").
2. Page-level tests with mocked API for the six untested pages — Ballot,
   Election, Candidate, SavedBallot, Follows, Settings (686 lines, most
   complex, zero tests) — covering empty/error/unverified states. (Replaces
   the old "mock API fixtures" item; libs + Home/Register/VerifyToken are
   already tested.)
3. Playwright smoke tests, three loops: address → ballot → election →
   candidate; register → verify → saved ballot; save areas → my_areas
   default sort → drag-rank → ballot reorders.
4. Accessibility pass, narrowed to real gaps: focus management on route
   changes (currently none), keyboard pass over the rank editor and follows
   toggles, checkbox/label audit in the legal gates.
5. Production env checklist + deploy runbook — grew since the original
   plan: `ADDRESS_API_ALLOWED_ORIGINS`, cookie `Secure`/`SameSite`/domain
   flags, `AUTH_PUBLIC_BASE_URL`, `NOTIFICATIONS_UNSUBSCRIBE_URL`, API base
   URL wiring, plus the notification stack that now exists — Redis + BullMQ
   scheduler workers (digest, new-election alerts, election reminders) need
   a where-do-workers-run runbook, SES needs unsandboxing (`AUTH_FROM_EMAIL`,
   region config), and the issue-broadcast CLI needs an operator note.
6. AI discoverability basics (cheap, ship with launch): `robots.txt` that
   allows AI crawlers (GPTBot, ClaudeBot, PerplexityBot, Google-Extended),
   `llms.txt` at the site root describing the app and key pages, sitemap,
   descriptive titles/meta per route, schema.org JSON-LD on election and
   candidate pages. (Full crawlability needs the SSR/prerender item parked
   in Phase 6 — the SPA ships near-empty HTML until then.)

### Phase 6 — parked (do not build yet)

In-app notifications feed, analytics events (needs privacy-policy treatment
first), and:

- **Error monitoring** — pulled forward pre-launch; superseded the old
  "error-report endpoint UI" idea (no custom receiver needed). Scoped in
  [plan-error-monitoring.md](plan-error-monitoring.md): Sentry SDK
  (GlitchTip-compatible, so the vendor stays swappable), errors only,
  PII-scrubbed, three phases — backend logging floor, backend SDK,
  frontend SDK + privacy-policy processor entry.

- **SSR/prerender for SEO + AI crawlers** — the SPA serves near-empty HTML,
  so search engines and AI crawlers (the ones Phase 5's robots.txt/llms.txt
  welcome) can't read ballot/election/candidate content. Prerendering public
  routes is likely enough; full SSR only if organic/AI-referral traffic
  proves out.
- **MCP server** — remote Model Context Protocol server wrapping the
  existing anonymous API (`lookup_ballot(address)`, `get_election(id)`,
  `get_candidate(id)`, `list_research_areas()`) so AI assistants (Claude,
  and equivalents on other platforms) can answer "what's on my ballot?"
  with VoteApp data and link back. Thin read-only wrapper — no new data
  paths. Every tool response must carry the AI-research disclaimer +
  source/date lines, same as the web UI. Requires the public API deploy;
  directory listings (Anthropic connectors directory, ChatGPT apps) are a
  separate application/review step after the server exists.

## 4. Backend gaps discovered by this audit

None blocking. Two config-level items when phases land:

1. Phase 3: `AUTH_PUBLIC_BASE_URL` → frontend origin (email links).
2. Prod deploy: `ADDRESS_API_ALLOWED_ORIGINS`, cookie `Secure`/domain flags,
   `NOTIFICATIONS_UNSUBSCRIBE_URL` → public API origin.

## 5. Testing & verification

- Vitest + Testing Library, weighted toward logic: API client (envelope,
  429/401/403 handling), legal gate (blocks until checked, version-keyed),
  auth flows (register payload includes terms version, token pages), handoff
  logic. Presentational components get light smoke tests.
- Every phase ends with a live end-to-end pass against the local backend
  (real resolve → ballot → detail; real register/login with console mailer)
  before its PR.
- One branch + PR per phase: `feat/frontend-phase-1` … `-4`.
