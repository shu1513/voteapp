# SSR / Prerender Plan (crawler-readable HTML)

Written 2026-07-05, before first public deploy. Goal: search engines and AI
crawlers (the ones robots.txt/llms.txt already welcome) must be able to read
election and candidate content from raw HTML. Companion to plan.md's parked
"SSR/prerender for SEO + AI crawlers" item, which this supersedes.

## What the audit found

1. The app is a plain Vite SPA: `index.html` ships `<div id="root"></div>`,
   and titles, meta descriptions, and JSON-LD are injected client-side
   (`useDocumentTitle`, `useJsonLd`). Both hooks already carry comments
   saying full crawler coverage waits on this plan.
2. Verified (July 2026): **no major AI crawler executes JavaScript.**
   GPTBot, ClaudeBot, PerplexityBot, Meta-ExternalAgent fetch raw HTML with
   tight timeouts and move on — an analysis of 500M+ GPTBot fetches found
   zero JS execution. The only renderer is Googlebot (and Gemini via the
   same Web Rendering Service). So today, every AI crawler sees a blank
   page on every route.
3. `frontend/public/sitemap.xml` is static and lists only six fixed routes.
   Even a JS-rendering crawler cannot *discover* `/elections/:id` or
   `/candidates/:id` URLs — nothing links to them from crawlable HTML.
4. The anonymous public API already serves everything a server renderer
   needs: `GET /api/elections/:id`, `GET /api/candidates/:id` (plus
   `/api/ballot`, `/api/research-areas`). No new data paths required.
5. The frontend is already on React Router 7.18 in data mode
   (`createBrowserRouter`). Framework mode — the same library's documented
   upgrade path — adds exactly the two capabilities needed: build-time
   `prerender` and per-route `ssr` with loaders. Vite 8 is supported since
   react-router 7.14 (peer-dep fix landed 2026-05); React Router v8
   (2026-06-17) is a minimal-breakage upgrade later, not a blocker now.
6. Route inventory by crawler value:
   - **Prerenderable static**: `/`, `/disclaimer`, `/terms`, `/privacy`,
     `/register`, `/login` — finite, content known at build time (legal
     pages `?raw`-import their full markdown, so prerendered HTML carries
     the entire text).
   - **SSR-worthy dynamic**: `/elections/:id`, `/candidates/:id` — the
     content crawlers actually want; thousands of URLs, growing, updated
     continuously by research. Cannot be enumerated at build time without
     going stale between builds.
   - **Client-only forever**: `/ballot?d=...` (infinite non-canonical
     query-string space; llms.txt already documents it), auth flows, and
     `/me/*` (private, personalized).
7. Not launched yet: deploy topology (static host + API server behind one
   reverse proxy) is written down but nothing is provisioned. Adding an SSR
   Node process is a topology change — far cheaper to decide now than to
   re-platform after launch.
8. Tests are safe: page tests render components through `MemoryRouter`
   (`src/test/render.tsx`), and route-module migration keeps components as
   plain default exports. Playwright e2e drives real URLs and is
   render-mode-agnostic.

## Decision

**React Router framework mode, hybrid rendering.** Prerender the static
routes at build time; server-render only the two detail page types; leave
ballot/auth/me pages client-rendered. No framework switch (Next.js/Remix
would be a rewrite for capabilities RR framework mode already has), no
second routing system (Vike), no headless-browser snapshotting (react-snap
is unmaintained and can't cover unenumerable URLs), no user-agent-sniffing
"dynamic rendering" (deprecated by Google, cloaking-adjacent, and a second
rendering pipeline to maintain).

ISR-style caching (render once, serve cached, revalidate) is the likely
end-state under real crawl load, but it is **one `headers` export + CDN
config away** from Phase 2's output — so it is deferred until measurement
says it's needed, not built speculatively.

## Phase 0 — dynamic sitemap (backend only, do regardless)

The discovery gap is independent of rendering and pays off immediately:
Googlebot *does* render JS, so a real sitemap makes election/candidate
pages indexable by Google before any SSR lands.

- `GET /sitemap.xml` on the API server: the six static routes plus
  `/elections/:id` (all rows) and `/candidates/:id` (excluding
  `deleted_at IS NOT NULL` / merged candidates), `<lastmod>` from
  `updated_at`. Public origin from existing config (same source as
  `NOTIFICATIONS_UNSUBSCRIBE_URL` / `AUTH_PUBLIC_BASE_URL`).
- In-memory cache (~1 hour TTL) — one query per hour, crawler-storm-proof.
  Well under the 50k-URL sitemap limit for the foreseeable future.
- Delete `frontend/public/sitemap.xml`; dev proxy + prod reverse proxy route
  `/sitemap.xml` to the API server. robots.txt already points at it.

## Phase 1 — framework mode migration, `ssr: false` + static prerender

Pure mechanical migration; app behavior and hosting model unchanged
(output is still static files). This is the reversible baby step that
de-risks Phase 2.

- Swap `@vitejs/plugin-react` for `@react-router/dev`'s `reactRouter()`
  plugin; add `@react-router/node`. Keep the Sentry vite plugin, dev proxy,
  and `fs.allow` scoping exactly as they are.
- `react-router.config.ts`: `ssr: false`,
  `prerender: ["/", "/disclaimer", "/terms", "/privacy", "/register", "/login"]`.
- `src/root.tsx` replaces `index.html` (same head content via `Layout` +
  `Meta`/`Links`/`Scripts`); `src/routes.ts` replaces the
  `createBrowserRouter` array (config-based routes, not file-convention —
  smallest diff); `entry.client.tsx` keeps `initErrorMonitoring()` and
  StrictMode. `App.tsx` becomes the layout route; `RouteError` becomes its
  `ErrorBoundary`. QueryClient/provider move to root.
- **No loaders yet.** Every page keeps its TanStack Query fetching
  unchanged.
- Build emits prerendered HTML for the six routes (legal pages now carry
  full text in raw HTML) plus `__spa-fallback.html` for everything else —
  the static host's SPA rewrite target changes from `index.html` to the
  fallback file (update docs/deploy-checklist.md).
- Gate: vitest suite, Playwright suite, and a manual
  `curl` of the built output confirming `/disclaimer` HTML contains the
  disclaimer text with JS disabled.
- Known risk to watch: hydration mismatches from locale/timezone-sensitive
  formatting (`toLocaleDateString`) once HTML is pregenerated — pin
  formatting to an explicit locale/UTC if warnings appear.

## Phase 2 — `ssr: true` + server loaders for the two detail types

The payoff phase: election and candidate pages become fully readable by
non-JS crawlers, with correct titles, descriptions, JSON-LD, and real HTTP
status codes.

- Flip `ssr: true` (prerender list stays; RR supports both together —
  listed paths stay static, everything else renders at request time).
- `loader` on the election and candidate route modules: server-side fetch
  of `GET /api/elections/:id` / `/api/candidates/:id` against
  `API_INTERNAL_URL` (default `http://127.0.0.1:3001`). Components read
  the subject from `loaderData` instead of `useQuery`; the personalization
  layers (follows, research-area preferences, `useMe`) stay client-side
  TanStack Query untouched — crawlers don't need them and they require
  cookies anyway.
- `meta` export (from loader data) replaces `useDocumentTitle` on these two
  routes; JSON-LD becomes a `<script type="application/ld+json">` rendered
  inline in the component (server-emitted), replacing `useJsonLd` there.
  The hooks remain for client-only pages.
- Unknown id: loader throws a 404 response → route `ErrorBoundary` renders
  not-found UI and the crawler gets a real HTTP 404 (the SPA can only fake
  this today).
- Serving: `react-router-serve ./build/server/index.js` — a second small
  Node process next to the API server; it needs only `API_INTERNAL_URL`
  (no DB, no Redis, no secrets). Reverse proxy: `/api/*` + `/sitemap.xml`
  → API server, everything else → SSR server. Update
  docs/deploy-checklist.md topology + env tables.
- Ballot/auth/me routes get no loaders: SSR emits their shell instantly and
  the client fetches after hydration, exactly like today.
- Error monitoring stays client-side for now (Sentry server-side SSR init
  is a separate, optional follow-up; the API server already has coverage).
- Gate: `curl -A "GPTBot" https://<host>/elections/<id>` returns HTML
  containing candidate names, the title tag, and the JSON-LD block;
  `curl -sw '%{http_code}' .../elections/<bad-id>` returns 404; Playwright
  suite green against the SSR server.

## Phase 3 — cache under load (post-launch, evidence-gated)

Only when metrics show crawl or traffic load hurting the SSR or API server:

- `headers` export on the two detail routes:
  `Cache-Control: public, s-maxage=300, stale-while-revalidate=86400`, and
  put a CDN in front of the SSR server. That is the ISR pattern — first
  request renders, the CDN serves everyone else, background revalidation
  keeps it ≤5 minutes stale — with zero new application machinery.
- Trigger to act: sustained SSR CPU/latency alerts or API load dominated by
  crawler traffic. Do not build ahead of that signal.

## Explicitly not doing

- Next.js / Remix migration — nothing needed that RR framework mode lacks.
- Vike or a prerender plugin — second routing/rendering system to maintain.
- Build-time prerender of election/candidate pages — thousands of URLs,
  stale between builds, and couples `npm run build` to a live database.
- Bot-detection dynamic rendering — deprecated practice, cloaking risk.
- SSR content for `/ballot?d=...` — infinite non-canonical URL space.
- Streaming SSR, Suspense data patterns, RSC (still unstable in RR), and
  TanStack Query dehydration/hydration — the loader carries the subject;
  everything else stays a plain client fetch.
- React Router v8 upgrade bundled into this work — do it separately later;
  v8's breaking changes are all adoptable on v7 first.

## Order and rationale

Phase 0 is independent and immediately useful (Google indexing) — it can
ship this week regardless of the rest. Phase 1 before first deploy, so the
hosting story never has to change twice (the SPA-rewrite target and build
pipeline are set up once, correctly). Phase 2 immediately after Phase 1
proves green — it is the actual goal; before launch is ideal since the
reverse proxy gets configured exactly once. Phase 3 waits for evidence.

Each phase lands on its own branch/PR with the suite green; Phase 1 and 2
are separable on purpose — if Phase 2 stalls, Phase 1's output still
deploys as a pure static site with prerendered legal/marketing pages.
