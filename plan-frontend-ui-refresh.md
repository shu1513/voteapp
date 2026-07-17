# Frontend UI refresh plan

Findings from a deep read of `frontend/src` plus a live walkthrough (anonymous
address search → ballot → election → candidate, desktop and 375px mobile) with
the Houston test address and the record-heavy candidates (Irene Bustamante
Adams, 71 records; John James, 57 records).

## Verdict on "brand new frontend"

Not worth it. The stack is current (React Router 7 SSR, React 19, TanStack
Query, Tailwind 4, Headless UI) and the architecture is sound: SSR loaders for
crawlable pages, careful a11y (skip link, focus-on-navigate, aria-labels on
repeated disclosures), good error/empty states, personal data kept out of
URLs. The problems are all **presentation-layer**: information density, visual
hierarchy, and repetition. A redesign of a handful of components fixes ~90% of
what feels rough, at ~5% of the cost of a rewrite.

## What actually hurts today (observed, in priority order)

1. **Chip soup on ballot cards.** Every `ElectionCard` renders *every*
   research area as an identical gray chip — up to 18 per card. The Houston
   ballot is 19 cards × ~12 chips. On mobile one card fills an entire screen;
   the information (title, date, candidates, vote power) drowns. The chips
   are also not differentiated: statewide races repeat the same chip set over
   and over.
2. **Date repeated 19×.** Every election on the ballot is "November 3, 2026",
   stamped on each card. No grouping; the eye re-reads the same date 19 times.
3. **Candidate page is a 12,000px wall.** 71 records render as one flat
   sequence of bordered cards; "Report an issue" appears **72 times** as an
   underlined text link, giving a trust-and-safety affordance the same visual
   weight as the content. No way to collapse a group or jump to an issue.
4. **Logged-in header will wrap on mobile.** `AccountNav` renders 5 inline
   items ("Hi name · My ballot · Following · Settings · Log out") in a
   `gap-4 text-sm` row next to the logo — at 375px this wraps into a messy
   two-line header. (Anonymous header is fine.)
5. **Election page: duplicate source lines.** `data.sources` renders
   verbatim; the Attorney General page shows "Source: sos.state.tx.us" twice.
6. **Plain first impression.** The home page is functional but generic: no
   statement of what you get, all-white, disabled gray search button. It reads
   as a form, not a product.

## Proposed changes (do now)

### 1. Redesign `ElectionCard` chip row — highest impact, small diff
`frontend/src/components/ElectionCard.tsx`

- Keep status chips (candidate count / roster status, vote power,
  competitiveness, results, "You follow …") — these are per-card signal.
- Research-area chips: show **saved-area matches first (highlighted, all of
  them), then at most 3 others, then a `+N more` chip**. No interaction
  needed — the election page already shows the full set; the card is a
  preview, not an index.
- Mobile result: card drops from ~950px to ~200px.

### 2. Group ballot by date
`frontend/src/pages/BallotPage.tsx`, `SavedBallotPage.tsx`

- When consecutive elections share `election_date`, render one date heading
  ("November 3, 2026") and omit the per-card date. Only applies under the
  default sorts; a heading per distinct date, cards unchanged otherwise.
  (Extract the small grouping helper + list rendering shared by both pages.)

### 3. Candidate record: collapsible issue groups + calmer report link
`frontend/src/pages/CandidatePage.tsx`

- Wrap each issue group in `<details open>`-style disclosure with the record
  count in the summary ("Civil Rights · 12"). Default: first 2–3 groups open,
  rest collapsed (or all collapsed when >5 groups). "Newest first" view stays
  flat but gets a "show more" cutoff (e.g. first 20, button reveals rest).
- Demote the per-record "Report an issue" from underlined link to a small
  muted icon-ish text (`text-xs text-ink-soft/70`, right-aligned on the meta
  row) — still per-record and discoverable, no longer 72 shouting links.
- Optional (cheap): issue-jump chips under the "Record" heading anchor-linking
  to each group.

### 4. Collapse logged-in nav into a menu on small screens
`frontend/src/App.tsx`

- Use Headless UI `Menu` (already a dependency) for the signed-in state:
  trigger "Hi {name} ▾" with My ballot / Following / Settings / Log out.
  Keep inline links at `sm:` and up if desired, or use the menu everywhere
  for one code path. Anonymous state unchanged.

### 5. Small fixes (one-liners)
- `ElectionPage`: dedupe `data.sources` before rendering (`[...new Set()]`).
- Home page: give the Search button a visible-but-disabled style
  (`disabled:opacity-50` on rausch instead of gray `bg-line`) so the page
  doesn't look broken before the checkbox is ticked.

### 6. Home page warmth — small, no marketing site
`frontend/src/pages/HomePage.tsx`

- Add one short benefits line under the intro (e.g. three inline items:
  "Every election on your ballot · AI-researched candidate records ·
  Campaign finance, sourced") and a very light `bg-surface` hero band.
  No images, no new components, ~15 lines.

## Explicitly deferred (not now)

- **Dark mode** — touches every className; do as its own pass.
- **Skeleton loaders** — current text notices are acceptable; revisit after
  the density fixes land.
- **Card/segmented-control design-system extraction** — only worth it if the
  above reveals real duplication pain.
- **Any rewrite/re-platform** — no justification.

## Verification

- Re-run the Houston walkthrough (anonymous search → ballot → election →
  candidate) at desktop and 375px; ballot page should show grouped dates and
  compact cards; Bustamante Adams page should open collapsed groups and stay
  scannable.
- `npm run test` + `npm run typecheck` in `frontend/` (existing tests cover
  BallotPage/CandidatePage rendering; update assertions that count chips or
  rely on per-card dates).
