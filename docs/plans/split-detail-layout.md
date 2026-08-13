# Plan: split-screen master–detail layout for election and candidate pages

Status: phase 1 shipped (PR #701 — DetailRail + ElectionPage); phase 2
implemented (CandidatePage roster rail; verified live — election → candidate
→ roster walk → exit restores the election's own ballot rail). Frontend-only;
no backend, API, route, or migration changes. Each phase is one PR.

## Goal

On wide screens, a detail page (election or candidate) shows the list the
visitor came from as a rail on the left and the detail content on the right —
the Gmail-style master–detail pattern. Clicking a sibling in the rail swaps
the detail panel without losing the reader's place in the list. The rail
replaces the top `DetailPager` bar ("Prev: … / Go back to: … / Next: …") on
desktop; narrow screens keep the current full-page flow and the pager bar
unchanged.

There is deliberately **no split-screen mode toggle**. The split is derived
from the route: list routes (`/me/ballot`, `/draft`, `/me/picks`,
`/me/follows`, `/ballot`) stay full-width lists; detail routes
(`/elections/:electionId`, `/candidates/:candidateId`) render the rail when
sibling context is available. The first click on a list item "enters" the
split because it navigates to a detail route; the "exit" control is the rail
header's back link (the existing `backTo` destination). This keeps URLs
canonical and shareable, browser back/forward correct, and adds zero new
persistent state.

## Background / verified facts

- Routes are flat under one layout (`frontend/src/routes.ts`): list pages and
  detail pages are separate routes, not nested. No route changes needed —
  the rail is rendered by the detail pages themselves.
- Detail pages already receive everything the rail needs via router state
  (`frontend/src/lib/detailNavContext.ts`):
  - `ElectionNavState.contests` — the ballot in displayed order
    (`{id, title}[]`), validated on read.
  - `CandidateNavState.candidates` — the race roster in displayed order
    (`{id, name}[]`), plus `electionId`.
  - Both carry `backTo` (path + label) and a nested `backState` that restores
    the previous page's own context on the back hop.
  - Validation is defensive: a corrupt or stale history entry degrades field
    by field, and `pagerNeighbors` returns null when the current id is
    missing from the snapshot. The rail inherits all of this for free.
- `ElectionPage.tsx` (~line 205) reads `readElectionNavState(location.state)`
  and renders `DetailPager` only when nav state exists. Deep links get no
  bar — by product choice. Same structure in `CandidatePage.tsx` (~line 463),
  including a special case that relabels an election back destination to
  "Election" for the pager caption.
- Both detail pages are `mx-auto max-w-3xl px-4 py-8`; the app header is also
  `max-w-3xl` (`App.tsx` ~line 181).
- Router state survives same-browser reloads (React Router copies it into
  `window.history.state`) but SSR renders without it, so the pager — and
  therefore the rail — appears only after hydration. This is the existing
  `DetailPager` behavior; the rail changes page width on desktop, so the
  hydration reflow is more visible than the pager's (see risks).

## UX spec

- Breakpoint: rail renders at `lg:` (1024px) and up. Below `lg` nothing
  changes — current pages, current `DetailPager`.
- Desktop detail layout: outer container widens to `lg:max-w-6xl`, grid
  `lg:grid-cols-[18rem_minmax(0,1fr)] lg:gap-8`. The detail column keeps its
  current content width (`max-w-3xl`), so the reading experience is
  unchanged.
- Rail structure, top to bottom:
  1. **Exit link** — the existing `backTo` (label truncated, `title=` for
     full text). Rendered as "← {backTo.label}". This is the user-visible
     "leave split screen" control: it returns to the full-width list page.
     Passes `backToState` exactly as `DetailPager` does today.
  2. **Sibling list** — one compact row per entry (title/name only, that is
     all the nav state carries — sufficient by design; the rail is a map,
     not a card list). Current page's row: highlighted, `aria-current="page"`,
     not a link. Other rows: `Link` with `state={navState}` — verbatim, the
     same `siblingState` contract `DetailPager` uses, so walking the rail
     preserves the whole context chain.
- Rail is `lg:sticky lg:top-4 max-h-[calc(100vh-2rem)] overflow-y-auto`; on
  mount and on id change the current row scrolls into view
  (`scrollIntoView({block: "nearest"})`).
- Rail is a `<nav>` landmark: `aria-label="Ballot"` on the election page,
  `aria-label="Candidates in this race"` on the candidate page.
- When the rail renders, `DetailPager` gets `lg:hidden` (it is fully
  redundant with the rail on desktop). When there is no rail context —
  deep link, stale snapshot, or sibling list absent/short — behavior is
  exactly today's: full-width `max-w-3xl` page, pager bar or nothing.
  A rail with only the current entry is not rendered (mirrors
  `pagerNeighbors`' `length < 2` rule); the exit link alone is not worth a
  column.
- Level semantics: the election page's rail is the **ballot** (contests);
  the candidate page's rail is the **race roster** (candidates). The exit
  link hops one level up each time — candidate rail exits to the election
  page, election rail exits to the originating list — which the nested
  `backState` chain already models.

## Phase 1 — `DetailRail` component + election page adoption

1. New `frontend/src/components/DetailRail.tsx`:
   - Props: `ariaLabel`, `entries: {id, label, path}[]`, `currentId`,
     `backTo`, `backToState?`, `siblingState?` — deliberately the same
     vocabulary as `DetailPager`.
   - Pure presentational; no data fetching, no context reading. The page
     builds `entries` from its validated nav state.
   - Handles highlight, `aria-current`, truncation, sticky scroll container,
     scroll-into-view of the current row.
2. `ElectionPage.tsx`:
   - Build rail entries from `navState.contests` (ids → `/elections/:id`
     paths, exactly as the pager's prev/next links are built today).
   - Render: `navState?.contests` valid and has ≥ 2 entries **and** contains
     `data.id` → rail on `lg`+, `DetailPager` wrapped in `lg:hidden`.
     Otherwise → current markup untouched.
   - Container: conditional classes — with rail
     `mx-auto lg:max-w-6xl px-4 py-8 lg:grid lg:grid-cols-[18rem_minmax(0,1fr)] lg:gap-8`
     with the detail column keeping `max-w-3xl min-w-0`; without rail, the
     existing `mx-auto max-w-3xl px-4 py-8` string stays byte-identical.
3. Tests (extend `ElectionPage.test.tsx`, new `DetailRail.test.tsx`):
   - Rail renders the contest sequence, marks the current contest
     `aria-current`, links carry the nav state.
   - No nav state → no rail, pager-less page (existing tests keep passing).
   - Sibling list missing the current id → no rail (stale-snapshot rule).
   - Exit link path/label/state equal `DetailPager`'s back slot.

## Phase 2 — candidate page adoption

1. `CandidatePage.tsx`: same pattern with `navState.candidates` →
   `/candidates/:id` entries. Keep the existing `pagerBackTo` "Election"
   relabel for the mobile pager; the rail's exit link uses the full
   `backTo.label` (an election ballot title — rail rows truncate, so length
   is fine and the fuller label is clearer there).
2. Tests mirroring phase 1 (`CandidatePage.test.tsx`).
3. Cross-page walk test: election → candidate → rail-walk to sibling
   candidate → exit → election still has its ballot context (the
   `backState` chain survives rail navigation because rail links pass
   `navState` verbatim).

## Explicitly out of scope (v1)

- **Live rail state.** The rail is the snapshot the visitor saw — identical
  semantics to today's prev/next. A pick made in the detail panel does not
  update rail badges; there are no badges. Revisit only if the rail later
  grows pick/follow indicators.
- **In-pane rendering of the detail on list pages** (true single-route
  master–detail with nested routes). Rejected: it would fork every list
  page's layout and duplicate detail data loading; the route-derived split
  gets the same UX for a fraction of the change.
- **Keyboard prev/next shortcuts.** The hidden-on-desktop pager removes the
  only prev/next affordance from keyboard-tab flow, but the rail links are
  tabbable in list order, which is equivalent coverage. Arrow-key
  navigation can come later if asked for.
- Mobile app (`mobile/`) — untouched.

## Risks / notes

- **Hydration reflow**: SSR has no router state, so a same-tab reload of a
  detail page renders full-width, then snaps to split after hydration. The
  pager already appears this way; the rail additionally shifts the content
  column. Accepted for v1 (reload-with-state is the rare path; external
  deep links never have state so never shift). If it proves jarring, a
  sessionStorage width hint is a follow-up, not a blocker.
- **Long ballots**: rails with 40+ contests are why the rail scrolls
  independently and scroll-into-view is required, not optional.
- The `contests` snapshot already reflects the list page's applied
  sort/filters (it is built from the displayed order), so the rail always
  matches what the reader just saw — do not "improve" it by refetching.
