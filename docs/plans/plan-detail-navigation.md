# Detail-Page Navigation Plan (back links + prev/next pagers) — v2

Goal: after internal navigation, a reader on an election or candidate page
can always identify where they came from and move on without the browser
back button — a labeled back link, plus "previous / next" pagers that walk
the ballot contest-by-contest and a roster candidate-by-candidate. Deep-link
visitors (shares, search) get a relevant escape route instead; no page can
reveal an origin the router never saw.

Web frontend only. Zero backend changes, zero URL/route changes, zero new
dependencies. The mobile app has the same gap but is explicitly out of scope
here.

v2 incorporates review feedback: saved-ballot query preservation, flat state
modeling, layered validation, a fuller producer inventory, an honest
district fallback label, and corrected router-state lifetime wording.

## Facts from the code (what shapes the design)

1. **Routes** (`frontend/src/routes.ts`): list pages are `/ballot`
   (anonymous, requires `?d=<district-ids>`) and `/me/ballot` (saved
   ballot); detail pages are `/elections/:electionId` and
   `/candidates/:candidateId`. Detail pages have **no back link of any
   kind** today.

2. **Detail pages are canonical and shareable.** Both use server loaders
   and share-card `meta`, and inbound links exist from at least seven
   places: `ElectionCard` (both ballot pages), the election roster,
   `CandidatePage`'s election-history list, `PicksPage`,
   `PublicPickCardPage`, `FollowedCandidatesSection` (links **and** a
   combobox that calls `navigate()`), and external shares/search. So a
   detail page **cannot know its parent list from the URL alone**, and any
   "next" order must be supplied by the page the visitor came from.

3. **Both ballot pages carry state in their query strings.** `/ballot`
   needs `?d=` (without it: "No districts selected"), and **both** ballot
   pages use `useBallotFilterParams()` for `?issues=`/`?impact=` filters
   (`BallotPage.tsx:55`, `SavedBallotPage.tsx:166`). A back link must
   preserve `location.pathname + location.search` — a hardcoded path would
   return the reader to a differently-filtered list.

4. **Router state is an established pattern here, and it survives
   reloads.** `HomePage` → `BallotPage` passes `matchedAddress` via
   `location.state` with defensive runtime checks (`BallotPage.tsx:39-47`).
   `SavedBallotPage.tsx:146-158` documents (with a regression test) that
   React Router copies navigation state into `window.history.state`, which
   **survives refresh and back/forward** — it had to capture-then-clear a
   sensitive banner because of exactly that. Consequence for us: nav
   context often outlives a reload (nice), but it is ephemeral and
   non-shareable, so nothing may *require* it, and stale copies from old
   deploys must validate safely.

5. **Displayed order ≠ payload order.** Ballot order depends on `?sort=`,
   the issues/impact filters, and `ElectionList`'s regrouping (readable
   races in date groups, then the "awaiting candidate information" tail —
   `ElectionCard.tsx:95-105`). Roster order depends on the
   my-issues/alphabetical sort, the party filter, and the records filter
   (`sortCandidatesByStance`, `ElectionPage.tsx`). Pagers must follow **the
   order the user just saw**, so sibling lists are captured from the
   rendered list by the producing page, never recomputed downstream.

6. **A candidate can be in several elections at once**
   (`candidate.elections` is an array; concurrent candidacies are real).
   "Back to the election" is ambiguous without arrival context.

7. **The district-scoped ballot is not "all elections".**
   `/ballot?d=<district_id>` is always a valid page
   (`ElectionDetail.district_id` exists), but it lists one district only
   and drops elections older than `BALLOT_PAST_ELECTION_VISIBILITY_DAYS`
   (= 3, `ballotLookup.ts:643`). A fallback link there is worthwhile; its
   label must say what it is.

8. `App.tsx` already moves focus to `<main>` on pathname change and mounts
   `<ScrollRestoration />` — pager navigation gets focus handling for free.

## Design

### Typed router state, validated in layers

New module `frontend/src/lib/detailNavContext.ts`:

```ts
export type BackTo = { path: string; label: string };  // destination only

export type ElectionNavState = {
  backTo: BackTo;
  /** Ballot contests in displayed order (races + measures + awaiting tail). */
  contests?: { id: string; title: string }[];
};

export type CandidateNavState = {
  backTo: BackTo;                     // usually the election page
  /** The election page's own nav state, restored on the back hop. */
  backState?: ElectionNavState;
  electionId?: string;                // scopes the candidate pager
  candidates?: { id: string; name: string }[];  // displayed roster order
};

export function readElectionNavState(state: unknown): ElectionNavState | null;
export function readCandidateNavState(state: unknown): CandidateNavState | null;
```

`BackTo` stays a pure destination — no nested state field, so no mutually
recursive type. The candidate state carries the election's context in a flat
sibling field (`backState`), and `BackLink` takes destination state as a
separate prop.

**Layered validation** (old history entries may hold shapes from any
deploy): an invalid `backTo` alone returns `null`; a valid `backTo` with
malformed `contests` / `electionId` / `candidates` / `backState` keeps the
back link and drops only the broken optional — each field degrades
independently. Back links are the core value and must survive pager-data
rot.

Names and titles ride along so pagers can label buttons ("← US Senate",
"Prop 12 →") with no extra fetch. Tens of contests = a few KB of strings —
well within history-state limits.

### Back link (ship first)

`frontend/src/components/BackLink.tsx`: a `Link` rendered as `← {label}`,
top of the page above the `h1` row, with an optional `state` prop
(forwarded to the `Link`) so chains survive round trips.

- **ElectionPage**
  - With state: `backTo` as given — "← All elections" (public ballot URL
    with its full query string), "← My Elections" (`/me/ballot` + query),
    "← My Picks", "← {candidate name}", "← Shared picks".
  - Without usable state (external link, stale state): fallback
    "← Elections in {formatDistrictName(data.district.name)}" →
    `/ballot?d=${data.district_id}` — honest about being one district's
    current list (fact 7).
- **CandidatePage**
  - With state: "← {backTo.label}" to `backTo.path`, passing `backState`
    as the link's state so the election page gets its ballot pager back.
  - Without state: if `candidate.elections.length === 1`, link that
    election (unambiguous even when historical); else if exactly one
    *ongoing* election, link it; otherwise no back link (the page's
    Elections section lists every race — guessing would misdirect).

### Prev/next pager (state-gated)

`frontend/src/components/DetailPager.tsx`, bottom of the page (before
`ReportContentButton`): a `<nav aria-label="…">` with previous link, the
`backTo` link, next link. Missing prev/next at sequence ends render as
empty slots. **Responsive:** ballot titles run long — stack as two columns
with a full-width back link below on narrow screens, three columns at
`sm+`, so labels never squeeze unreadable.

Rules:

- Renders only when the optional sibling list validated, contains the
  current id, and has ≥ 2 entries. Otherwise hidden cleanly (back link
  unaffected — layered validation).
- Prev/next links forward the same nav state they were rendered from (same
  sibling array, same `backTo`/`backState`), so a reader can walk the whole
  sequence end-to-end.
- Election pager walks the full displayed ballot — office races and
  measures alike.
- Candidate pager is scoped to `electionId` (fact 6); labels are candidate
  names.

### Producers (every internal link into a detail page)

| Producer | Link target | State passed |
|---|---|---|
| `ElectionList` (`ElectionCard.tsx`) — both ballot pages | election | `backTo` (page-supplied) + `contests` built by `ElectionList` in its own render order (flattened date groups, then awaiting tail) |
| `BallotPage` | — | supplies `backTo: { path: location.pathname + location.search, label: "All elections" }` |
| `SavedBallotPage` | — | supplies `backTo: { path: location.pathname + location.search, label: "My Elections" }` (preserves `?issues=`/`?impact=`) |
| `ElectionPage` roster cards | candidate | `backTo` = this election (+ title), `backState` = election's own incoming nav state, `electionId`, `candidates` = the rendered `orderedCandidates` mapped to `{id, name}` |
| `CandidatePage` election-history list (`:550`) | election | `backTo` = this candidate (`/candidates/{id}`, label = display name); no contests |
| `PicksPage` election links | election | `backTo` = "My Picks" (`/me/picks`); no contests |
| `FollowedCandidatesSection` (on **PicksPage**, `:323`) — links *and* the search combobox's `navigate()` call (`:146`, use the `{ state }` option) | candidate / election | `backTo` = "My Picks"; no siblings |
| `PublicPickCardPage` election + candidate links | both | `backTo: { path: location.pathname, label: "Shared picks" }`; no siblings (anonymous card is a real origin worth returning to) |

In `ElectionPage`, hoist the currently-inline
`sortCandidatesByStance(visibleCandidates, …)` call into a
`const orderedCandidates = …` used by both the JSX and the state payload —
one computation, guaranteed agreement.

## Deliberate limitations

- **Nav context is ephemeral and non-shareable.** It may survive
  same-browser reloads, back/forward, and session restore (fact 4), but a
  shared URL or external visit never has it — so every page must be fully
  usable without it, degrading to the fallback back link (or none) and no
  pager. No sibling refetch, no URL params, no sessionStorage cache —
  considered and rejected as over-engineering for a degraded-mode nicety.
- **The back link scrolls to top.** It's a forward navigation, so
  `ScrollRestoration` won't restore list position (browser back still
  does). Accepted; no `navigate(-1)` heuristics.
- **Returning from candidate to election does not restore roster filter
  controls** (party/records/sort are component state). Accepted — not
  worth persisting.
- **Sibling snapshots can lag server changes mid-walk.** Labels only;
  every click lands on a live server-loaded page. Harmless.

## Phases

### Phase 1 — back links

1. `lib/detailNavContext.ts`: types + layered `read*` validators (pure,
   unit-testable).
2. `components/BackLink.tsx`.
3. `ElectionPage`: read state; back link with district fallback.
4. `CandidatePage`: read state; back link with single-election →
   single-ongoing → nothing fallback chain.
5. All producers from the table (ElectionList `backTo`+`contests` prop
   plumbing included here since it's the same change; pagers just don't
   read `contests` yet).

### Phase 2 — pagers

6. `components/DetailPager.tsx` (responsive layout).
7. `ElectionPage`: bottom pager over `contests`, forwarding state.
8. `CandidatePage`: bottom pager over `candidates` scoped to `electionId`,
   forwarding state.

Phase 1 independently shippable; both can land as one PR with back links
committed first.

## Tests

Extend existing page test files (they already render routes with loaders):

- `detailNavContext` unit tests: valid shapes pass; junk and wrong-typed
  state → `null`; **valid `backTo` + malformed optional list keeps the
  back link fields** (layered degradation, each optional independently).
- `ElectionPage.test.tsx`: back link from state; fallback label
  "Elections in {district}" → `/ballot?d=<district_id>`; pager renders
  with ≥2 contests including current id; hidden on no/malformed/missing-id
  state while back link survives; prev/next neighbors labeled by title;
  **pager state survives two consecutive next clicks**; roster-link state
  reflects active party/records filters and sort order.
- `CandidatePage.test.tsx`: back link from state passing `backState`
  through; `elections.length === 1` historical fallback; several
  elections + no state → no back link; pager scoped to `electionId` with
  name labels; election-history links carry candidate `backTo`.
- Ballot pages: card click lands with `backTo` preserving the **full query
  string (`?d=`, `?sort=`, `?issues=`, `?impact=`)**; `contests` order =
  readable-races-then-awaiting-tail (differs from payload order).
- Round trip: election (with ballot state) → candidate → back → election
  still has its ballot pager.
- `PublicPickCardPage` and `FollowedCandidatesSection` (links and
  combobox `navigate()`) carry back-link state labeled correctly
  ("Shared picks" / "My Picks").

Gates (in `frontend/`): `npm run typecheck`, `npm test`, `npm run lint`.

## Rough size

1 new lib file, 2 new small components, edits to ~8 existing files
(`ElectionPage`, `CandidatePage`, `ElectionCard`, `BallotPage`,
`SavedBallotPage`, `PicksPage`, `FollowedCandidatesSection`,
`PublicPickCardPage`), test updates. No migrations, no API changes, no new
packages.
