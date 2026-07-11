# Campaign Finance UI Plan

*Written 2026-07-10. Goal: surface the finance data the backend already
computes — who funds a candidate (occupations, industries) and which outside
groups support/oppose them — on the election page and the candidate profile
page. Not a goal: new backend endpoints, new pipelines, charts/visualizations,
or rendering every field the payload carries.*

## What exists today

**Backend — complete, untouched by this plan.** FEC (federal) plus ~30
state finance pipelines, each gated to that state's qualifying offices.
`GET /api/elections/:election_id` already returns, per candidate, a
`finance_summary` (null when the candidate/office/state has no coverage) with:

- `direct_campaign`: total raised / spent / cash on hand / debts, top donor
  occupations, top employers, top industries, contribution-size buckets
- `outside_spending`: support/oppose totals, top supporting/opposing groups
  (committees), top supporting/opposing industries
- `backing_summary`: prose explanations + per-org evidence
- `source` (FEC / UTAH_DISCLOSURES / …), `cycle`, `last_synced_at`,
  per-row `source_url`s

Full shape: `backend/src/pipeline/address/ballotLookupFinanceShared.ts`.

**Frontend — the gap.**

- `ElectionPage.tsx` renders only `Raised $X` per candidate card.
- `CandidatePage.tsx` renders no finance at all. The candidate detail API
  deliberately excludes it (`candidateDetailReader.ts:32` — election-specific
  finance stays on the election endpoint). That decision stands.
- `packages/api-client` `FinanceSummary` type mirrors only the money totals,
  not the breakdowns.

## Decision

**Frontend-only change.** The election endpoint already delivers everything;
the candidate page fetches it client-side with react-query (already a
dependency — `useFollows` uses it, `apiRequest` exists). No backend work, no
API shape changes, no duplication of ballot-lookup finance logic.

### 1. Types (`packages/api-client/src/types.ts`)

Extend `FinanceSummary` with only the fields the UI renders (the existing
type is already a deliberate partial mirror):

- `FinanceBreakdown` = `{ category_name, amount, contributor_count, source_url }`
- `FinanceOutsideGroup` = `{ committee_id, committee_name, support_oppose, amount, source_url }`
- `direct_campaign` gains `top_occupations: FinanceBreakdown[]`,
  `top_industries: FinanceBreakdown[]`
- `outside_spending` gains `top_supporting_groups` / `top_opposing_groups`
  (`FinanceOutsideGroup[]`) and `top_supporting_industries` /
  `top_opposing_industries` (`FinanceBreakdown[]`)

Skipped deliberately: `top_employers`, `contribution_size_buckets`,
`backing_summary` (verbose prose; occupations+industries already answer
"who supports this candidate"), `fec_candidate_id`. Add when a UI needs them.

Industry `category_name`s arrive as slugs (`oil_gas_energy`). Add a tiny
`formatFinanceCategory` helper in `format.ts` mirroring the backend's
`financeIndustryDisplayName` display map + snake_case fallback. Also add
`financeSourceLabel(source)`: the `source` enum is raw
(`MASSACHUSETTS_OCPF`, `UTAH_DISCLOSURES`) and must never reach the screen
unmapped — map known values to display names ("FEC", "Massachusetts OCPF"),
fall back to title-cased words.

**Zero is data.** `$0` raised is a real disclosure; `null` means not
reported. `formatMoney` already renders `null` as "—"; `hasFinanceContent`
(below) must count `0` as content and only treat `null` as missing.

### 2. Shared component (`frontend/src/components/FinanceSummaryCard.tsx`)

One component, used by both pages. Renders from a `FinanceSummary`:

- **Money row**: Raised / Spent / Cash on hand / Debts (only non-null ones —
  `$0` is a real value and renders), `formatMoney`.
- **Top disclosed occupations of direct donors** and **Industries
  represented among direct contributions**: compact rows `name · $amount`.
  Lists arrive pre-capped and pre-sorted from the backend — render as-is.
  `contributor_count` is deliberately not rendered: state adapters disagree
  on its meaning (Colorado counts contribution rows, Utah counts distinct
  contributors, FEC counts itemized receipts), so any single label
  ("donors", "contributions") would be misinformation for some sources.
  Show it only after the backend guarantees one semantic across loaders.
- **Outside spending** (only when support or oppose total non-null): support
  and oppose columns, green/red accents matching the ballot-measure YES/NO
  pattern; each lists its total, top groups, and top industries. Wording is
  claims-precise: "Outside groups reporting support" / "reporting
  opposition", "Industries funding groups reporting support" / "reporting
  opposition" — these are disclosure reports, and "outside group" (never
  "Super PAC") because state terminology differs. Industry amounts are
  contributions INTO the groups across the cycle, not candidate-specific
  expenditure, so the card carries a one-line note saying exactly that.
- **Footer**: `Source: <financeSourceLabel> · 2026 cycle · synced <date>`
  plus one source link — the first non-null `source_url` across rows — via
  the existing `SourceLine` provenance pattern. No per-row links (noise).

Sections render only when their list is non-empty; the whole card renders
only when `finance_summary` is non-null. Empty-everything summary (nulls +
empty lists) still shows the money row's "—" states? No — if every section
would be empty, render nothing (caller checks a small `hasFinanceContent`
helper exported next to the component).

### 3. Election page

Candidate card today is one big `<Link>`; an expandable region inside an
anchor is invalid HTML and would fight navigation. Restructure minimally:

- Wrapper `<div>` keeps the card border/shadow/hover styles.
- Existing card content stays inside the `<Link>` (unchanged behavior,
  including the `Raised $X` chip and the stopPropagation follow button).
- Below the link, inside the wrapper: `<details>` "Campaign finance" with
  `FinanceSummaryCard` — only when the candidate has renderable finance.
  Native `<details>` = no state management, keyboard-accessible, collapsed
  by default so cards stay scannable.

### 4. Candidate page

New "Campaign finance" section between the header block and "Record":

- **Ongoing elections** = `election_date >= today` (calendar-date string
  compare on the viewer's local date — same YYYY-MM-DD semantics as
  `formatElectionDate`; election day itself counts as ongoing).
- For each ongoing election (typically 0–2): `useQuery(["election", id])` →
  `GET /api/elections/:id` → pick this candidate's `finance_summary` →
  `FinanceSummaryCard` under a sub-heading naming the election + date.
  Candidates whose ongoing election has no finance render nothing — no
  "unavailable" placeholder (matches the user requirement: show only when
  available).
- **Past elections**: no separate list — the existing "Elections" section
  gains a per-row `<details>` "Campaign finance" toggle on past rows. The
  election detail query is `enabled` only after first expand (`onToggle`),
  so nothing is preloaded and there is no arbitrary cap. A fetch that yields
  no finance for this candidate shows "No finance data for this election."
  (an explicit empty state is right here: the user opened it asking).
- Section hidden entirely when the candidate has no ongoing elections.
- Loader/SSR untouched: finance hydrates client-side; crawler-visible
  content (records, elections) is unchanged.

New hook `useElectionFinance(electionId, candidateId, enabled)` in the page
file (not api-client — single consumer; promote later if a second appears).

Known cost, accepted for now: the profile fetches the full election detail
(all candidates + records) to extract one candidate's finance summary,
0–2 times per profile visit, client-side, react-query-cached. State finance
adapters short-circuit for non-matching states, so the overhead is the
records payload the election page already serves. Eager display is the
product requirement, so the fix is not "make it lazy" — it is a narrow
backend endpoint (single candidate/election pair through
`loadCandidateFinanceSummariesByCandidateElection`) as a follow-up.

### 5. Tests

- `frontend/src/test/fixtures.ts`: one full `financeSummaryFixture`
  (occupations, industries, outside groups) reused by both page tests.
- `ElectionPage.test.tsx`: finance `<details>` renders occupations/
  industries/outside groups when present; absent when `finance_summary`
  null; card link still navigates.
- `CandidatePage.test.tsx`: ongoing election → section appears with fetched
  finance (mock `/api/elections/:id`); the fetch selects the summary for
  this exact candidate_id, not an opponent's; only past elections → no eager
  fetch, expanding a past row's finance toggle triggers it; no finance in
  payload → no section; finance fetch failure → rest of the profile renders
  untouched (no error boundary trip, section just absent/quiet).
- `packages/api-client`: `formatFinanceCategory` unit tests (mapped slug,
  unmapped slug fallback) and `financeSourceLabel` (known enum, unknown
  fallback); `hasFinanceContent` treats `0` as content, `null` as missing.

## Non-goals (rejected while planning)

- Compact/detailed dual-mode component: occupations on the card face bloats
  the candidate list; chip + collapsed details covers both needs.
- Restructuring the candidate card to a name-only link: kills the whole-card
  tap target for no gain — the wrapper-div split already keeps interactive
  finance content out of the anchor.
- Fetching only the "nearest" election on the profile: a candidate can be in
  two concurrent races (primary + special); fetch each ongoing one.
- Finance-based candidate sorting: money informs, it must not rank.
- Rendering employers, contribution-size buckets, or backing-summary prose.

## Order of work

1. api-client types + `formatFinanceCategory` / `financeSourceLabel` (+ tests)
2. `FinanceSummaryCard` + `hasFinanceContent`
3. ElectionPage card restructure + tests
4. CandidatePage section + hook + past-row toggles + tests
5. `npm run typecheck` / `lint` / test suites; browser-verify both pages
   against varied real rows: FEC candidate with outside spending, a
   state-source candidate (e.g. Utah/Texas), a candidate with no finance,
   and a candidate with both ongoing and past elections.
