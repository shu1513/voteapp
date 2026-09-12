# Research demand ledger

We will never research every candidate in every election in every district up
front. Instead, every user address lookup counts the gaps on the ballot it
produces, so manual research works what real users are waiting on.

## Two ledgers

- **District queue** — `manual_district_research_requests` (migration 150).
  An address lookup enqueues a district whose elections were never searched
  (or not for 180 days). Claim lifecycle, hottest first:
  `npm run manual:district-research:claim`.
- **Demand ledger** — `manual_research_demand` (migration 279). Once a
  district's elections exist the queue goes quiet, so the same lookup also
  counts every finer gap the ballot shows. Counters only, no claims.

## Stages counted

| stage | target | open when |
|---|---|---|
| `candidate_roster` | election | upcoming office election, no candidate link, no live roster deferral |
| `candidate_profile` | candidate | live candidate on an upcoming ballot with an empty summary |
| `candidate_records` | candidate | same, `last_records_searched_at IS NULL` |
| `ballot_measure` | election | upcoming measure election with no `ballot_measures` row yet, never researched, or without a summary |
| `election_results` | election | past office race with a roster but no won/advanced/runoff row; past measure election with a measure lacking its result |
| `missing_general` | election | primary with no later same-contest election (the `manual:elections:missing-generals` rules, scoped to the ballot's districts) |

Window: 365 days either side of today (`--horizon-days`).

## Triggers

`POST /api/address/resolve` (guests included) and `PUT /api/me/address`, in
`AUTO_DISTRICT_RESEARCH_MODE=manual` (the default). Fire-and-forget; never
touches the address response. Ballot page loads do not count — only address
entry — so reloads cannot inflate a row. No user id is stored.

## Reading it

- `npm run manual:demand:status -- [--stage x] [--state XX] [--limit n]` —
  open gaps hottest first. Gaps are re-derived from live data on every read,
  so a row researched through any path shows as closed without anyone
  updating the ledger.
- `npm run manual:demand:prune` — delete rows whose gap has closed.

Code: `backend/src/pipeline/address/manualResearchDemand.ts`.
