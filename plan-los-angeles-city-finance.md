# Los Angeles City Campaign Finance Plan

*Written 2026-07-11 against `c97ad41a`. Based on a fresh code audit plus live
probes of Los Angeles Ethics Commission and Los Angeles Open Data sources.
Goal: add accurate, cached campaign-finance summaries for Los Angeles city
offices, starting with Mayor, without weakening the existing California
state-office pipeline.*

## 1. Code findings that determine the design

### Existing California finance cannot simply be widened

`backend/src/pipeline/californiaFinance/` is a CAL-ACCESS/California Secretary
of State module. Its office allowlist is intentionally state-only: statewide,
State Senate, and State Assembly. Its committee resolver, raw ZIP cache,
Power Search outside-spending client, identifiers, source labels, and database
tables all encode that authority.

Los Angeles filings come from the City Ethics Commission and Los Angeles Open
Data. They use Los Angeles election IDs, candidate person IDs, FPPC committee
IDs, and a separate internal expenditure `cmt_id`. Reusing the California
resolver or writing Los Angeles records into `ca_candidate_finance_*` would
make provenance and joins ambiguous. Build a dedicated
`backend/src/pipeline/losAngelesCityFinance/` module.

The best local-office precedent is
`backend/src/pipeline/districtOfColumbiaFinance/`: jurisdiction-specific
eligibility, conservative candidate/committee linking, snapshot writes,
batch refresh, scheduler, and ballot loader. Reuse that structure, but not its
source-specific parsing.

### Ballot integration supports another California adapter

`backend/src/pipeline/address/ballotLookup.ts` runs every state-finance adapter
sequentially and merges by `(candidate_id, election_id)`. A second `state:
"CA"` adapter is safe if its loader is strictly gated to Los Angeles local
elections; the existing California loader has no links for those elections.
Put the Los Angeles adapter after the California adapter and add a regression
test proving:

- California state-office rows still come from `CALIFORNIA_SOS`.
- Los Angeles local rows come from a new `LOS_ANGELES_CITY_ETHICS` source.
- No non-Los-Angeles place election can read Los Angeles finance rows.

The shared `BallotLookupFinanceSummary` already represents receipts,
spending, cash, contribution breakdowns, and independent support/oppose. It
does **not** represent matching funds or membership communications. Do not
mislabel either. Phase 1 persists both accurately but leaves them out of the
shared response. `direct_campaign.total_raised` is the official contribution
total, not contributions plus matching funds; `outside_spending` excludes
membership communications. A later frontend/API change can expose explicitly
labeled fields without reingesting data.

### VoteApp jurisdiction and office identity

The existing Los Angeles Mayor ballot fixture establishes the exact city
identity:

- state: `CA`
- district type/scope: `place`
- `geoid_compact`: `0644000`
- district name: `Los Angeles city`
- canonical office: `Mayor`

`place::City Council Member` and `place::Municipal Attorney` already exist.
`City Attorney` is already an alias of `Municipal Attorney`. There is no local
controller office: existing `Comptroller` is statewide and must not be reused.
Before City Controller support, add `place::Municipal Controller`, aliases
`City Controller` and `Municipal Controller`, a concise office summary, and
appropriate research-area mappings.

City Council elections use the parent Los Angeles place district in VoteApp;
the council seat is carried by `official_ballot_title`. Therefore council
linking must require both exact parent GEOID and a conservatively parsed
district number from the title. It must never treat all council candidates in
the city as one contest.

LAUSD is also present in the same Ethics data, but it is not a place office.
It requires an exact `school_unified` district identity and board-district
seat mapping. Keep it as a later expansion, not part of Mayor v1.

### Naming collision

`la_candidate_finance_*` already means **Louisiana**. Los Angeles tables,
queues, job names, and log prefixes must use `lacity_*` / `los_angeles_city_*`.

## 2. Verified source contract and accounting rules

### Official sources

- Los Angeles Ethics election totals and candidate pages:
  <https://ethics.lacity.gov/elections/>
- Election-total fragment used by the official site:
  `https://ethics.lacity.gov/cfcs/Display/DisplayPanels.cfc?method=ElectionTotalsResults&useBS4Tabs=yes&showdates=yes&election_id={id}`
- Contributions Open Data dataset: `m6g2-gc6c`
- Expenditures Open Data dataset: `5mrt-4zhe`
- Campaign statements Open Data dataset: `br3a-db9a`

Live probes found broad coverage: Mayor, City Attorney, City Controller, all
15 City Council districts, all seven LAUSD board districts, and ballot
measures. This unlocks several offices, but only within Los Angeles City and
LAUSD—not arbitrary California municipalities.

### Required accounting rules

These rules need fixture tests before any database write:

1. Treat the Ethics election-total page as the authoritative source for
   candidate headline totals. Use line-item datasets for breakdowns and
   reconciliation, not as a substitute for official headline semantics.
2. Schedule I is miscellaneous cash and is not an ordinary contribution.
3. Net loan receipts by subtracting `con_amount_pd_forgiven`; raw loan amount
   alone overstates receipts.
4. Candidate expenditure totals include schedules C, E, F, and H. Exclude
   Schedule G subvendor rows and Schedule D duplication from the headline.
5. Contributions/statements identify committees by FPPC ID. Expenditure
   dataset `cmt_id` is a different Los Angeles internal ID. Never join them as
   though they were the same key. Store both with explicit names.
6. Independent expenditures and membership communications are separate.
   Rows flagged as membership communications must not enter outside support
   or oppose totals.
7. Never infer zero from a failed or incomplete source request. Keep the last
   good snapshot, record the failure, and surface stale timestamps.
8. Every paged Socrata query needs stable ordering, bounded page size,
   maximum-page protection, timeout, retry for 429/5xx, and an optional app
   token. No unbounded citywide download in the scheduled job.
9. Ethics HTML responses are slow in live probes. Parse and cache them only in
   scheduled/background work. Ballot requests remain database-only.
10. Do not depend on the embedded Tableau independent-expenditure workbook;
    its current view is unavailable. Use the Ethics compact/search results
    that back the official site, with saved fixtures and strict parsing.

For the current 2026 Mayor election, the official page exposed independent
support, independent opposition, and membership support as distinct totals.
That separation is a release invariant, not a presentation preference.

## 3. Scope and explicit non-scope

### Phase 1 release scope

Only:

- jurisdiction: exact `CA` + `place` + GEOID `0644000`
- office: `place::Mayor`
- candidate campaign totals and direct-contribution breakdowns
- independent support/oppose totals and spender groups
- matching/public funds stored separately
- no ballot-request-time network calls

### Deferred

- City Attorney and City Controller: Phase 2
- City Council districts 1–15: Phase 3
- LAUSD board districts 1–7: Phase 4, after exact district identity is
  validated in the database
- ballot-measure committees: separate product/data contract; not candidate
  finance
- independent-spender funding industries: defer until notification-funder
  rows are proven deduplicable and cycle-correct
- membership-communication UI: defer unless the shared API and frontend add
  an explicit labeled field
- real-time Form 497 alerts: useful later, but not part of cumulative totals

## 4. Phased implementation

### Phase 0 — executable source contract, no schema and no writes

Create:

- `losAngelesCityEthicsClient.ts`: Ethics election index/totals/independent-
  spending fetchers with timeout, retry, strict parser, and source URLs.
- `losAngelesOpenDataClient.ts`: filtered Socrata client for the three dataset
  IDs, stable keyset ordering where possible, otherwise bounded offset
  paging.
- `probeLosAngelesCityCampaignFinance.ts` and an npm command. It prints
  election/office/candidate IDs, record counts, authoritative totals,
  reconciled line-item totals, response latency, and mismatches. It writes
  nothing.
- Sanitized HTML/JSON fixtures for one current Mayor candidate, one candidate
  with a repaid/forgiven loan, one independent support spender, one oppose
  spender, and one membership communication.

Tests pin all ten accounting rules above, parser failure on changed markup,
pagination/429 behavior, and candidate/committee ID namespaces.

Gate to continue: run from the production host, not only a laptop. Require
repeatable responses, parser success, and reconciled known examples. If one
candidate or historical election is malformed, log/skip it and continue to
the next candidate/election; one bad record must not abort the batch.

### Phase 1 — Mayor end to end, behind flags

#### Database

Add a migration with:

- `lacity_candidate_finance_links`
- `lacity_candidate_finance_summaries`
- `lacity_candidate_finance_direct_breakdowns`
- `lacity_candidate_finance_outside_groups`

Do not add outside-group industry tables yet; there is no validated input for
them. Follow the newer Louisiana/Maine writer invariants rather than the older
California migration:

- composite `(link_id, election_year)` foreign keys
- one active link per `(candidate_id, election_id)` via partial unique index
- link states `active`, `needs_review`, `inactive`
- link sources `manual`, `lacity_ethics`
- transactional full-snapshot replace, including deletion of rows absent
  from the new successful snapshot
- never deactivate or overwrite a manual link during auto-linking

Link columns should include election ID, candidate person ID, FPPC controlled
committee ID/name, and optional internal expenditure committee ID as distinct
fields. Summary columns should include receipts, expenditures, cash,
matching/public funds, outside support, outside oppose, membership support,
membership oppose, source URL, reported-through date, and sync timestamp.

Direct breakdown types: `occupation`, `employer`, `industry`, and
`contribution_size`. Outside group identity: the official spender identifier
plus name, support/oppose, amount, expenditure count, and source URL.

#### Eligibility and linking

Create `losAngelesCityFinanceEligibleOffices.ts`. Phase 1 contains only
`place::Mayor` and additionally requires exact GEOID `0644000`; state + office
alone are insufficient.

Create a resolver and auto-linker that:

1. maps VoteApp election date/stage/title to one Ethics election ID;
2. filters the official election totals to Mayor;
3. exact-normalizes candidate name, with narrowly documented suffix/name-
   order handling;
4. captures candidate person ID and FPPC controlled committee ID from the
   authoritative rows/statements;
5. links only one unambiguous candidate result;
6. returns `needs_review`/skip for zero, multiple, office-mismatched, or
   identifier-conflicting results;
7. supports a protected manual-link escape hatch.

Do not fuzzy-pick the “best” ambiguous candidate. Continue to the next row.

#### Aggregation and sync

- Headline totals: parsed authoritative Ethics election totals.
- Contribution breakdowns: accepted receipt schedules only, net loan rule,
  itemized records only for occupation/employer/industry counts.
- Industry labels: existing deterministic/AI finance classifier service;
  preserve source labels and classifier provenance.
- Outside groups: official independent-expenditure results, explicit
  support/oppose only, membership rows excluded.
- Reconciliation: compare breakdown-source aggregate against headline totals
  with a documented tolerance. A mismatch does not replace authoritative
  totals; it records a warning/metric. Structural errors prevent that
  candidate snapshot only.
- Writer: one transaction per candidate snapshot. A failed fetch/parser never
  erases the previous good snapshot.

#### Runtime integration

Add:

- due-sync batch with per-election shared source fetches to avoid N+1 calls;
- daily BullMQ scheduler and manual trigger/sync-due scripts;
- candidate-profile finance fanout hook gated by exact jurisdiction/office;
- ballot loader returning source `LOS_ANGELES_CITY_ETHICS`;
- feature flags:

```bash
LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED=false
LOS_ANGELES_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED=false
LOS_ANGELES_OPEN_DATA_APP_TOKEN=
```

Master flag gates reads and all sync. `--force` may bypass only the sync
subflag, matching repo convention.

The candidate-profile context currently has district name/type but not
`geoid_compact`. Extend its election query/type with `geoidCompact` and use
that in the fanout gate; do not rely on the mutable string `Los Angeles city`.
The ballot loader already receives `ElectionRow.geoid_compact`, so its request
builder must filter exact GEOID before querying Los Angeles tables. Add
`LOS_ANGELES_CITY_ETHICS` to the shared finance-source union.

Use daily refresh, 730-day lookahead, and a 45-day post-election grace window.
Los Angeles totals continue changing with post-election filings, so the
one-day grace copied by older modules is too short. Keep stale threshold one
day while an election is in that window. No request-time refresh.

#### Phase 1 tests

- source clients/parsers and accounting fixtures
- exact GEOID + office eligibility; near-name city and other California Mayor
  elections rejected
- resolver ambiguity, manual-link preservation, and ID namespace separation
- direct and outside aggregators, including loans/Schedules I, D, G and
  membership exclusions
- atomic writer rollback, stale-child deletion, active-link uniqueness
- batch continues after source/election/candidate failure
- scheduler disabled/master/subflag/force behavior
- candidate-profile fanout exact gate
- ballot loader source, null-vs-zero behavior, source URLs, top-N ordering,
  and no collision with California SOS/FEC loaders
- migration constraints/indexes via the existing migration test helpers

Rollout: migrate with flags off; run probe; run dry sync; enable sync only;
inspect links/totals/reconciliation; then enable reads for Mayor.

### Phase 2 — City Attorney and City Controller

Add `place::Municipal Attorney` to eligibility. Add the new
`place::Municipal Controller` canonical office, aliases, seed, migration, and
research-area mappings before enabling controller finance.

Expand only office mapping and source fixtures. Reuse the Phase 1 client,
schema, writer, sync, scheduler, and ballot loader. Validate each office's
Ethics election ID mapping and at least two candidates before turning reads
on. No new architecture.

Implementation validation on 2026-07-12 pinned the source mapping to Ethics
election `76` for 2026 and `64` for the 2022 citywide cycle. End-to-end live
2026 probes found three City Attorney candidates and two City Controller
candidates; each candidate's Open Data contribution aggregate reconciled
exactly to the Ethics headline, and independent support/oppose requests
succeeded. The live 2022 page additionally parsed four conservatively
linkable City Attorney candidates and five City Controller candidates;
candidates with multiple controlled committee IDs remained skipped as
ambiguous. Historical pages use direct headline cells and may report
matching-fund status as `ACCEPTED`; the parser preserves that status as an
unknown amount (`null`), never a fabricated zero.

### Phase 3 — City Council districts

Add `place::City Council Member`, still requiring parent GEOID `0644000`.
Implement one seat parser that accepts only district numbers 1–15 from
recognized official-title forms. Store normalized `seat_number` on the link
and require exact agreement with the Ethics office/seat. Unknown or
conflicting titles skip to review; they never become citywide matches.

Test all 15 districts, title variants, single-digit collisions (`1` vs `11`),
and candidates with the same name in different seats. Roll out one election
cycle/seat subset first, then all validated seats.

### Phase 4 — LAUSD board districts

First query the deployed database to pin the exact LAUSD `school_unified`
district row/GEOID used by elections. Then add a separate jurisdiction
definition plus board-seat parser for districts 1–7. Reuse source and storage
components, but keep eligibility independent from Los Angeles place offices.
Do not infer LAUSD merely from state, county, or district name text.

## 5. Operational behavior and observability

Each run should report elections discovered, candidates examined, linked,
needs-review, synced, stale retained, parser failures, HTTP failures,
reconciliation mismatches, and per-source latency. Logs must include source
election/candidate IDs, not contributor PII.

Failure scope should be as small as possible:

- malformed row -> skip row, continue page;
- unresolved candidate -> needs review, continue candidate;
- broken election fragment -> retain that election's old snapshots, continue
  next election;
- one source unavailable -> retain old data and continue independent work;
- scheduler run failure -> retry with bounded backoff, never clear data.

Alert on parser-wide zero results for a known active election, repeated
source failures, and large reconciliation drift. “No rows” is valid only
after a successful, structurally valid response.

## 6. Definition of done

Mayor Phase 1 is done when a Los Angeles ballot can read a recently synced,
source-linked summary entirely from PostgreSQL; totals match the official
Ethics candidate page; loan, expenditure, independent-spending, and
membership rules are fixture-pinned; another California mayor cannot leak
into the module; California state-office finance remains unchanged; one bad
candidate/election does not stop the run; and disabling the master flag
removes both reads and writes without deleting stored snapshots.

This is intentionally one reusable Los Angeles pipeline delivered office by
office. It avoids a premature generic “all local finance” abstraction while
leaving a clean path to three citywide offices, 15 council seats, and seven
LAUSD seats from the same official source family.
