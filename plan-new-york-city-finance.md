# New York City Campaign Finance Plan

*Written 2026-07-11 against merged `origin/main` `4eb16359`. The working tree
was detached at `cf98b2b0`, so all architectural conclusions use the merged
branch, including the completed New York State finance module from migration
168. Official NYC Campaign Finance Board (CFB) sources were probed live on
2026-07-11. This plan is intentionally separate from
`plan-new-york-finance.md`, which covers NYS Board of Elections filings.*

## Implementation status

Implemented on `codex/nyc-cfb-finance`:

- Phase 0 source probe/cache/parser and explicit `not_yet_published` outcome;
- Phase 1 office catalog, migration 177, exact resolver, contribution
  aggregation, transactional snapshots, batch sync;
- Phase 2 composed NY ballot adapter, shared API fields, web/mobile UI,
  CLI, and BullMQ operations;
- Phase 3 independent expenditures remains intentionally deferred: no stable
  machine-readable candidate-target/direction contract was found;
- City Council remains intentionally deferred pending correct 51-district
  geography/address resolution.

Live verification on 2026-07-11: 2025 artifacts downloaded and parsed with 109
eligible analysis candidates (Mayor 69, Public Advocate 13, Comptroller 7,
Borough President 20), zero malformed eligible rows; both expected 2029 files
returned `not_yet_published` without job failure.

## Outcome

Add authoritative campaign-finance summaries and donor breakdowns for these
NYC offices already representable by VoteApp's geography model:

| CFB office | VoteApp office | VoteApp district |
|---|---|---|
| Mayor (`OFFICECD=1`) | `place::Mayor` | New York city place, GEOID `3651000` |
| Public Advocate (`2`) | `place::Public Advocate` | New York city place, GEOID `3651000` |
| Comptroller (`3`) | `place::Comptroller` | New York city place, GEOID `3651000` |
| Borough President (`4`) | `county::Borough President` | Bronx/Kings/New York/Queens/Richmond county |

This is eight races per regular city cycle: three citywide and five borough
president races. City Council (`OFFICECD=5`) is a separate geography project,
not part of the initial finance implementation.

## 1. Findings from the code and data audit

### Existing architecture

- `backend/src/pipeline/newYorkFinance/` is a complete NYSBOE provider module.
  It deliberately excludes NYC offices. It assumes NYS filer/committee links,
  NYS SODA schemas, NYS office names, and no occupation/employer data.
- Migration `168_add_new_york_campaign_finance_tables.sql` created `ny_*`
  tables for that provider. Those tables currently contain zero rows locally,
  but their schema is provider-specific: filer IDs, `ny_soda_api`, NYS totals,
  and NYS breakdown constraints.
- Other providers use their own module, tables, flags, sync job, writer, and
  ballot loader. DC is the closest local-office precedent; bulk-file providers
  provide the artifact-cache precedent.
- Ballot lookup currently registers exactly one state finance adapter per
  state. Adding a second `state: "NY"` entry would violate that assumption and
  could overwrite one provider's summary. NYC and NYS loaders must be composed
  behind the single NY adapter, with warned CFB precedence for impossible
  duplicates.
- The shared finance response supports totals, debts, occupations, optional
  employers, industries, size buckets, and outside spending. The public API
  client intentionally omits employers and size buckets, and web/mobile do not
  render them. It has no public-funds field.

### Office and geography readiness

- The NYC place district already exists: `New York city, New York`, GEOID
  `3651000`.
- The five boroughs map cleanly to existing county districts:
  `36005`, `36047`, `36061`, `36081`, and `36085`.
- `place::Mayor` already exists.
- `place::Public Advocate`, `place::Comptroller`, and
  `county::Borough President` do not exist. `statewide::Comptroller` exists and
  must not be reused for the city office.
- VoteApp has no local council-district type, no 51 NYC council geography rows,
  and no address resolver for them. Putting all council elections on NYC's
  single place district would leak all 51 races onto every NYC ballot.
- No NYC mayor election is currently stored locally. The finance pipeline must
  enrich existing candidate/election records; it must not invent elections or
  candidates. The manual-research workflow remains responsible for rosters.

### Official source quality

Primary source: [CFB Data Library](https://www.nyccfb.info/follow-the-money/data-library/).
It publishes cycle CSVs for contributions, expenditures, public-fund payments,
financial analysis, and intermediaries. Verified 2025 files include:

- `2025_Contributions.csv` (~49.7 MB)
- `2025_Expenditures.csv` (~8.5 MB)
- `2025_Payments.csv`
- `EC2025_FinancialAnalysis.csv`

The files have stable URLs plus Last-Modified metadata and cover all five CFB
offices. Contributions include occupation and employer. The 2025 data contains
large, useful samples for every office: 189,606 mayoral contribution rows,
8,654 Public Advocate rows, 21,500 Comptroller rows, 26,839 Borough President
rows, and 96,676 City Council rows. Among individual mayoral contributions,
97.4% have occupation and 83.2% have employer.

The [CFB Follow the Money portal](https://www.nyccfb.info/FTMSearch/Home/FTMSearch)
uses an internal `FTMSearchWebAPI`. It is useful for manual verification, but
detailed searches require POST requests and anti-CSRF state. Do not build the
production pipeline on this undocumented interface or browser automation.

Official NYC Open Data mirrors exist for
[contributions](https://data.cityofnewyork.us/d/rjkp-yttg),
[expenditures](https://data.cityofnewyork.us/d/qxzj-vkn2),
[financial analysis](https://data.cityofnewyork.us/d/m3tj-a2pb), and
[public funds](https://data.cityofnewyork.us/d/u69g-mvrb). They are useful for
historical backfill and independent cross-checks, but are updated "as needed"
rather than continuously; the verified copies were last updated 2025-12-19
and had no 2029 data. They are not the current-cycle primary source.

The first 2029 disclosure period runs 2026-01-12 through 2026-07-11 and is due
2026-07-15. No 2029 money rows were available on 2026-07-11. That is an
expected future-availability state, not an implementation blocker.

### Correct accounting semantics

- Use the latest complete `FinancialAnalysis` row for authoritative headline
  totals. Do not sum raw transaction files into headline totals; amendments,
  refunds, adjustments, and carry-forward rules make naïve sums wrong.
- Map `net_cntns` to private/direct contributions raised.
- Map `net_expnd` to spent.
- Map `outstanding_bills` to debts owed.
- Store and display `pubfnd_pmt` separately as public funds. Do not add public
  funds into `total_raised`, because that would change the meaning of the
  existing field across providers.
- Leave cash on hand `NULL`. Do not derive it from contributions plus public
  funds minus expenditures; loans and other receipts make that formula unsafe.
- Use raw contribution rows only for occupation, employer, contribution-size,
  and industry breakdowns. Never persist or expose contributor street
  addresses.

## 2. Design decisions

### Separate provider boundary

Create `backend/src/pipeline/newYorkCityFinance/` and `nyc_*` tables. Reusing
the NYS `newYorkFinance/` module or `ny_*` tables would spread source-specific
conditionals through the resolver, writer, scheduler, breakdown constraints,
and API loader. Separate provider modules match the repository's existing
design and make rollback safe.

Use source value `NEW_YORK_CITY_CFB` and separate flags:

```bash
NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED=false
NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED=false
NEW_YORK_CITY_CFB_CACHE_DIR=
```

The existing `NEW_YORK_CAMPAIGN_FINANCE_*` flags continue to control only the
NYSBOE provider.

### Bulk artifact, not per-candidate downloads or a raw-data warehouse

Download each cycle file once into a cache with ETag/Last-Modified metadata,
atomic temporary-file rename, byte/row sanity checks, and a last-known-good
fallback. Stream CSV parsing. During one batch run, build indexes only for
candidate IDs linked to due VoteApp elections. Do not download 50 MB per
candidate, and do not ingest every raw donor row into Postgres.

Phase 1 needs only contributions and financial-analysis artifacts.
Expenditures are unnecessary because the analysis file supplies authoritative
net spending. Payments are unnecessary because it supplies authoritative
public-fund totals. Add either file later only for a concrete product feature.

The cache contains public disclosure data but also addresses. Store it outside
the web root, use restrictive file permissions, never log rows, and apply a
documented retention policy (current cycle plus last completed cycle).

### Conservative identity linking

Use the CFB candidate/recipient ID (`cand_id` / `RECIPID`) as the external key,
not a guessed committee name. Auto-link only when all are true:

1. election cycle matches;
2. CFB office code maps exactly to the VoteApp canonical office;
3. citywide/borough seat matches exactly;
4. normalized candidate name produces exactly one CFB candidate ID.

Zero or multiple matches are skipped and reported. Support a manual link with
an audit source. Never fuzzy-auto-link money records.

Office mapping is strict:

- `1` -> NYC place + Mayor
- `2` -> NYC place + Public Advocate
- `3` -> NYC place + Comptroller
- `4` -> borough code/name -> exact county + Borough President
- `5`, `6`, and `IS` -> ineligible in the initial module

### Minimal API expansion

Add nullable `public_funds_received` under `direct_campaign` across backend,
API-client, web, and mobile. Render a fifth money statistic only when non-null.
Also expose/render the already-produced optional `top_employers` and
`contribution_size_buckets`; this is small contract work and is the main value
of NYC's unusually rich disclosures. Existing providers remain unchanged
because every new field is optional or nullable.

Do not add NYC-specific response objects. Consumers should continue to receive
the shared finance shape.

### NY adapter composition

Replace the one NY registry function with a small composed NY loader:

1. call NYS loader;
2. call NYC loader;
3. merge maps;
4. prefer CFB for impossible-by-design duplicate candidate/election keys and
   emit an operational warning instead of failing the ballot request.

The office allowlists make collisions impossible by design; CFB precedence
keeps corrupt cross-provider links from failing a voter request. FEC retains
its existing final precedence for federal races.

## 3. Implementation phases

### Phase 0 — source contract and freshness gate (read-only, shippable alone)

Build `probeNewYorkCityCfbFinance.ts` and fixture-contract tests before schema
work.

The probe must:

- discover/download the configured cycle's Contributions and Financial
  Analysis files from the official CFB Data Library;
- validate required headers and parse representative rows for office codes
  1-5;
- report ETag/Last-Modified, bytes, row counts, distinct candidate counts,
  office counts, latest included statement number, and malformed-row counts;
- compare headline totals for a sample of candidates against NYC Open Data;
- return a structured `not_yet_published` result when a future-cycle file is
  absent, without throwing or marking a goal blocked;
- fail closed on schema drift, HTML challenge pages, truncation, or implausible
  size/count regression.

Run from the production host after the 2029 filing deadline. Gate production
enablement on CFB publishing the current-cycle machine-readable files within
an acceptable delay. Until then, implementation and tests use a small,
committed, de-identified slice of 2025 rows; flags stay off.

Acceptance:

- live 2025 probe succeeds repeatedly;
- missing 2029 artifact reports `not_yet_published` plus next-check metadata;
- parser contract tests catch renamed/missing columns;
- no database writes.

### Phase 1 — office catalog, artifact reader, direct finance persistence

#### Schema/catalog migration

Add the next numbered migration and matching authoritative seed updates:

- offices: `place::Public Advocate`, `place::Comptroller`,
  `county::Borough President`;
- exact aliases only; do not alias city Comptroller to statewide Comptroller;
- curated office research-area sets in both seed authority and reconciliation
  migration: Public Advocate uses the City Council set plus
  `anti_corruption`; place Comptroller reuses the statewide Comptroller set;
  Borough President reuses the County Executive set. Do not invent another
  policy taxonomy for these offices;
- `nyc_candidate_finance_links` keyed by candidate, election, cycle, and CFB
  candidate ID;
- `nyc_candidate_finance_summaries` with private contributions, expenditures,
  outstanding bills, public funds, source URL, and sync timestamp;
- `nyc_candidate_finance_direct_breakdowns` allowing `occupation`, `employer`,
  `industry`, and `contribution_size`.

Keep the existing active-link uniqueness, foreign-key, updated-at trigger,
amount, non-empty-text, and lookup-index patterns. Do not create outside-
spending tables until that source contract is ready.

#### Provider files

Create focused units under `newYorkCityFinance/`:

- `newYorkCityCfbArtifactCache.ts` — conditional download, metadata, atomic
  replacement, validation, last-known-good behavior;
- `newYorkCityCfbCsv.ts` — typed header mapping and streaming parsers;
- `newYorkCityFinanceEligibleOffices.ts` — the four strict mappings above;
- `newYorkCityCandidateFinanceAutoLink.ts` — exact unique resolver plus manual
  override support;
- `newYorkCityDirectContributionAggregator.ts` — breakdowns only;
- `newYorkCityFinanceWriter.ts` — one transaction per candidate snapshot;
- `newYorkCityCandidateFinanceSync.ts` and batch sync.

Select the latest complete financial-analysis row per
`(cycle, cand_id, office, boro_dist)` by `to_stmt`, then `from_stmt`, with a
deterministic tie-breaker. Reject negative or non-finite headline values rather
than coercing them. CFB publishes statement numbers, not statement dates; do
not create a permanently-null statement-date column.

For contribution breakdowns, implement amendment/refund semantics from CFB
documentation and validate them against published analysis totals before
shipping. Required safeguards: candidate ID/office/cycle match, stable
transaction identity (`REFNO` plus filing context), superseded-row handling,
refund/adjustment handling, and positive net amounts only in displayed
categories. Individual rows feed occupation/employer; organization/employer
labels may feed the existing industry-classification service. Unknown labels
remain unclassified rather than guessed.

Acceptance:

- migration replays from empty DB and upgrades a seeded DB;
- exact 2025 fixtures link every eligible office and reject deliberate
  ambiguity/wrong-borough cases;
- rerunning a snapshot is idempotent and removes stale breakdown rows;
- stored totals match sampled Financial Analysis rows to the cent;
- no address fields appear in tables, logs, or returned objects;
- City Council rows are demonstrably ignored.

### Phase 2 — ballot delivery, UI, operations, guarded rollout

Add:

- NYC feature flags in `backend/src/config/featureFlags.ts`;
- `newYorkCityBallotLookupFinanceLoader.ts`;
- composed NY loader in `ballotLookup.ts` with warned CFB precedence;
- `NEW_YORK_CITY_CFB` to the shared source union and source-label map;
- `public_funds_received`, employers, and size buckets to the API-client
  contract, content detection/source-link helpers, web card, and mobile card;
- sync-due CLI plus BullMQ scheduler/worker/trigger scripts following existing
  provider conventions.

Use a default NYC election lookahead of 1,460 days, not the repository's common
730 days. NYC candidates register and disclose more than two years before the
election; the first 2029 reporting period began in January 2026. Keep the
four-year post-election lookback and seven-day stale threshold so winners' and
losers' periodic post-election filings and audit amendments remain refreshable. Artifact refresh is
once per run/cycle, not once per due candidate.

If the current election/roster is not yet in VoteApp, auto-linking correctly
does nothing. The manual-research deferral table remains the place to record a
future roster recheck; finance synchronization must not report that as a hard
failure.

Rollout order:

1. deploy flags off;
2. run 2025 fixture/integration tests and a current-cycle dry run;
3. enable artifact refresh/sync only;
4. audit link decisions and totals;
5. enable ballot reads;
6. verify one citywide and one borough election end to end on web and mobile.

Acceptance:

- NYS state-office summaries still load unchanged;
- NYC summaries load only for eligible NYC elections;
- impossible duplicate keys warn and resolve to authoritative CFB data;
- public funds are labeled separately from private contributions;
- `NULL` remains "not reported" while numeric zero remains visible;
- disabled NYC flags cause no NYC queries or network calls;
- absent future artifacts produce a deferred/not-yet-published metric, not a
  failed job loop.

### Phase 3 — independent expenditures (separate source gate)

CFB exposes independent-spending profile/summary pages with spender, target,
support/oppose, amount, and funders. The generic expenditure CSV does not
provide a reliable candidate-target/direction contract for these records, so
outside spending must not block direct campaign finance.

First prove a stable, machine-readable, non-browser source. Then add
`nyc_candidate_finance_outside_groups` and breakdown tables using the existing
shared outside-spending shape. Require exact target candidate ID, explicit
support/oppose, exact cycle, and audited deduplication. If no stable source is
available, keep this phase unimplemented rather than scrape brittle HTML.

## 4. Explicitly deferred work

### NYC City Council

Support requires a new local legislative geography capability:

1. add a council-district type without disturbing Census-derived types;
2. import 51 authoritative NYC council polygons/identifiers;
3. resolve an address/point into exactly one current council district;
4. model redistricting/version dates;
5. attach council elections to those districts;
6. only then allow `OFFICECD=5` in the finance module.

That is valuable but materially larger than a finance provider. It should have
its own plan and tests. Until complete, rejecting council rows is correctness,
not missing coverage.

### Other New York cities

NYC CFB data opens Mayor, Public Advocate, Comptroller, Borough President, and
Council only inside New York City. It does not cover Buffalo, Rochester,
Yonkers, Syracuse, Albany, or other place-level offices. Those require their
own municipal/board-of-elections sources and should not be presented as an
extension of this provider.

## 5. Test matrix

- Parser: BOM/case variations, quoted commas/newlines, blank money, bad dates,
  missing headers, duplicate refs, amendments, refunds, malformed rows.
- Mapping: each office code, five borough mappings, NYC place GEOID, wrong
  county, wrong cycle, unsupported office, undeclared/independent spender.
- Linking: exact unique, no match, ambiguous name, manual override, candidate
  withdrawn/lost, no VoteApp roster yet.
- Writer: transaction rollback, idempotency, stale-row deletion, active-link
  uniqueness, decimal precision, statement ordering.
- Loader: flag off, no candidate rows, NYC direct summary, NYS summary,
  duplicate-key warning/CFB precedence, FEC precedence, null-vs-zero semantics.
- UI/API: public funds, employers, size buckets, source label/link, empty
  outside section, responsive web/mobile money grid.
- Operations: 304/not-modified, interrupted download, corrupt replacement,
  last-known-good cache, missing future cycle, schema drift, retryable 5xx,
  non-retryable 4xx, scheduler idempotency.

## 6. Completion definition

The initial project is complete when official current-cycle CFB data flows for
all linked Mayor, Public Advocate, NYC Comptroller, and Borough President
candidates; audited totals and breakdowns appear correctly in ballot lookup on
web and mobile; NYS finance remains unaffected; missing future rosters/files
are deferred rather than treated as blockers; and City Council remains safely
excluded pending its geography project.
