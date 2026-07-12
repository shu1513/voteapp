# Houston Local Campaign Finance Plan

Written 2026-07-11 after probing both City of Houston disclosure systems and auditing the post-refactor campaign-finance architecture. The implementation must remain isolated, flag-gated, conservative about identity, and reuse shared finance infrastructure instead of adding another large loader inside `ballotLookup.ts`.

## Implementation status

Implemented on `codex/houston-mayor-finance` through Phase 9. Phase 0 passed against John Whitmire's 2023 race using exact `purpose.csv` report relationships joined to `cand.csv` and `expend_*.csv`, then exact GPAC committee IDs joined to contribution rows. Production code also excludes organization names found in TEC's filer registry so political committees are not mislabeled as industries.

Live validation completed against both city systems:

- Current eFile: Neeloy Azad's 2027 Mayor filing parsed 23 contributions and a `$7,525` cover total.
- Legacy WebForms: John Whitmire report `114213` downloaded by postback and parsed as Houston Mayor, election `2023-11-07`, period `2023-01-01` through `2023-06-30`, with 170 itemized contributions.
- TEC GPACs: exact 2023-cycle matching produced `$1,679,144.71` support and no safely matched opposition, with finance, real estate, construction, oil/gas, and insurance among deterministic organization-donor industries before optional AI fallback.

One source limitation remains deliberate: Houston-local SPAC filings are not included in v1, so outside-spending results are labeled Texas-level GPAC coverage and are not exhaustive.

## 2026-07-12 office expansion

The merged Mayor flow is extended without new finance tables to City Controller and exact-seat City Council races. Live probes established the required identity fields:

- Legacy Controller PDFs identify `Controller` on the cover page.
- Legacy council PDFs preserve seats such as `City Council - District C`.
- Current eFile uses seat-specific codes such as `CCM_AL2`, and its PDF preserves `At Large 2`.
- TEC purpose and candidate-expenditure rows contain explicit Houston Controller and exact council-district descriptions, but also contain generic, wrong-seat, `ASSIST`, and later cross-office rows for the same people.

The expansion therefore carries a normalized office target through every stage. Council sync is allowed only for ballot titles and disclosure rows that resolve to `District A`–`District K` or `At-Large 1`–`At-Large 5`. Generic council labels and mismatched seats are skipped. City-only SPACs remain deferred.

## Goal and v1 scope

Support Houston Mayor elections only:

- Top occupations of people who contributed directly to the candidate.
- Standard direct-contribution size buckets.
- Texas-level GPAC groups that explicitly supported or opposed the candidate.
- Industries behind organization contributors to those GPACs.

V1 deliberately excludes Houston-filed local SPACs. That keeps the outside-spending path on the existing Texas TEC artifact infrastructure, but means outside results are not exhaustive. A later phase may merge city SPAC filings with explicit cross-source deduplication.

## Proven source access

- Legacy City search: `https://cohweb.houstontx.gov/CampaignFinanceWeb/CFRwebsiteSimpleSearch.aspx`
- Official transition page: `https://www.houstontx.gov/campaignfinance/`
- Current eFile configuration: `https://reporting.ethicsefile.com/assets/config/app-config.json`

The legacy WebForms search and report-PDF postback work from backend calls. The current system exposes a public report index and static machine-readable PDFs. The intended two-level reporting UI hostname currently has a certificate mismatch, so ingestion must use the validated API and report hosts from the official configuration rather than navigate through that UI.

## Phase 0: prove TEC GPAC coverage

Before schema or Houston runtime plumbing, use the existing TEC artifact reader to test John Whitmire's Houston mayor race.

The probe must demonstrate all of the following:

1. A TEC expenditure/candidate relationship identifies John Whitmire.
2. Structured context distinguishes Houston Mayor from another office or city.
3. The amount is candidate-specific.
4. Direction is explicitly support or oppose; blank, assist, or inferred direction is rejected.
5. The spender committee ID links to TEC contribution rows.
6. Organization contributors can be separated safely from people.
7. The match does not depend on candidate-name substring matching alone.

If these conditions fail, stop the GPAC implementation. Do not weaken matching rules. Houston direct occupations may still proceed, while local-SPAC PDF parsing becomes the only safe future outside-spending option.

## Phase 1: Houston direct-filing clients and cache

Create `backend/src/pipeline/houstonFinance/` with:

- `houstonLegacyCampaignFinanceClient.ts`
- `houstonEthicsEfileClient.ts`
- `houstonCampaignFinancePdfCache.ts`
- `houstonCampaignFinanceTypes.ts`

Legacy client requirements:

- Maintain the ASP.NET session cookie.
- Parse the required hidden WebForms fields.
- Submit candidate searches and use the result form's actual action URL.
- Download candidate/officeholder report PDFs by report ID.
- Use a long bounded timeout, concurrency one, and one retry because first-time legacy PDF generation is slow.

Current eFile client requirements:

- Fetch the official configuration and select only `cityofhouston`.
- Strictly validate expected HTTPS API and report hosts.
- Fetch the public report index and construct static report PDF URLs.
- Never route through the certificate-broken reporting UI hostname.

Cache requirements:

- Cache by source system and report ID.
- Validate content type, `%PDF-` signature, maximum bytes, and nonempty body.
- Write through a temporary file and atomic rename.
- Treat a successful report-ID PDF as immutable; corrections have new report IDs.
- No separate Houston artifact-refresh scheduler. Sync downloads only missing PDFs.

Use a maintained Node PDF library; do not rely on an operating-system `pdftotext` binary.

## Phase 2: strict direct-finance PDF parser

Add `houstonCampaignFinancePdfParser.ts` and normalize both city systems into one report model containing source, report/filer identity, filer type, office, election date, reporting period, filing timestamp, correction status, source URL, contributions, and expenditures.

Rules:

- Require a recognizable Houston cover page and report identity.
- Require candidate/officeholder filer type for direct finance.
- Parse fixed-form pages using text coordinates grouped by line.
- Parse Schedule A1 monetary contributions.
- Parse Schedule A2 in-kind contributions only when fields are unambiguous.
- Capture amount, contributor identity, and occupation.
- Reject malformed amounts and incomplete rows.
- Do not OCR or guess from scanned pages.
- Bound PDF bytes, pages, and reports per candidate.

Corrections replace their corresponding originals. If the old and new Houston systems contain the same reporting period, select one authoritative report rather than summing both.

Tests should store compact extracted text-item fixtures, not multi-megabyte production PDFs.

## Phase 3: eligibility, resolution, and direct aggregation

Add:

- `houstonFinanceEligibleOffices.ts`
- `houstonCandidateCommitteeResolver.ts`
- `houstonDirectContributionAggregator.ts`

Eligibility requires all of:

- State `TX`.
- District type `place`.
- Houston Census place GEOID `4835000`.
- Office scope `place`.
- Canonical office `Mayor`.

Resolver requirements:

- Exact normalized candidate name.
- Exact Mayor office.
- Exact election year/date from the report cover.
- Exactly one unambiguous filer per source system.
- Stored links become trusted identities on later syncs.

Direct aggregation writes:

- `total_receipts`
- `direct_contribution_total`
- occupation breakdowns
- standard contribution-size buckets

Include positive A1/A2 donor support. Exclude loans, pledges, refunds, and other funds received. Occupations come only from actual individual-donor occupation fields. Do not produce employer or direct-industry output.

## Phase 4: reusable TEC GPAC outside-spending path

Do not duplicate the TEC ZIP cache or CSV reader. Add narrowly scoped reusable functions in `texasFinance` for local-candidate GPAC spending and spender-funder lookup.

Inputs include candidate name, office `Mayor`, place `Houston`, and election year.

Accept outside spending only when:

- The transaction is an independent/direct campaign expenditure.
- Candidate identity exact-normalizes.
- Houston Mayor context is explicit and exact.
- Support or oppose is explicit.
- The amount is candidate-specific.
- A valid spender committee ID exists.

Skip blank direction, assist, unclear office/place, and unsplit multi-candidate rows.

Backtrace contributions by exact spender committee ID. Keep only organization contributors. Never convert an individual's employer into an organization donor. AI classifies only still-unknown organization labels totaling at least $25,000, after deterministic rules and persisted `finance_label_classifications` are checked.

These TEC functions remain in `texasFinance`; Houston orchestration calls them. The existing Texas state-office flow must remain behavior-identical.

## Phase 5: isolated schema, flags, and writer

Use the next free migration number at implementation time. Add:

- `hou_candidate_finance_links`
- `hou_candidate_finance_summaries`
- `hou_candidate_finance_direct_breakdowns`
- `hou_candidate_finance_outside_groups`
- `hou_candidate_finance_outside_group_breakdowns`

Use short `hou_cff_*` constraint names. Links store namespaced city filer identity, raw source filer ID, source system, candidate/election identity, active status, source URL, and verification timestamp.

Add flags defaulting off:

- `HOUSTON_CAMPAIGN_FINANCE_ENABLED`
- `HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED`

Add `houstonFinanceWriter.ts` with transactional snapshot replacement and bulk breakdown writes.

Presence semantics are explicit:

- Source unavailable: preserve prior data.
- Source successfully returns no qualifying data: write zero totals and clear stale details.
- Failed TEC outside retrieval must never erase prior outside data.

Reuse the shared classification table; add no Houston-specific classification cache.

## Phase 6: sync, auto-link, and batch

Add:

- `houstonCandidateFinanceSync.ts`
- `houstonCandidateFinanceAutoLink.ts`
- `houstonCandidateFinanceBatchSync.ts`

Batch flow:

1. Find missing eligible Houston Mayor candidate/elections.
2. Resolve exact city filer identities from Houston reports.
3. Insert only safe links.
4. List due active links.
5. Load/cache Houston reports for direct finance.
6. Load the existing TEC artifact once per batch.
7. Resolve safe GPAC spending and exact committee funders.
8. Classify industries.
9. Write one combined Houston snapshot.

Direct and outside retrieval are independent. One source failing must not block or erase successful data from the other.

Defaults: maximum 10 candidates, daily freshness, two-year lookahead, election day plus one-day grace, legacy concurrency one, and explicit `--force` for historical backfill.

## Phase 7: BullMQ and scripts

Add a Houston-specific scheduler and:

- `syncDueHoustonCandidateFinance.ts`
- `triggerHoustonCandidateFinanceSync.ts`
- `upsertHoustonCandidateFinanceSyncScheduler.ts`
- `runHoustonCandidateFinanceSyncSchedulerWorker.ts`
- `probeHoustonCandidateFinance.ts`

Use worker concurrency one and deterministic linked-election job IDs. Candidate-profile enqueue requires exact Houston Mayor eligibility and enables AI industry classification automatically.

The Houston scheduler consumes the existing TEC artifact. Missing/stale TEC data produces a clear skipped reason and preserves outside data; it does not fail direct Houston sync.

## Phase 8: non-repetitive ballot-loader integration

Reuse the finance refactor:

- `ballotLookupFinanceShared.ts` owns common finance types/helpers and request construction.
- The registry in `ballotLookup.ts` owns source assembly and precedence.
- State/source loaders stay in their family folders.

Do not copy another large Texas-derived loader. Add a small standard-table loader helper under `backend/src/pipeline/finance/` for the common five-table schema, with compile-time table configuration and strict SQL-identifier validation. Houston's family loader supplies only its flag, exact eligibility predicate, table names, source name, and fallback source URL.

Add source `HOUSTON_CAMPAIGN_FINANCE` and register Houston after Texas. Exact office/GEOID eligibility makes their domains disjoint.

Add API-client label:

- `HOUSTON_CAMPAIGN_FINANCE`: `City of Houston / Texas Ethics Commission`

Existing web and mobile finance cards should require no Houston-specific component.

## Phase 9: tests and live validation

Tests cover:

- Legacy WebForms session/postback handling.
- Current eFile configuration and host validation.
- Cache integrity and atomic writes.
- Cover/A1/A2 parsing and correction replacement.
- Old/new Houston overlap deduplication.
- Exact Houston Mayor eligibility and resolver ambiguity.
- Occupation and size aggregation.
- Exact TEC Houston Mayor matching.
- Same-name/different-office and other-city skips.
- Blank/assist/unsplit outside-spending skips.
- Exact GPAC committee funder backtrace.
- Individual-employer exclusion.
- $25,000 AI threshold and classification-cache reuse.
- Writer preserve-versus-clear behavior.
- Scheduler disabled/enabled behavior.
- Non-Houston Texas isolation.
- Existing Texas TEC behavior unchanged.
- Shared ballot/candidate finance output.

Validation gates:

- Backend typecheck.
- Focused Houston and Texas tests.
- Full backend suite.
- Empty-database migration run.
- API-client tests.
- Live John Whitmire smoke for direct occupations and, only if Phase 0 passes, GPAC groups/industries.
- Live current-eFile Houston Mayor candidate smoke.
- Real database-backed election-detail and per-candidate finance API calls.

## Explicit limitation and deferred work

V1 does not claim exhaustive Houston outside spending because city-only SPACs are excluded. A later isolated phase may parse Houston SPAC PDFs, merge them with TEC GPAC evidence, and deduplicate spender/expenditure identities. Do not add that complexity to v1.
