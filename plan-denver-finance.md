# Denver City Campaign Finance Plan

Written 2026-08-12 after live probing of SearchLight Denver (JSON endpoints exercised anonymously via curl and the public dashboard, POST bodies captured from the live site, local DB and codebase audited). Revised same day after an external review round; every adopted correction below was re-verified against the live API before adoption. Verdict: **GO — Phase 0 first.** Schema and any published numbers wait until the Phase 0 gates reconcile to the cent.

Follow the launch checklist in the `voteapp-new-state-finance-checklist` memory (flags in `.env` + `render.yaml`, source label in `packages/api-client/src/format.ts`, this plan doc's naming).

## Verified source (probed live 2026-08-12)

### SearchLight Denver JSON API (undocumented — treat as fragile)

Denver's Clerk & Recorder runs all campaign finance filing through SearchLight (`https://denver.maplight.com`, MapLight-built, launched 2022, data since 2012). All disclosure is filed electronically in SearchLight (per the Clerk's campaign finance handbook) — there is no paper-filing coverage gap of the San Diego kind. The public dashboard is an Angular SPA over an anonymous JSON API: no auth, no key, no CORA request needed. Every endpoint below was exercised live with plain curl.

### Identity model (verified via `api/Filer/filer/658`)

- **`filerId` is the canonical, stable identity** (e.g. Johnston = 658, spans the 2023 and 2027 cycles, carries status/termination). This is the link identity.
- **Committee entity ids are mutable auxiliaries**: the Filer endpoint returns `committeeIds: [641, 807]` for filer 658; transaction rows carry the entity id (`committeeId: 807`), and the filings endpoint queried with `committeeId=807` reports `filerId: 658, entityId: 807`. Store entity ids alongside the link (refreshed from the Filer endpoint), never as the link identity.
- Beware the search endpoint: `getAllCommitteesAndCandidate` returns `id` = filerId (`uniqueId` "com658"/"can658"), while `GetCandidatesByElectionCycle` rows carry both `committeeId` (entity) and `filerId`. Do not call the search `id` a committee id.

### Endpoint inventory

GET endpoints (query params `filerId` + `electionCycleId` unless noted):

| Purpose | Endpoint | Verified example |
|---|---|---|
| Election cycles | `api/Calendar/getElectionCycles` | cycle 26 = 2023 Municipal General, 33 = 2027 Municipal General (2027-04-06), 36 = 2026 City Council Vacancy Election (2026-11-03, active) |
| Candidates per cycle (finance-registration list) | `api/contact/GetCandidatesByElectionCycle?electionCycleId=` | cycle 36 → 12 rows, each with `fullName`, `officeSought`, `district`, `committeeId`, `filerId` |
| Filer record (identity + entity ids) | `api/Filer/filer/{filerId}` | 658 → Active, not terminated, `committeeIds [641, 807]` |
| Committee/candidate/IE search | `api/Committee/getAllCommitteesAndCandidate?search=` | "Advancing Denver" → `{uniqueId:"Ind787", id:787, type:3}` |
| Cycles a filer participated in | `api/Lookup/getElectionCyclesByFiler?filerId=` | 658 → cycles 26, 33 |
| Financial overview (private raised / FEF / IE for/against, one call) | `api/committee/getFinancialOverviewByCandCommittee` | see fixtures |
| Contributions total (**includes FEF**) | `api/Committee/getContributionsTotalByCommittee` | 658/26 → 2,016,263.63 |
| Expenditures total (**includes FEF-funded spending**) | `api/Committee/getExpendituresTotalByCommittee` | 658/26 → 2,014,644.23 |
| FEF subset totals (reconciliation only, never added) | `api/Committee/GetFEFContributionTotalByCommittee`, `getFEFExpendituresTotalByCommittee` | 658/26 → 766,923.75 both |
| Cash on hand (**filer-scoped, NOT cycle-scoped**) | `api/committee/GetCashOnHand?filerId=` | no cycle param — see semantics |
| Top donors (name + total; diagnostic reconciliation only) | `api/Committee/GetSupportingContributorsByCommittee` | 8 rows for 658/33 |
| Outside spenders, support/oppose (aggregated, direction-labeled, **no ids**) | `api/Committee/GetSupportingorOpposingIndependentSpendersByCommittee?...&positionType=1|2` | 658/26 support: Advancing Denver 4,962,415.47 + COLOR Action Fund 33,905.80 + CWA-COPE 13,633.33; oppose: A Better Denver 156,659.16 + CWA-COPE 356.77 |
| Filings list (versioned) | `api/Filing/GetCampaignFilingByCommittee/Committee/?committeeId=` | rows carry `filingPeriodId`, `filingVersion`, `filingStatusName`, `filingId` |

POST `api/Transaction/SearchContributionTransactions` — the transaction-level feed behind the dashboard's Advanced Search. This is where donor **occupation and employer** live (`contributorOccupation`, `contributorEmployer`, plus name, `contributorId`, city/state/zip, street address, `transactionSubType`, `fefTransaction`, `txnPurpose`). Body captured from the live site via XHR intercept:

```json
{"ballotIssue":null,"candidateName":null,"committeeName":null,"committeePosition":null,
 "contributionsFrom":null,"contributionsFromCityStateCode":null,"contributionsToIds":null,
 "electionCycleIds":null,"isBallotIssue":false,"isCandidate":false,"ballotIssueId":null,
 "candidateOfficeSoughtId":null,"transactionFromDate":null,"transactionToDate":null,
 "transactionSubTypeId":null,"pageNum":1,"pageSize":10}
```

POST `api/Transaction/SearchExpenditureTransactions` — same body shape, verified live. Rows carry `transactionSubType`, `independentExpnFlag`, `electioneeringCommFlag`, `fefTransaction`, `committeeId` (entity id).

Filtering by `candidateName` (string) + `electionCycleIds` works and was replayed with curl. `contributionsToIds` with filerId or entity id returned 0 rows — resolve in Phase 0. Until an id-based filter is pinned, the name filter is acceptable **only with hard row filtering**: every returned row's entity id (`recipientCommitteeId` / `committeeId`) must be in the filer's `committeeIds` set from the Filer endpoint (verified: all 562 Johnston expenditure rows carry entity id 807).

Fetch hygiene: allowlist exactly `denver.maplight.com`, HTTPS only; cap response sizes and page counts; contract-test the response shapes and fail closed on drift. The API is undocumented and is the *primary* source, so the contract tests are load-bearing. **Persisted artifacts are sanitized**: strip `address1`/`address2` (and any other street-address field) from every response before it is written to disk, logged, quoted in an error, or pinned in a fixture; record content checksum + fetch timestamp on the sanitized form. Raw bytes live only in memory.

## Verified fixtures (pin these in the Phase 0 probe)

All from Mike Johnston, filerId 658, cycle 26 (2023 mayor), full sweeps (7,978 contribution rows, 562 expenditure rows — one page each at `pageSize` 8000/1000):

1. **Receipts.** `getContributionsTotalByCommittee` = 2,016,263.63 = overview `campaignContributionsToCandidate` 1,249,339.88 + `fairElectionsFundToCandidate` 766,923.75 — **the contributions total already includes FEF**. Transaction-feed subtype breakdown reproduces both parts exactly: Monetary 1,025,908.43 (fefTransaction=false) + Monetary 221,074.00 (fefTransaction=true, qualifying money) + In-Kind 2,357.45 = 1,249,339.88 private; subtype `Fair Elections Payments` = 766,923.75 (4 rows, contributor "Denver Fair Elections Fund Disbursement").
2. **FEF identification is by subtype, never by the boolean.** The 4 city-payment rows have `fefTransaction: false`; the boolean marks donor contributions eligible for matching, not city money.
3. **Disbursements.** `getExpendituresTotalByCommittee` = 2,014,644.23 = transaction rows with `transactionSubType === "Expenditure"` and `independentExpnFlag === false` (1,247,720.48 non-FEF + 766,923.75 FEF-funded). `getFEFExpendituresTotalByCommittee` (766,923.75) is a **subset — adding it double-counts**. Excluded: `Unpaid Obligation` 5,000.00 (2 rows).
4. **The expenditure search header is not candidate spending.** `totalExpendituresAmount` for the candidate-name search = 7,186,614.76 = direct 2,014,644.23 + Unpaid Obligation 5,000 + IE rows 5,166,970.53. Never publish a header total; always post-filter rows.
5. **IE lists sum to the overview.** Support spenders sum to 5,009,954.60 = `independentExpendituresSupportingCandidate`; oppose to 157,015.93 = `independentExpendituresOpposingCandidate`; and the IE rows in the expenditure feed sum to support + oppose = 5,166,970.53.
6. **Negative rows are real**: 208 negative contribution rows (refunds, `txnPurpose` "Overlimit"/"Over limit"), no pointer to the original transaction. API totals already net them.

These reconciliations are internal (totals and transactions come from one system). The probe also runs a **filed-report reconciliation**: sample 2–3 filed report summaries via the filings endpoints (latest `filingVersion` per `filingPeriodId` only — versions are real, the list shows them) and reconcile against the API totals. Filed reports are the authoritative *filed record*, not independent ground truth — they originate in SearchLight too; name the gate accordingly.

## Semantics decisions

- `totalReceipts` = `getContributionsTotalByCommittee` (= private + FEF; fixture 1 cross-checks the composition every sync). `directContributionTotal` = overview `campaignContributionsToCandidate` (private donor money only). A short `direct_coverage_note` discloses that receipts include Fair Elections Fund public matching — one sentence, since a "raised" number silently mixing public matching with donations misleads.
- `totalDisbursements` = `getExpendituresTotalByCommittee`. FEF endpoints are reconciliation splits only — never added to anything.
- Transaction inclusion matrix (fixture-pinned): direct contributions = subtypes `Monetary` + `In-Kind`; FEF city money = subtype `Fair Elections Payments`; direct spending = subtype `Expenditure` with `independentExpnFlag === false`; exclude `Unpaid Obligation`; preserve signed refunds/adjustments.
- Occupation + contribution-size buckets: aggregate from the contribution feed, excluding `Fair Elections Payments` rows, integer cents. Follow the LA precedent exactly (`losAngelesDirectContributionAggregator.ts`): occupation buckets net signed amounts; **size buckets describe gross positive receipts only** (a refund has no pointer to its original receipt, so signed bucketing invents negative-size buckets); contributor counts count positive rows (LA semantics, kept for cross-module consistency; `contributorId` exists if unique-donor counts are ever wanted). Missing occupation is expected below Denver's $50 aggregate-itemization threshold (observed: only 6 of 7,978 rows blank).
- Outside groups: totals and direction from `GetSupportingorOpposingIndependentSpendersByCommittee` positionType 1/2 (server-aggregated, server-resolved targets — no target-matching veto machinery). But rows carry **no id**, and the writer requires a stable one: resolve each spender name via `getAllCommitteesAndCandidate` requiring exactly one type-3 (IE) match, persist the returned `uniqueId` (e.g. `Ind787`); zero or multiple matches fail the candidate closed. Cross-check list sums against the overview IE fields every sync; mismatch fails the candidate closed.
- Cash on hand: `GetCashOnHand` is filer-scoped, cycle-agnostic — wrong for any filer active in more than one cycle (Johnston: 26 and 33). Publish `cash_on_hand = null` unless Phase 0 pins a cycle-correct source (the latest in-force filed report summary is the candidate); cash is **not** a Phase 4 release requirement.
- Top-donor endpoint (`GetSupportingContributorsByCommittee`): diagnostic reconciliation only, not a published surface.
- Contributor street addresses: parse, never persist — see fetch hygiene (sanitized artifacts).

## Prerequisites

1. **Rosters.** Local DB has the district (`Denver city, Colorado`, id `34e810ef-6e89-4d27-8beb-481fc9a8576c`) but **zero** Denver city elections in 2026+. The Nov 3, 2026 City Council Vacancy Election (At-Large Seat B) must be created via the `voteapp-manual-research` skill from **official election evidence** (Clerk & Recorder election pages), not from SearchLight: the SearchLight cycle-36 list (12 entries as of 2026-08-12) is a campaign-finance registration list, not proof of ballot qualification. SearchLight is used only for the finance association after the roster exists. At-large seat = election on the place district with the seat in the ballot title. IE window: Oct 4 – Nov 3, 2026.
2. **coloradoFinance untouched.** Denver municipal filers are not in state TRACER; `coloradoFinanceEligibleOffices` does not widen. Denver gets its own module.
3. **Known registration anomalies (verified live, blocking for auto-link):** "Monica Martinez" appears twice with different filerIds (1322, 1328), both non-terminated; filerIds 1329 and 1330 appear in the cycle-36 candidate list but `getElectionCyclesByFiler` returns `[]` for both. The resolver fails closed on duplicate-name candidates within a cycle and on filers whose cycle list is inconsistent; the probe documents each anomaly.

## Architecture

New module `backend/src/pipeline/denverFinance/`, tables prefixed `denver_` (longest: `denver_candidate_finance_outside_group_breakdowns` = 49 chars, under the 63 limit). Five-table shape mirroring the `sjc_` set; link identity column `filer_id` plus an auxiliary entity-ids column refreshed from the Filer endpoint. New migration number — never renumber. Scope guard: an explicit **cycle allowlist `[36]`** plus the Denver place geoid in the eligible-offices gate (the writer's `minElectionYear` only validates writer input; it is not a roster gate).

**Writer: copy-adapt `sanJoseFinanceWriter.ts`, not the standard factory.** The factory's link upsert overwrites `link_status`/`link_source` on conflict (`standardStateFinanceSnapshotWriter.ts` link upsert) and cannot protect operator links; the SJ writer already implements in-transaction manual-link protection (matching automatic upsert reuses the operator's row; conflicting one errors per-candidate) plus summary/breakdown/outside upserts. Swap `fppc_id` → `filer_id`. Extending the shared factory is deliberately rejected: it touches every state module for one city's benefit.

Reuse **as-is**: `standardStateFinanceBallotLookupLoader`, `standardStateFinanceDueListQuery`, shared name gates (`personNameMiddleEvidence`, `personFirstNameNicknames`, suffix veto).

Copy-adapt templates:

- `washingtonPdcClient.ts` → `denverSearchlightClient.ts` — the JSON-API client pattern (typed fetch, allowlist, contract tests), plus the sanitized-artifact layer above.
- Resolver: much smaller than the CAL/Georgia class — `GetCandidatesByElectionCycle` is an official candidate→committee mapping. Auto-link = roster candidate ↔ SearchLight candidate by normalized name + office/district with the standard middle-name/suffix/nickname gates; filerId follows from the mapping; anomaly rules above fail closed. No committee-name-parsing evidence tier.
- Sync/batch/auto-link/ballot-lookup loader/scripts: mechanical rename of the San José set, minus the workbook machinery. No client-side amendment layer for **transactions** (the feed serves current state — fixture-verified against the overview); **filed-report reconciliation** selects the latest `filingVersion` per `filingPeriodId`.

### Phases (small; single-source module)

- **Phase 0 — probe (no schema, no publication).** `denverSearchlightClient` + `probeDenverCandidateFinance` npm script. Hard gates:
  1. Fixtures 1–6 reproduce to the cent from live responses (receipts composition, FEF-by-subtype, disbursement matrix, header-total rejection, IE sums, signed negatives).
  2. Identity: Filer-endpoint cardinality pinned for every cycle-36 filer (filerId ↔ entity ids); every transaction row's entity id lands in the expected filer's set.
  3. Pagination: two consecutive full sweeps of Johnston cycle 26 are identical (row ids and sums); no duplicate transaction ids within or across pages; first page re-fetched after the final page still matches; max usable `pageSize` pinned (8,000 verified for contributions).
  4. IE spender-id resolution: every cycle-26 Johnston spender name resolves to exactly one type-3 `uniqueId`.
  5. Filed-report reconciliation: 2–3 report summaries (latest version per period) reconcile against API totals; cash-on-hand source decided here.
  6. Registration mapping: every cycle-36 registrant maps to exactly one committee **or** carries an explicit documented anomaly (the Monica Martinez duplicate and the 1329/1330 empty-cycle filers must surface).
  7. PII redaction: sanitizer strips street addresses from artifacts/fixtures; a test proves no persisted probe output contains `address1` content.
  8. `SearchExpenditureTransactions` body + `contributionsToIds` behavior pinned.
- **Phase 1 — schema + writer.** Migration for the five `denver_` tables; SJ-pattern writer (manual-link protection) + writer tests.
- **Phase 2 — resolver + links.** Cycle-candidate mapping resolver, auto-link, anomaly fail-closed rules.
- **Phase 3 — aggregation + sync.** Totals per the semantics section, occupation/size buckets, outside groups with id resolution, `direct_coverage_note` (FEF disclosure), sync + due-list + scheduler, flags, source label, ballot-lookup loader.
- **Phase 4 — live run + UI check.** Full cycle-36 ingest locally; FinanceSummaryCard renders raised/spent/occupations/outside S-O (cash only if Phase 0 pinned a cycle-correct source); standard prod checklist. Prod scheduling stays manual-trigger while `render.yaml` finance crons are commented out (repo-wide state).

### Flags & labels (checklist items, do not skip)

- `DENVER_CAMPAIGN_FINANCE_ENABLED` + `DENVER_CAMPAIGN_FINANCE_SYNC_ENABLED` in `featureFlags.ts` (code defaults `false`), set `true` in `backend/.env` AND documented in `backend/.env.example` (alphabetical), read flag added to `render.yaml`. No third raw-data-refresh flag: unlike San José there is no bulk-artifact refresh pipeline — sanitized response artifacts are written by the sync itself. Deliberate deviation from the SJ trio.
- Source enum `DENVER_CLERK_RECORDER` in `ballotLookupFinanceShared.ts`; display label "Denver Office of the Clerk and Recorder" in `FINANCE_SOURCE_LABELS` (`packages/api-client/src/format.ts`, alphabetical) + `format.test.ts` case.

## Out of scope (v1)

- 2027 Municipal General (Apr 2027, cycle 33) — structural support only; extending the cycle allowlist waits until that race is in scope.
- Ballot-measure committees (cycle 35), pre-2026 elections, per-donor industry classification (runs later via the existing shared classifier), unique-donor contributor counts, individual-vs-organizational category splits (no such category type in the schema), Fair Elections Fund compliance detail beyond the receipts split.
