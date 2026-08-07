# Georgia campaign finance — implementation plan

Date: 2026-08-06. Governs the Georgia state-finance build. Read alongside
`plan.md` ("Pause point — add new states") and
`docs/finance-module-capability-matrix.md` — their rules apply to every PR
here. Feasibility: agent report dated 2026-08-02, then independently
re-verified live on 2026-08-06 against both source systems (this session;
findings below **supersede** the agent report where they differ). Do not
re-hit the portals during code-only PRs — portal work happens in the
acquisition spike PR, when authorized.

## Verdict

Georgia is born on the shared factories with **zero migration debt** and
**zero new factory capability**. Canonical identity (`committee_id` =
PeachFile numeric `filerEntityId`, `committee_name`), all 5 standard tables,
standard summary columns, std direct categories (`occupation` +
`contribution_size`), donor/industry outside breakdowns. All four product
features are supported by verified live data:

1. Total raised / total spent / cash on hand — official full-cycle totals in
   the PeachFile candidate index (verified: Carr $5,374,711.06 raised,
   $1,167,791.24 cash).
2. Top donor occupations — occupation + employer populated per transaction in
   the search APIs of both systems (verified: Carr → Retired 648, Attorney
   127, …; `Information Requested` is a real placeholder value).
3. Outside support/oppose — structured IE targets with stance, office,
   jurisdiction, district, and a **stable `filerRegistrationGuid` join** to
   the candidate registration (no name matching).
4. Outside-group funders/industries — outside spenders are ordinary filers;
   their itemized contributions come from the same search APIs.

Reference sibling: **tennessee** (HTTP JSON client per linked candidate —
`tennesseeCampClient.ts` shape), NOT ohio (statewide-bulk streaming). Reason
is finding F1 below.

## Two source systems (both required)

| Period | System | API host | Access |
|---|---|---|---|
| 2022 – 2025-06-30 (by report) | EFile archive, frozen 03/06/2026 | `api-recordsearch.ethics.ga.gov` | Search APIs work via plain fetch; bulk-export endpoint 406s for non-browser clients |
| 2025-07-01 onward (by report) | PeachFile, nightly extract | `api-peachfile.ethics.ga.gov` | Search APIs AND bulk export work via plain fetch |
| 2005–2021 legacy | media.ethics.ga.gov | — | Out of scope v1 |

A 2026-cycle candidate has 2025 activity in the archive and 2026 activity in
PeachFile. Verified boundary (Carr): archive transactions end 2025-06-30
(June 30 CCDR); the PeachFile filing-year-2026 data starts 2025-07-01
(2026 Jan 31 CCDR). Partition is **report-level and clean** for the probed
filer — but must be re-verified per filer in the spike (assumption A1).

## Live-verified findings (2026-08-06)

Numbered like Ohio's decisions; **F** = fact probed from real bytes.

- **F1 — statewide bulk contribution CSV has occupation/employer 100%
  blank.** `POST /api/ExportPublicData/GetExportPublicDownloadData`, body
  `{"transactionTypeCode":"TCON","type":"CSV","filingYear":"2026"}` → 74 MB
  CSV; all 119,962 itemized-individual rows have empty
  `Contributor/Person Responsible for Loan Occupation` and `... Employer`
  columns. The columns exist in the data key; the export does not populate
  them. **Therefore the bulk file cannot be the production transaction
  source for the occupation feature.** The per-transaction search API
  populates both fields.
- **F2 — per-filer transaction search API is the production source.**
  `POST /api/PublicTransactionDetails/GetTransactionDetails` (both hosts).
  Filter body includes `transactionTypeCode:"TCON"`, `filerName`, dates,
  election year. Items carry `transactionId`, `filerEntityId`, `filerName`,
  `sourceName`, `payeeOccupation`, `payeeEmployer`, `transactionSubTypeDesc`,
  `transactionStatusCode`, `filerReportId`, `filerReportVersionId`,
  `electionYear`, `electionType`, `transactionAmount`, `reportName`.
  **`pageSize` hard cap 100** — anything larger is rejected by the WAF with
  `{"message":"Potentially harmful payload detected!"}` (F2a). Archive
  returns `totalItems`; PeachFile returns `totalRows: null` on this endpoint
  — page until short page (F2b).
- **F3 — candidate index gives resolver identity AND full-cycle official
  totals.** `POST /api/PublicFilerDetails/GetCandidateDetails`
  (`filerTypeCode:"RC"`). Items carry candidate name parts, `committeeName`,
  `office`/`officeId`, `districtName`/`districtId`, jurisdiction fields,
  `politicalPartyCode`, `electionCycleName`, `filerStatusCode`,
  `filerEntityId`, `filerRegistrationId`, `guid`, and `totalContributions`,
  `totalExpenditures`, `cashOnHand`. Verified the totals are **whole-cycle
  across both systems** (Carr: index $5.37M vs PeachFile-only transaction sum
  $1.80M). These are the summary source and the reconciliation anchor.
- **F4 — filer identity splits across systems, and worse.** Carr has THREE
  filer ids: archive legacy `2750` (his 2022 AG committee — which ALSO
  carries $995k of 2026-cycle rows), archive `757274`
  ("Carr, Christopher Michael"), PeachFile `100035`
  ("Carr for Georgia, Inc."). Archive-757274-2026 + PeachFile = $5.27M vs
  official $5.37M — a $103k residual whose composition is UNPROVEN: the
  legacy filer's 2026-cycle rows total $995k, so at most a slice of them is
  inside the official total (which slice, and whether the residual involves
  them at all, is spike work — A6). A naive per-year split **loses money**,
  and an office-exact filer match can never surface the legacy committee. A
  per-candidate cross-system filer map is mandatory (decision D3).
- **F5 — independent expenditures.**
  `POST /api/PublicIndependentExpenditureDetails/GetIndependentExpenditureDetails`
  (both hosts; PeachFile 551 txns, archive 3,679 as of probe). Each item:
  spender (`filerName`, `filerRegistrationGuid`, committee contact block),
  `amountApplied`, `transactionId`, `filerReportVersionId`, report name, and
  `candidateMeasures[]` — each target with `candidateMeasureTitle` (usually
  the candidate COMMITTEE name), `officeName`/`officeId`, jurisdiction
  fields, `districtName`/`districtId`, `stance` (`Support`/`Oppose`),
  `reasonTypeCode` (`CAN` = candidate; ballot/amendment codes excluded), and
  the target's **`filerRegistrationGuid`** → ID join to the candidate
  registration. Archive targets are weaker: office/jurisdiction often null,
  `candidateMeasureTitle` = "Last, First (Committee Name)".
- **F6 — no per-target amount exists anywhere.** Not in the JSON
  (`candidateMeasures[]` has no amount), not in the bulk CSV (header row
  carries the amount with blank target columns; target rows carry
  stance/target with blank amount, sharing the `Transaction ID`). Probed
  filing-year-2026 file: 387 IE transactions, 338 single-target, **49
  multi-target (~13%), up to 21 targets on one transaction**. → decision D6.
- **F7 — filtered grid download exists but is inferior.**
  `POST /api/PublicGridDownload/DownloadPublicGridData`
  (`publicGridName:"ContributionsPublicGrid"` + the search filter) returns a
  filtered CSV that DOES include occupation/employer — but has no
  transaction id and no amended flag. Use only as a validation cross-check,
  never as the production source.
- **F8 — bulk files still useful for reconciliation.** PeachFile bulk
  TCON/TEXP work via plain fetch and carry `Transaction Id` + `Amended` +
  report names; use them as cycle-level cross-checks of the API-synced rows
  (not as the primary source; F1). Archive bulk export is 406-blocked for
  non-browser clients — archive checks go through its search APIs.
- **F9 — CSV/parser hazards** (for the bulk cross-check path and fixtures):
  cp1252 bytes (0xa0 seen), Excel-guard zips `="30026"`, amounts
  `"$1,000.00"`, **parenthesized amount = returned transaction** (per data
  key), occasional ragged rows (embedded newlines), a handful of stray rows
  with transaction dates 2001–2024 and off-cycle `Election Year` values
  (filter on election year + cycle). Archive dates are ISO
  (`2025-06-30`), PeachFile dates are `MM/DD/YYYY`. Archive occupation
  values are right-padded with spaces — trim.
- **F10 — data keys are versioned schema contracts.**
  `POST /api/PublicFiledReportAndDownload/DownloadFileTemplate/?transactionTypeCode=TCON&fileName=Data%20Key%20Detail`
  → PDF (same for TEXP). Fixture copies retrieved 2026-08-06. "Filing Year"
  on the download page = filing/report cycle, **not** transaction year
  (verified: filing-year-2026 file contains H2-2025 transactions).

## Settled design decisions

1. **D1 — transport**: JSON search APIs on both hosts, per linked candidate
   (TN-style client), `pageSize` 100, polite delay between pages, exact
   fail-closed on non-200/non-JSON. No headless-browser dependency. The
   PeachFile bulk endpoints are a separate, optional reconciliation artifact
   path (F8) cached with manifests per the pause-point rules.
2. **D2 — committee identity**: `committee_id` = PeachFile `filerEntityId`
   (numeric, strict validation + trim), `committee_name` = PeachFile
   committee name. Canonical 5-table schema, no extra link columns.
3. **D3 — cross-system filer identity map**: one Georgia-specific extra
   table (`ga_finance_filer_identity_map`) covering **every filer role we
   join on — candidate committees AND outside spenders** (archive IEs carry
   archive spender ids; PeachFile IEs carry PeachFile ids; the same PAC
   spending in both halves of the cycle must land under one outside-group
   identity). Rows: canonical (PeachFile) filer id ↔ per-system
   `filerEntityId` + registration `guid`, role, cycle, provenance,
   verified-at; multiple source ids per canonical entity (F4: legacy
   committee + candidate filer). Discovery collects **every archive filer
   associated with the candidate by name + cycle as evidence** — office
   match is corroboration, never a discovery filter, because a legacy
   committee registered for a prior race's office (F4: Carr's 2022 AG
   committee) can carry current-cycle money and an office-exact rule could
   never propose it. Inclusion is then decided by reconciling the candidate
   filer set against the official candidate-index totals; a cross-office
   carryover filer enters the map only with manual confirmation. Ambiguous →
   manual review, fail closed. **The map table's migration lands post-spike**
   (its shape depends on A6); the 5 canonical tables don't wait for it.
4. **D4 — summary source**: candidate-index totals (F3) are official and
   full-cycle, and Georgia's "total contributions" includes loans, interest,
   and unitemized money — it is NOT an itemized/direct subtotal. The shared
   loader also displays `direct_contribution_total ?? total_receipts`
   (`standardStateFinanceBallotLookupLoader.ts` total_raised), so writing a
   computed itemized sum there would hide the official number. **v1: write
   index totals to `totalReceipts`/`totalDisbursements`/`cashOnHand`; leave
   `directContributionTotal` NULL.** Itemized/direct sums stay as sync
   diagnostics and reconciliation inputs only — material unexplained
   difference (beyond nightly-extract timing) → keep previous good
   snapshot. Occupation and size breakdowns are unaffected. (Do not add a
   loader preference variant for this — zero new factory capability.)
5. **D5 — occupation rules**: individuals only; filed value only; blanks and
   placeholders (`N/A`, `Unknown`, `Information Requested`, empty) → unknown
   bucket; keep `Retired`/`Student`/`Homemaker`/`Self-employed` as-is;
   aggregate dollars, not row counts; unitemized dollars are never assigned
   to occupation or size buckets. **Returned contributions are excluded from
   breakdowns and counted in a diagnostic** until the spike pins how a
   return links to its original contribution (A9) — the canonical schema
   rejects negative breakdown totals, so subtract-in-place is not safely
   expressible without that linkage.
6. **D6 — IE allocation (release-blocking rule)**: "single target" means
   **exactly one target row on the transaction, of any kind** — a
   transaction with one candidate target plus one ballot target is still
   unallocatable. Single candidate target → full `amountApplied` to it.
   Everything else → quarantine from per-candidate totals + diagnostic +
   `outsideCoverageNote`. Also quarantine: duplicate target GUIDs, mixed
   stances for one target, missing/unresolvable `filerRegistrationGuid`,
   missing stance. Never repeat the full amount per target. Only
   `reasonTypeCode = CAN` targets can receive money. Diagnostics report
   **excluded dollars, not just excluded transaction counts** (the 13%
   multi-target row share understates the money at stake).
7. **D7 — writer config** (`createStandardStateFinanceSnapshotWriter`
   wrapper, maryland/ohio pattern): `label: "Georgia"`,
   `minElectionYear: 2026` — link identity is the PeachFile `filerEntityId`,
   which only exists for PeachFile-era filers, so v1 links are scoped to the
   2026 cycle (also matches product scope). Archive-only entities (2022–2025
   cycles) are out of v1 link scope; revisit identity if older cycles are
   ever wanted. `outsideGroupValidation: "pairing"` (mandatory — cascade-FK
   trap), `summaryUpdatePolicy`: replace summary fields, preserve-when-null
   the two outside totals (direct and outside sync from different API legs),
   `supersededLinkSource: "peachfile_api"` (also the link-source CHECK value
   next to `'manual'`).
8. **D8 — amendments**: select the **latest complete accepted report
   version atomically** (group by filer + report, take the newest
   `filerReportVersionId`, use only its transactions) — never
   per-transaction max-version, which would resurrect transactions deleted
   by a later amendment. Unknown `transactionStatusCode` values (observed:
   `TPEN`, `TFIL`, archive `F`) fail closed: excluded + counted. Spike (A2)
   determines whether the search APIs already return only latest-version
   rows, which would make this a verification instead of a selection. Bulk
   `Amended` flag (`Y` on live rows) is the cross-check.
9. **D9 — eligible offices v1** (DB-grounded at PR time against Georgia 2026
   election rows): statewide constitutional offices (Governor, Lt. Governor,
   Secretary of State, Attorney General, Agriculture/Insurance/Labor
   Commissioners, State School Superintendent, PSC) + State Senate + State
   House. No county/municipal in v1 (local filings are largely uploaded
   documents; IE targets may still name local candidates — those rows only
   flow into IE totals when the target resolves to a covered candidate). No
   federal offices — FEC connector owns them; the Georgia connector must
   refuse US House/Senate rows.
10. **D10 — source labels**: source enum `GEORGIA_ETHICS`;
    `FINANCE_SOURCE_LABELS` display "Georgia Government Transparency and
    Campaign Finance Commission" (shortenable to "Georgia Ethics Commission"
    if the card overflows — decide in the labels PR); generic source URL
    `https://ethics.ga.gov/records-search-all/`.
11. **D11 — flags** (mirroring Ohio's `featureFlags.ts` trio):
    `GEORGIA_CAMPAIGN_FINANCE_ENABLED`,
    `GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED`,
    `GEORGIA_ETHICS_RAW_DATA_REFRESH_ENABLED`. Wire the read flag in
    **`backend/.env.example` AND `render.yaml` in the schema PR** (Ohio
    shipped missing both — pause-point rule). `backend/.env` is gitignored —
    updating the local `.env` is a merge-time runbook action, not a PR
    change.
12. **D12 — coverage notes**: outside note must disclose (a) multi-target IE
    quarantine (D6) and (b) any archive-side gaps found at spike. Direct
    occupation coverage = occupation-covered itemized individual dollars over
    all direct dollars; unitemized stays out of `Unknown`.

## Open spike questions (assumptions to confirm/kill)

- **A1**: report-level partition (archive ≤ Jun 30 2025 / PeachFile ≥ Jul 1
  2025) holds for every filer type, or per-filer exceptions exist (probe ≥10
  filers incl. non-candidate committees).
- **A2**: search APIs return only latest amendment version (D8), and what
  each `transactionStatusCode` value means (`TPEN` pending vs `TFIL` filed —
  does a pending row belong in published totals? default: exclude + count).
- **A3**: `filerName` filter matching semantics (substring? exact?) and
  whether filer-scoped queries are better keyed by another filter field
  (e.g. registration guid) — probe for an id-based filter param.
- **A4**: PeachFile `GetTransactionDetails` result ordering is stable across
  pages under `sortBy: "Transaction Date"` (pagination-drift risk while new
  filings arrive mid-sync).
- **A5**: IE `amountApplied` vs bulk `Transaction Amount` agree per
  transaction; returned/voided IEs representation.
- **A6**: archive candidate index exposes the same
  totals/identity fields as F3 (probe `GetCandidateDetails` on
  api-recordsearch) — settles the filer-map shape (D3) before its
  migration. Must also decompose one full candidate (Carr): which archive
  filers' transactions compose the official full-cycle total, including how
  much of the legacy filer's current-cycle money ($995k on filer 2750)
  belongs in it and what explains the $103k residual (F4).
- **A7**: rate limits / WAF behavior under sustained polite paging from a
  server IP (Render egress), not just a residential IP.
- **A8 — transaction taxonomy gate**: pin the complete observed vocabulary
  from real pages before any aggregator code: `transactionSubTypeDesc`,
  `transactionStatusCode`, `transactionSourceTypeCode` (individual-vs-
  organization discriminator), monetary vs in-kind vs loan vs interest vs
  refund/return meaning, and gross-vs-signed amount behavior per type.
  Anything outside the pinned set → diagnostic counter, never a bucket.
- **A9**: how a returned contribution links to its original (same
  transaction id? separate row? negative amount only?) — gates D5's
  returns handling.

## Module layout

```text
backend/src/pipeline/georgiaFinance/
  index.ts
  georgiaEthicsClient.ts                       # both hosts; search APIs; pageSize<=100; fail-closed
  georgiaEthicsArtifactCache.ts                # optional bulk cross-check artifacts (F8), manifest+SHA-256
  georgiaCandidateCommitteeResolver.ts         # candidate-index resolution (F3), fail closed on ambiguity
  georgiaCandidateFinanceAutoLink.ts           # sibling copy (tennessee/maryland)
  georgiaFilerIdentityMap.ts                   # D3 cross-system map access
  georgiaDirectContributionAggregator.ts       # occupation + contribution_size (D5)
  georgiaOutsideSpendingAggregator.ts          # IE leg (F5/F6/D6)
  georgiaOutsideGroupContributionAggregator.ts # funders of outside spenders (TN pattern)
  georgiaFinanceEligibleOffices.ts             # D9
  georgiaFinanceWriter.ts                      # factory wrapper (D7)
  georgiaCandidateFinanceSync.ts
  georgiaCandidateFinanceBatchSync.ts
  georgiaBallotLookupFinanceLoader.ts          # shared-loader wrapper + coverage notes (D12)
```

Schema: `db/migrations/<next>_add_georgia_campaign_finance_tables.sql` —
clone the maryland/ohio migration, `ga_` prefix (longest identifier
`ga_candidate_finance_outside_group_breakdowns` = 45 chars; use the short
`ga_cff_` constraint-prefix trick where needed). The D3 map table is a
**separate post-spike migration** (shape gated on A6). Allocate the next
free migration number at PR time.

Partial-snapshot contract (writer semantics, pinned here because it is easy
to get wrong): a leg that didn't run passes **`undefined`, never `[]`** —
empty arrays delete the stored rows. Direct-only sync omits the outside
arrays; outside-only sync omits `directBreakdowns`.

## PR sequence (one PR at a time; `npm run typecheck` + `npm test` green in `backend/`; cite matrix row)

Ohio/NC precedent holds: canonical schema is spike-independent (matrix-pinned
shape) and lands first; only the D3 map table waits for the spike.

1. **PR 1 — plan + canonical schema + flags + labels + loader**: this doc;
   migration (5 canonical tables only); `featureFlags.ts` trio (D11);
   `backend/.env.example` + `render.yaml` read flag; `GEORGIA_ETHICS` in
   `FINANCE_SUMMARY_SOURCES` (`ballotLookupFinanceShared.ts`) and
   `FINANCE_SOURCE_LABELS` (`packages/api-client/src/format.ts`); loader
   wrapper + **`ballotLookup.ts` registry entry** (`{ state: "GA", load: … }`
   — the source union alone activates nothing) + characterization test up
   front (born-shared state).
2. **PR 2 — acquisition spike (gated, user-authorized)**: answer A1–A9
   against live systems; commit small redacted fixtures for both hosts
   (candidate index, TCON pages, IE pages incl. one real multi-target,
   data-key PDFs); revise this plan's decisions before any parser code.
3. **PR 3 — map-table migration + client + resolver + auto-link**: D3 map
   migration (shape from A6); `georgiaEthicsClient` (+ fixture tests),
   candidate-index resolver, auto-link; due-list builder config + due-list
   test.
4. **PR 4 — direct finance**: per-filer TCON sync across both systems,
   aggregators (D5, taxonomy from A8), summary from candidate index (D4),
   reconciliation guard; sync + batchSync + npm scripts
   (`georgia-candidates:finance:*`, ohio block as template).
5. **PR 5 — outside spending**: IE leg with D6 allocation rule + coverage
   note; support/oppose totals + groups.
6. **PR 6 — outside-group funders/industries**: TN-style contribution
   aggregation for IE spenders. Sync never calls AI; unresolved labels
   persist as `unknown` classification rows for the manual
   industry/committee-label queues.
7. **PR 7 — scheduler wiring + live run**: scheduler upsert/worker/trigger
   scripts (ohio `ohio-candidates:finance:scheduler:*` as template); then a
   live run through the committed sync scripts — fresh pull, full sync of
   linked Nov-2026 Georgia candidates, money-reconciliation + match-rate
   report against candidate-index totals, and first-run DB verification
   (spot-check written rows against the public search UI).

## Status

- 2026-08-06: plan written from live verification. Revised same day after a
  second-opinion review (D3 scope widened to outside spenders, map table
  moved post-spike, D4 official-totals-to-`total_receipts`, D7 floor 2026,
  D8 report-version-atomic, D6 exact-one-target, A8/A9 added).
- 2026-08-07: **PR 1 implemented** — migration 213 (5 canonical tables,
  NC-212 clone, `ga_` prefix, link sources `manual`/`peachfile_api`), flag
  trio, `.env.example` + `render.yaml`, writer wrapper (D7) + eligible
  offices (D9, DB-grounded: 9 statewide + both chambers; US Senate/us_house/
  county/school excluded) + loader wrapper + `ballotLookup.ts` registry +
  `GEORGIA_ETHICS` source union + `FINANCE_SOURCE_LABELS` label ("Georgia
  Ethics Commission" — D10 short form; the statute name overflows the card)
  + writer/offices/characterization tests. Spike (PR 2) requires user
  authorization before portal access.
