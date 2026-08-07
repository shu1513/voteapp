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
PeachFile. **The spike killed the clean-date-partition picture** (results
item 1): PeachFile migrated some filers' pre-cutover reports (with re-keyed
ids and content drift), other filers' pre-cutover reports exist only in the
archive, and the archive kept accepting special-election reports with
periods into late 2025. The boundary is **per registration and per report**,
never a global date — see the spike results for the source-selection rule.

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
   verified-at; multiple source ids per canonical entity mean **the same
   registration chain re-keyed across systems** (A6: archive `757274` ↔
   PeachFile `100035` — one committee, two ids). **A legally separate
   committee is NEVER a source id of the candidate's canonical entity**
   (A6 refuted the earlier F4 reading): Carr's legacy `2750` is its own
   terminated ledger (termination signal = `filerStatusCode: "T"` — never
   the `isTerminated` boolean, which is `false` on this very registration;
   see D8's string-codes-only rule) whose $1,202,308.37 stays out of
   candidate totals —
   Georgia's own official number excludes it, and its only crossing is an
   $8,400 capped transfer that appears as an ordinary itemized contribution
   row. Legacy committees get mapped, if at all, as distinct entities
   (relevant as donors or outside-spender identities), and the map schema
   must make candidate-total inclusion an explicit per-row property, not a
   consequence of sharing a canonical id. Discovery still collects **every
   archive filer associated with the candidate by name + cycle as
   evidence** — office match is corroboration, never a discovery filter,
   because a legacy committee registered for a prior race's office (Carr's
   2022 AG committee) can carry current-cycle rows and an office-exact rule
   could never propose it for the map at all. Inclusion in candidate totals
   is then decided by reconciling the candidate filer set against the
   official candidate-index totals; a cross-office carryover filer enters
   the map only with manual confirmation. Ambiguous → manual review, fail
   closed. **The map table's migration lands post-spike**
   (its shape depends on A6); the 5 canonical tables don't wait for it.
   **Name-form facts pinned at the spike** (results item 3): archive
   transaction search keys candidate filers by the person's display name
   ("Carr, Christopher Michael" — one query returns rows across ALL of the
   person's committees, including the legacy one), and does NOT match
   committee names; PeachFile keys the same search by committee name and
   does not match "Last, First" person names. `filerEntityId` AND
   `transactionId` are both re-keyed between systems (zero shared ids on the
   same report's rows), so cross-system row identity is (date, source,
   amount) at best — another reason report-level, not row-level,
   source selection. Discovery therefore queries **both hosts'
   `GetFilerReport` inventories and candidate indexes**, with per-host name
   forms, and the map stores the per-host names it verified.
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
   **Index semantics proven at the spike** (results item 6): the index total
   is the exact sum of every transaction row the commission holds for the
   candidate's current registration chain — archive-store rows for
   pre-cutover reports + PeachFile-era rows **including in-kind, unitemized,
   and timed-report-pending (`TPEN`) money** — verified to the cent on Carr
   ($3,468,940.96 + $1,802,135.40 + $103,634.70 = $5,374,711.06). CCDR cover
   Line 6 EXCLUDES in-kind (tracked in a parallel column), so covers and the
   index legitimately differ by in-kind-to-date. The reconciliation guard
   compares our synced row sums against the index with a small tolerance:
   PeachFile's migrated copies of pre-cutover reports can drift from the
   archive originals the index reflects (Carr report 37: −$21,675.00 on
   identical row count, including donor re-attributions) — ~0.4% on Carr.
5. **D5 — occupation rules**: individuals only; filed value only; blanks and
   placeholders (`N/A`, `Unknown`, `Information Requested`, empty) → unknown
   bucket; keep `Retired`/`Student`/`Homemaker`/`Self-employed` as-is;
   aggregate dollars, not row counts; unitemized dollars are never assigned
   to occupation or size buckets. **Returned contributions are excluded from
   breakdowns and counted in a diagnostic — now permanent** (A9 answered):
   a return is its own `Return Contribution` transaction with an
   always-negative amount and a free-text reason, with **no structural link
   to the original row** (contributor-name matching is the only join), so
   subtract-in-place is not safely expressible, ever. Totals are unaffected
   (they come from the index, which nets returns itself).
6. **D6 — IE allocation (release-blocking rule)**: "single target" means
   **exactly one target row on the transaction, of any kind** — a
   transaction with one candidate target plus one ballot target is still
   unallocatable. Single candidate target → full `amountApplied` to it.
   Everything else → quarantine from per-candidate totals + diagnostic +
   `outsideCoverageNote`. Also quarantine: duplicate target GUIDs, mixed
   stances for one target, missing/unresolvable `filerRegistrationGuid`,
   missing stance. Never repeat the full amount per target. Only
   `reasonTypeCode = CAN` targets can receive money. Diagnostics report
   **excluded dollars, not just excluded transaction counts** (spike
   refresh: 76 of 551 PeachFile IE transactions are multi-target — 14% — up
   to **65 targets** on one transaction, so the share understates the money
   at stake).
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
8. **D8 — amendments and report-source selection (CONFIRMED + EXTENDED at
   the spike, results items 2 and 4)**:
   - **The transaction search already serves only the latest report
     version's rows** — verified: every row's `filerReportVersionId` equals
     the report's current version, amendment-changed rows are flagged
     `TAMD`, and no prior-version rows appear. Latest-version-atomic is
     therefore a *verification* (assert version consistency per report), not
     a selection algorithm.
   - **Pinned `transactionStatusCode` vocabularies — PER HOST** (the two
     systems use disjoint code sets; a single list would reject one host's
     entire store). PeachFile: `TFIL` (disclosed on a filed CCDR), `TPEN`
     (disclosed on a timed report — Two Business Day etc. — not yet folded
     into a CCDR; **included in official index totals**, so included in
     ours; the store holds each contribution once, never timed+CCDR
     duplicated), `TAMD` (current value of an amended row), `TPAMD`
     (amended while still timed-pending). Archive: `F` (filed — every one
     of the 1,114 archive TCON rows probed) and `A` (amended-current;
     observed once, on an IE row). Anything outside the host's own set
     fails closed: excluded + counted.
   - **Report-source selection across hosts**: build the report inventory
     per registration as the UNION of both hosts' `GetFilerReport` results.
     The cross-host match key is **(registration via the D3 map, normalized
     report family, period start, period end)** — NEVER the raw
     `reportTypeCode` (the hosts use disjoint code sets: archive `103`/`104`
     vs PeachFile `FPCFDR`/`FPTBDR`, so raw-code matching would let the same
     report survive the union twice and double-count its contributions),
     and NEVER report/amendment status (the PeachFile copy of a migrated
     report is often "Amended" while the archive copy stays "Original" —
     status is version state, not identity). The per-host
     `reportTypeCode` → normalized-family mapping is pinned from fixtures
     at PR 3 and fails closed on unknown codes. Where both hosts hold the
     same report, **PeachFile wins** (it carries current amendment state —
     pre-cutover reports get amended in PeachFile after migration);
     archive-only reports sync from the archive. Report NAMES are an open
     vocabulary ("2026 Jul 31 CCDR", "Two Business Day Report", "Two Week
     Prior CCDR", "6 Day Prior CCDR", "Special General CCDR Nov 17", …) —
     never enumerate names in logic. Amendment chains ride
     `hasChild`/`childResults` (per-version GUIDs, per-version PDF paths,
     `reportStatus` "Version N").
   - **Timed-pending rows carry no real `filerReportGuid`, and the two
     endpoints encode the absence DIFFERENTLY**: the TCON endpoint writes
     the zero GUID (`00000000-0000-0000-0000-000000000000`) while the IE
     endpoint writes `null` (both captured in fixtures). Grouping rule:
     when `filerReportGuid` is null OR the zero GUID, group the row by
     `timedFiledReportGuid` (populated on those rows; `filerReportId` is a
     cross-check where present). A null-only check misses the sentinel and
     would collapse every filer's timed-pending money into one pseudo-report
     that matches no inventory entry — and since TPEN money is inside
     official totals (D4), those rows must reconcile, not fail.
   - **String status codes are authoritative; the boolean convenience flags
     are broken upstream and must never carry logic.** Captured in
     fixtures on BOTH hosts: registration 15866 has
     `filerStatusCode: "T"`/`filerStatus: "Terminated"` with
     `isTerminated: false`; reports 13310 (archive) and 38 (PeachFile) have
     `reportStatus: "Amended"`/`reportVersionId: 2` with
     `isAmended: false`. Key on `filerStatusCode`, `reportStatus`,
     `reportVersionId`, and `hasChild`/`childResults` — never on
     `isAmended`/`isTerminated`.
   - Bulk `Amended` flag (`Y` on live rows) stays the cross-check, with the
     caveat that bulk lags: it contains only CCDR-disclosed rows (timed-
     pending money and the newest grace-window filings are API-only).
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

## Acquisition spike results (run 2026-08-07, user-authorized)

~200 paced requests across both hosts (1 in flight, 2s gaps, descriptive
UA), zero blocks/429s — including a 35-page sustained pull. Artifacts +
SHA-256 manifest cached under `scratch/georgia-campaign-finance/`
(gitignored). Redacted fixtures (street addresses blanked; names,
occupations, amounts kept — public-record data our features display) under
`backend/tests/fixtures/georgiaFinance/`, including both data-key PDFs.
Probe sample: Carr (full three-registration decomposition), Jackson,
Burt Jones, 8 more statewide/legislative candidates, AFP / Fair Fight /
American Future PACs; both hosts' report inventories; 4 CCDR cover PDFs;
full PeachFile IE store (551 txns); fresh bulk TCON (274,680 rows) + TEXP
(65,106 rows).

Verdict: **Georgia is feasible and the architecture survives, but the
two-system model is messier than planned** — the schema (migration 213) and
factory config stand unchanged; the client (PR 3/4) gains a per-report
source-selection layer instead of a date partition. Every dollar question
closed exactly:

1. **A1 — the date partition is DEAD; source selection is per registration
   and per report.** PeachFile migrated Carr's registration with FULL
   pre-cutover history (both 2024–25 CCDRs, re-keyed ids); Harper (29
   archive reports), King (28), Dolezal (30), Powell (16) have ZERO
   PeachFile copies; Bottoms's Feb–Jun 2025 CCDR exists ONLY in the
   archive; the archive kept accepting special-election reports with
   periods through Nov 2025 (Fair Fight). Rule: inventory-union per
   registration, PeachFile wins per report where both hosts hold it (D8).
   Terminated pre-cutover registrations (Carr's legacy committee) are
   archive-only.
2. **A2 — answered, better than hoped** (now in D8): search APIs serve
   latest-version rows only; `TFIL`/`TPEN`/`TAMD`/`TPAMD` pinned; TPEN
   money IS in official totals; no timed/CCDR double-listing (verified on
   Jackson: 67 CCDR rows + 6 pending, zero cross-report duplicates);
   amendment chains in `childResults` with per-version PDFs.
3. **A3 — filerName is a case-insensitive substring over the per-host
   display name** (archive: person-format for candidates, committee names
   DON'T match; PeachFile: committee name). Id-based filter params
   (`filerEntityId` etc.) are **silently ignored** — an unknown body field
   returns the full 2.56M-row store, so the client must post-filter rows by
   `filerEntityId` and treat "filter had no effect" as a hard error.
   `sourceName` filters work (substring; used to find the legacy transfer).
4. **A4 — offset pagination is unstable under date-sort ties**: an
   immediate identical refetch of page 3 differed by 1 row in 100; a
   35-page pull produced 39 duplicate ids AND therefore 39 silently missed
   rows. Alternative `sortBy` values ("Transaction ID", "Amount") return
   **0 rows silently** — an invalid sort looks like an empty filer, so the
   client pins `sortBy: "Transaction Date"` and treats empty-on-nonempty-
   filer as failure. Sync rule: page-until-short-page + dedup by
   `transactionId` + per-report row-count/sum reconciliation; slice by date
   windows to bound drift. **Date windows must not become filters that lose
   rows**: the store contains garbage transaction dates on valid rows
   (fixture: IE transaction 257851 dated 2001-04-27 on a 2026 report — the
   F9 stray-2001–2024-dates hazard exists in the API store, not just bulk).
   Window bounds derive from nothing narrower than the filer's full
   plausible range, and every per-filer pull ends with an unbounded
   (no-date-filter) sweep pass whose job is to catch out-of-window rows;
   out-of-range dates land in the row's report group normally (report
   membership comes from `filerReportGuid`/`timedFiledReportGuid`, never
   from the transaction date) plus an impossible-date diagnostic.
   **Residual risk, owned by PR 3**: date filters
   can't subdivide below one day, so a deadline day with more than one page
   of tied rows rides on dedup + reconciliation alone — the client needs a
   tested bounded-retry (re-pull window until the unique-id set is stable
   across two passes) that fails the report closed when it never
   stabilizes.
5. **A5 — `amountApplied` == bulk `Transaction Amount` for all 387
   CCDR-disclosed IE transactions (0 mismatches)**; the API additionally
   carries 164 timed-pending IE txns (TPEN/TPAMD) the bulk lacks (~30%
   in-season) → the IE leg syncs from the API, bulk is cross-check only.
   No negative/voided IEs observed; returned expenditures exist as
   `Return Expenditure` type (146 rows) for the funders leg to exclude.
6. **A6 — Carr fully decomposed, to the cent**: official index
   $5,374,711.06 = archive 757274 store $3,468,940.96 (= cover Line 6
   $3,439,652.67 + in-kind-to-date $29,288.29, both cover-exact) +
   PeachFile CCDR rows $1,802,135.40 + timed-pending 2BD rows $103,634.70
   (49 TPEN rows — the formerly "UNPROVEN residual" of F4, now identified
   exactly). The legacy committee ($1,202,308.37 raised AND spent in its
   own terminated 2026-cycle registration, cover-exact) contributes only an
   $8,400 max-limit transfer INTO the candidate committee — an ordinary
   itemized row already inside the official total. **Legacy committees are
   never summed into candidate totals; Georgia's own official number
   excludes them.** Archive candidate index exposes the same
   identity+totals fields as PeachFile's (per-registration), so the D3 map
   shape holds. Bonus instrument: archive CCDR PDFs download cleanly by
   `filerReportGuid` and their Summary Report page is machine-readable
   structured arithmetic (Lines 2–6 text layer, no OCR) — the per-report
   official decomposition source. PeachFile's public report-PDF download is
   **broken server-side** (the UI's own call 400s; all recovered contracts
   400/500) — nothing in v1 needs it; re-probe at PR 4.
7. **A7 — zero throttling** at ~200 requests/2s pacing incl. bulk pulls
   from a residential IP; Render-egress behavior still unproven (unchanged
   caveat, first live sync measures it). Archive bulk export remains
   406-blocked for non-browser clients; archive search/PDF endpoints are
   fine.
8. **A8 — transaction taxonomy pinned from real bytes, PER HOST**
   (sample-complete, population-checked at the live run; like the D8 status
   codes, EVERY taxonomy field is a disjoint vocabulary per host — a merged
   list would silently break one host's store):
   - **PeachFile**: bulk/API types {Contribution, Loan Received, Return
     Contribution, Interest Earned (Non-Investment Account), Loan Payment,
     Loan Forgiven}; subtypes {Itemized Contribution, Unitemized
     Contribution, In-Kind Contribution, Anonymous Contribution};
     `transactionSourceTypeCode` {TIND individual, TBSN business, null}.
   - **Archive** (from the 1,114-row Carr pull): `transactionTypeCode`
     {CON, description "Contributions" — plural}; subtypes {Monetary
     Itemized, Monetary Non-Itemized, In-Kind, Anonymous};
     `transactionSourceTypeCode` {IND individual (722), null (274),
     OTH (98), COM (20)}.
   - **D5's individuals-only occupation gate keys on the per-host
     individual code — `TIND` on PeachFile, `IND` on the archive.** Pinning
     only the PeachFile codes would silently drop every pre-cutover
     individual contribution from the occupation breakdown with no error
     raised (the outside-pinned-set rule diagnoses-and-excludes), gutting
     D12 occupation coverage for archive money.
   - TEXP types {Expenditure, Independent Expenditure, Return Expenditure}
     (PeachFile bulk). Bulk hazards confirmed on the new files: cp1252,
     parenthesized negatives, `="…"` Excel guards, ragged rows (a stray
     "SELF EMployed" leaks into Transaction Type on 3 rows — ragged-row
     detection stays mandatory). Data-key PDFs committed as fixtures.
   Note: `GetTransactionDetails` ignores `transactionTypeCode:"TEXP"`
   (returns TCON) — candidate expenditure line-items aren't needed for any
   v1 feature; the funders leg uses TCON of IE spenders.
9. **A9 — returns have NO structural linkage** (D5 now permanent):
   `Return Contribution` rows carry their own id, always-negative amount,
   free-text reason ("over allowed contribution limit", "Stop Payment"),
   and no original-row reference.

## Open spike questions — ANSWERED 2026-08-07 (kept for the record)

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
   migration (shape from A6: per-host filerEntityId + registration guid +
   per-host display-name forms, multiple rows per canonical entity);
   `georgiaEthicsClient` (+ fixture tests) implementing the D8
   inventory-union/source-selection layer and the A4 paging rules
   (date-window slicing, id-dedup, pinned sortBy, filter-effectiveness
   check); candidate-index resolver, auto-link; due-list builder config +
   due-list test.
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
  + writer/offices/characterization tests. Merged as PR #568.
- 2026-08-07: **PR 2 acquisition spike run** (user-authorized) — all nine
  questions answered, Carr decomposed to the cent, date-partition model
  replaced by per-report source selection (D8), returns rule made permanent
  (D5), pagination/fail-closed rules pinned (A4), fixtures + data keys
  committed. Schema and factory config unchanged — migration 213 stands.
  Next: PR 3 (map migration + client + resolver + auto-link).
