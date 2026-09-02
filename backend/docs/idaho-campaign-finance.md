# Idaho campaign finance — feasibility (probed 2026-08-26)

Verdict: **buildable for Nov 2026** — totals raised/spent and outside spending
with true Support/Oppose stance. **Donor occupation/employer: NOT available**
(not collected by Idaho at all) — occupation chart must stay null; there is no
employer-industry fallback either.

## Two systems — use the new one

- **Legacy archive**: https://sunshine.sos.idaho.gov (Next.js, API
  `https://canvass.sos.idaho.gov/eng`, open POST JSON). Covers ~2020–2023 only;
  transaction data effectively ends Dec 2023 (1 donation row dated 2024, none
  later). Ignore for Nov 2026.
- **Current system**: https://sunshine.voteidaho.gov — **Civix CFIS, same vendor
  family as New Hampshire (`cfsapi.sos.nh.gov`) and New Mexico.** API host:
  `https://api-sunshine.voteidaho.gov/api` — anonymous JSON, no auth, plain curl
  works (no Akamai UA games needed, unlike NH). Data is current: transaction
  dates through the probe day (2026-08-26), IE rows filed 2026-08-24.

## Bulk CSV export (totals raised / spent)

Same endpoint name as NH — reuse the NH client/cache/reader design:

`POST /api/ExportData/GetExportPublicDownloadData`
Body: `{"type":"CSV","filingYear":<YYYY>,"transactionTypeCode":"TCON"|"TEXP"}`

Verified filing years 2023–2026 (2023 = 38,821 receipt rows, so the new system
back-fills 2023; earlier years live only in the legacy archive).

- **Filing year = transaction-date calendar year** (2024 file: dates
  01/01/2024–12/31/2024). Different from NH, where filing year ≠ txn year.
- TCON 2026 (probe day): 53,049 rows, $24.4M. 2025: 82,656 rows, $17.1M.
  2024: 97,429 rows, $32.2M.
- TCON columns (28, exact list in `idahoCfsCsv.ts`): Filing Entity ID, Filing
  Entity Name, Campaign Name, Registration Type (Candidate / Political
  Committee / Central Committee), Transaction Id, Transaction Type
  (Contribution / Loan Received / Loan Forgiven / Outstanding Loan / Return
  Contribution), Sub Type (Itemized/Unitemized/In-Kind/Interest/Anonymous),
  Contributor Type (Person/Company/Political Committee/Self/Candidate/Central
  Committee), contributor name+address fields, Transaction Date, Transaction
  Amount, Loan Interest Amount, Total Loan Amount, Election Type, Election
  Year, Description, Amended (Y/N), Timed Report Name/Date, Report Name,
  Report Filed Date. (The trailing-space header quirk is on the TEXP file's
  `"Filing Entity Name "`, not on TCON — corrected 2026-09-01.)
- **No occupation and no employer columns anywhere** (CSV and JSON APIs both).
  Idaho Code § 67-6607 requires only name + address for itemized contributors.
  Set occupation chart to null for Idaho; do NOT route through the
  employer-industry pipeline — there is no employer string to classify.
- **`Election Type` / `Election Year` header values are swapped** in the data:
  the `Election Type` column holds the year ("2026") and `Election Year` holds
  the stage ("Primary"/"General"). Map accordingly. ~50% of rows (PAC money)
  have both blank.
- CSV quirks: cp1252 bytes (0xA0 in category strings — not UTF-8); zip codes as
  Excel formulas `="83702"`; ~0.1–0.2% of rows arrive column-shifted (embedded
  quotes) — parse with a real CSV parser, validate `Transaction Amount` matches
  `^\$[\d,.]+$`, quarantine the rest (2025 file: 109 bad of 82k).
- `Amended` is `N` on 100% of rows in all four files — because **the export
  contains exactly the version-1 transactions**. Any contribution edited or
  added through an amended report (`transactionVersionId` > 1 in the search
  API) is absent from the export while the grid total includes it. Verified
  row-for-row 2026-09-01 on four filers (bulk rows == search rows with
  version 1: same Transaction Ids, counts, and cents; e.g. Ackerman 808: bulk
  $1,360 vs grid $1,560, the two missing $100 rows are version 2). Across the
  grid only 89% of registrations reconcile from bulk sums. **Do not use the
  export for totals or breakdowns; use the per-registration search (below).**
  The Phase 0 probe re-checks this and fails if the export ever changes.

## Reconciliation anchor (verified cent-exact)

`POST /api/PublicFilerDetails/GetCandidateDetails`
`{"pageNumber":1,"pageSize":N,"sortBy":null,"sortType":null}` → paged candidate
registrations with `totalRaised`, `totalSpent`, `balanceOfFunds`, office,
district, party, electionYear, filingCycleId, treasurer, status, entityGuid.

- Registration totals are **per filing cycle**, and CSV files are per calendar
  year. Verified: Todd Achilles (filerEntityID 257) 2024-cycle registration
  `totalRaised` $89,667.61 = CSV 2023 ($15,055.00) + 2024 ($74,612.61) sums
  **exactly**. His 2025 rows ($5,900) belong to the next registration (2026 US
  Senate). Same entity ID spans registrations; split cycles via the CSV
  `Campaign Name` / registration lookup, mirroring the NM committee-cycle model.
- `filerName` in the grid body caused a server-side timeout (>30s) — filter
  client-side or paginate; don't pass name filters. `pageSize` 5000 returns
  the whole grid (2,048 rows on 2026-09-01) in one page.
- **Cycle attribution (superseding the note above):** the calendar-year split
  is a coincidence for Achilles. The exact rule is the registration guid:
  every contribution row from the search API carries `filerRegistrationGuid`,
  and Σ rows with that guid == grid `totalRaised` to the cent on every probed
  registration (Achilles 2024: 286 rows, $89,667.61; Achilles 2026; Blad;
  Blanksma; Bruno; Boyle 2024 + 2026; Ackerman). Registrations with returned
  contributions show grid < Σ rows (the state subtracts returns, which the
  search does not serve) — take the grid figure, never recompute it.
- Grid identity fields used by the Phase 1 resolver (all 729 election-year-2026
  rows inspected 2026-09-01): `office` (exact text: "State Senator", "State
  Representative", "County Commissioner", "Clerk", "Assessor", "Coroner",
  "County Treasurer", "Sheriff", "Governor", "State Controller", ...),
  `districtType` ("State" / "Legislative" / "County" / "Judicial" / ...),
  `cityDistrict` ("Statewide", "Legislative District 16", "Ada County"),
  `jurisdiction` ("Idaho State" or the bare county name "Ada"), and `seatZone`
  (House seat "A"/"B"; commissioner district "1"–"3"; null elsewhere).
  `firstName`/`middleName`/`lastName` are always filled; `filerName` is
  "Last, First Middle" and quotes call names ("Bertling, Timothy 'Tim' Paul");
  generational suffixes live in `lastName` ("Myricks II"). `filerTypeCode` is
  `CAN` on every grid row. 5 entities carry two 2026 registrations (two
  Active for the same race in 3 cases, Terminated + Active in 1, two offices
  in 1). `balanceOfFunds` can be negative (Hernandez 2026: −$1,321.99).

## Per-registration transaction search (cracked 2026-09-01 via the SPA)

`POST /api/PublicTransactionDetails/GetContributionsDetails`
```json
{"pageNumber":1,"pageSize":500,"sortBy":"TransactionDate","sortType":"desc",
 "transactionTypeCode":"TCON","filerName":"<First Middle Last>","sourceName":null,
 "transactionAmountMax":null,"transactionAmountMin":null,"sourceTypeCode":null,
 "committeeType":null,"transactionSubTypeCode":null,"electionID":null,
 "reportName":null,"toDate":null,"fromDate":null,"electionType":null,
 "electionYear":null,"filerRegistrationGuid":null}
```
- `filerName` = the registration's `firstName middleName lastName` (grid
  fields; "Todd Baker Achilles" and "Todd Achilles" are different
  registrations of entity 257). Rows for every registration sharing that name
  come back together — filter by `filerRegistrationGuid` locally.
- The body's `filerRegistrationGuid` is **ignored** server-side (returns the
  whole 269,854-row table); `transactionTypeCode` other than `TCON`
  (`TRCON`, `TCON,TRCON`) is also ignored — returned contributions are not
  retrievable here.
- Row fields used: `guid`, `transactionId`, `transactionVersionId`,
  `filerReportId`, `filerReportVersionId`, `filerReportGuid`,
  `filerRegistrationGuid`, `filerEntityId`, `filerName`, `transactionAmount`,
  `transactionDate` (MM/DD/YYYY), `transactionTypeCode` (always `TCON` here),
  `transactionSubTypeCode` (ITMY / NITMY / INKIND / ANYMS / ITR),
  `transactionSourceTypeCode` (TIND person / TBSN company / TPAC / TCAN /
  TSELF / TCENC), `sourceName`, `contributorCity`/`State`, `stateType`
  (INST / OTST), `electionYear` (number, the registration's cycle),
  `electionTypeCode` (PRMELEC / GRNELEC / ...), `reportName`, `timedReport`,
  `filedDate`. Rows on 48-hour reports appear immediately (before the monthly).
- Sibling endpoints seen in the SPA (same body shape, `filerRegistrationGuid`
  honoured on these): `PublicTransactionDetails/GetExpendituresDetails`
  (`transactionTypeCode:"TEXP"`), `PublicFilerDetails/GetFinancialSummaryDetails`
  (`{filerRegistrationGuid, filingStatusCode:"FIL", isLegacy:"false"}` →
  `totalContributions`, `totalLoansReceived`, `totalExpenditures`,
  `endBalance`, `lastFilingDate`, `recentFiledFilePath`),
  `PublicFilerDetails/GetContributionsCategoriesDetails` (amount by source
  type), `PublicFilerDetails/GetContributionsInStateAndOutStateDetail`
  (in/out-of-state + unitemized), `PublicFilerDetails/GetFilerDetailsById`
  (registration profile incl. treasurer, e-mail). Candidate profile deep link:
  `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=<registrationGuid>&tabName=CAN&isLegacy=false`.
- Edge blocks library user agents (Python-urllib → 403); `curl` and
  `Mozilla/5.0` pass. Send the SPA Origin/Referer as NH does.

## Outside spending (support / oppose) — YES, both stances

Two equivalent sources, both current:

1. **TEXP CSV** (34 columns; header `"Filing Entity Name "` has a trailing
   space): Transaction Type `Independent Expenditure/ Electioneering
   Communication`, Sub Type `Independent Expenditure` (3,152 rows FY2026) or
   `Electioneering Communication` (166). Columns: **`Candidate
   Supported/Opposed`** ("Last, First"), `Candidate Office Sought`, `Measure
   Supported/Opposed` (holds the candidate's display name on candidate rows),
   **`Stance`**, `Amount Applied`, Public Distribution Start/End Date, Purpose.
   **Row structure (verified 2026-09-01): one transaction = one parent row
   (blank target, blank stance, blank `Amount Applied`, full `Transaction
   Amount`) + one allocation row per target sharing the same `Transaction Id`
   with the per-target `Amount Applied`.** The 736 "blank stance" rows are the
   parents, not missing data. Σ `Amount Applied` == `Transaction Amount` for
   648 of 735 groups; 87 allocate less (legitimate partial allocation). Never
   sum `Transaction Amount` across allocation rows. EC rows have stance N/A —
   exclude ECs. Being the bulk export, this file is version-1-only too
   (166 FY2026 IE rows in the export are EC rows absent from the API; the API
   has 541 FY2026 rows absent from the export).
2. **IE search API**: `POST
   /api/PublicIndependentExpenditureDetails/GetIndependentExpenditureDetails`
   `{"pageNumber":1,"pageSize":N,"sortBy":null,"sortType":null}` → 9,897 rows
   all-time (2023: 23, 2024: 5,497, 2025: 1,421, 2026: 2,956; `pageSize`
   10000 returns everything in one page). Fields: `candidateMeasure`,
   `officeSought` (null on measure rows), `stance` (**only Support / Oppose —
   ECs never appear**), `amountApplied` (per target), `transactionDate` (ISO),
   `filerName`, `filerRegistrationGuid`, `purpose`, `sourceName`,
   `reportName`, `timedReport`, `transactionTypeCode` (`TIECOM` = registered
   Idaho filer, 7,538; `TEXP` = non-registered filer such as federal PACs,
   2,359, with `isNonRegisteredEntity:true`), and
   **`candidateMeasureFilerRegistrationGuid`** — the target candidate's
   registration guid, filled on 8,738 of 9,522 candidate rows (8,635 match a
   current grid `guid`); the remaining 1,159 rows are name-only targets
   flagged `isCandidateNonRegisteredEntity:true`. `guid` is the parent
   transaction's, shared by its allocation rows; 225 allocation rows are exact
   duplicates and the state counts them. This API is the outside-spending
   source (complete, current, stance-declared); the TEXP export is only a
   cross-check. 2026 candidate-target money: $3,403,243.20 support /
   $849,889.46 oppose.

Targets resolve by registration guid first (99%); name-only rows go through
the existing name matcher, same as NH/Montana.

## Other useful endpoints (probed OK)

- Lookups: `PublicLookup/GetElectionYearLookup`, `GetOfficeSoughtLookup`,
  `GetPublicFilingYear`, etc. (NH `Lookup/GetElectionLookupData` path 404s here;
  Idaho uses the `PublicLookup/*` controller.)
- Committee grid: `PublicFilerDetails/GetCommitteeDetails` (assumed mirror of
  candidate grid; not yet probed).
- Filer profile money: `FilerDashboard/GetPublicFinancialSummary`,
  `PublicFilerDetails/GetFinancialSummaryDetails` — POST body shape not yet
  cracked (guid guesses → 500). Not needed: grid totals + CSV sums suffice.

## v1 shape (revised 2026-09-01; plan in `docs/plans/idaho-finance.md`)

- Adapter `backend/src/pipeline/idahoFinance/` cloning the NH client/parsing
  style but **search-driven, not bulk-driven**: candidate grid → registration
  per linked candidate (guid); contribution search by the registration's name,
  rows filtered by `filerRegistrationGuid`; IE search filtered by
  `candidateMeasureFilerRegistrationGuid`. Headline totals = grid
  `totalRaised` / `totalSpent` / `balanceOfFunds`; breakdowns from the search
  rows (size, source type, in-state); occupation chart forced null.
- Bulk CSV: Phase 0 contract check only (headers, encoding, re-join of split
  records, quarantine of corrupted rows) — never a row source, because it is
  version-1-only. Its per-year artifact cache is not built.
- Cycle attribution = registration guid (grid `electionYear`/`filingCycleId`
  say which cycle a registration is); no filing-year arithmetic.

## Cross-validation vs external feasibility report (verified 2026-08-26)

An independent report (researched 2026-08-03) was checked point-by-point.
Confirmed live:

- Public SPA routes exist: `/public/cf/downloads`, `/public/cf/independent`,
  `/public/cf/contribution`, etc. The Download Data page shows the extract
  timestamp ("as of 08/26/2026 08:59 PM"), links per-year Data Keys, and lists
  transaction years **2020–2026** — but those year links call the SAME
  `ExportData/GetExportPublicDownloadData` API this doc uses. There is no
  separate "official bulk surface" vs "internal API": one backend, so using the
  JSON API directly matches house precedent (NH/NM adapters do the same).
- **Pre-2023 years in the new system are near-empty**: TCON 2020 = 17 rows,
  2021 = 0, 2022 = 28 — vs 97,964 donation rows for 2022 in the legacy archive.
  Migration of old transactions effectively has not happened. Authority split:
  new system 2023+, legacy `canvass.sos.idaho.gov` for 2020–2022 history.
- Statutes (sunshine_laws.pdf, 28 pp): **zero "occupation" occurrences** in the
  whole lawbook (lone "employer" hit is the employee definition). 67-6607 =
  full name + complete address for >$50 aggregate, ≤$50 may be one unitemized
  line. 67-6611 = IE >$100 aggregate per target; statement must name the
  candidate/measure and whether support or opposition, per-target totals, filed
  ≥7 days pre- and 30 days post-primary/general. 67-6606 = nonbusiness entity
  >$1,000/yr must disclose >$500 payors (funder source for non-PAC spenders).
  67-6628 = electioneering communications: spender + its $50+ contributors,
  **no stance field required** — matches the observed `Stance: N/A` EC rows;
  keep ECs out of support/oppose totals but note they carry a donor schedule.
- Disclosure manual: zero occupation/employer; county/city/special-district/
  judicial candidates exempt until **$500** raised/spent (67-6608) — absent
  local filer ≠ $0; PACs exempt until $1,000. 2026 schedule: monthly reports
  due the 10th for candidates on a 2026 ballot.
- Official 2024 IE workbook
  (`archive.sos.idaho.gov/elections/data/finance/2024_legislative_spending_raw.xlsx`,
  linked from the voteidaho.gov spending dashboard) reproduces exactly:
  5,116 rows, 12 columns, Support 3,646 rows / $6,790,523.61, Oppose 1,470 /
  $2,843,904.03, 578 exact-duplicate rows worth $247,317.20 (workbook has no
  txn ID — never dedupe it on visible fields), 123 measure rows (Prop 1 /
  HJR 5) to exclude from candidate totals. Use as a permanent regression
  fixture. One report error: smallest allocation is **$0.01**, not $0.68.
- 48-hour/timed reports do NOT double-count in the bulk export: TCON 2026 has
  53,020 distinct `Transaction Id` over 53,049 rows (4,908 rows carry a Timed
  Report Name) — one canonical row per transaction. Current CSVs DO have
  `Transaction Id` (unlike the 2024 workbook), so production dedupe keys exist.

Report ideas worth adopting: FEC-ID bridge for federal PACs filing Idaho IEs
(FEC ID goes in the filer name per SOS guidance — use only when explicitly
filed), nonbusiness-entity statements as outside-group funder source, and the
$500-threshold coverage state for local offices.
