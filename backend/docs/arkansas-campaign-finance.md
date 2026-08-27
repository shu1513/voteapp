# Arkansas campaign finance — feasibility (probed 2026-08-26)

Verdict: **buildable for Nov 2026**. Totals raised/spent YES (two independent
routes), donor occupation YES (structured, ~87% filled — better than NH),
outside-spending stance PARTIAL (free text only; small volume).

## System

- Vendor: **Civix CFIS** — same family as New Mexico and New Hampshire
  (`arcfis-*.azurefd.net` behind Azure Front Door). Launched Sept 2023.
- Public SPA: https://ethics-disclosures.sos.arkansas.gov (Angular).
- API host: **https://api-ethics-disclosures.sos.arkansas.gov/api** — anonymous
  JSON, no auth, no token. Response wrapper `{data, succeeded, error}`.
- **DNS gotcha (verified 2026-08-26):** both hostnames return NXDOMAIN from some
  resolvers (local router DNS and Anthropic's WebFetch both failed) but resolve
  fine via 8.8.8.8. Probes used `curl --resolve` with pinned IPs
  (150.171.109.145 site / .146 api). The adapter should not need pinning on a
  normally-configured host, but if fetches fail with ENOTFOUND, this is why.
- Pre-Sept-2023 filings = legacy PDF archive only
  (https://www.ark.org/sos-filing_search/ + `PublicLegacyFiledReport/*`
  endpoints). New-system transaction data effectively starts filing year 2022
  (small file: late-2022 transactions back-filed into CFIS).

## Key endpoints (verified working, anonymous)

Endpoint constants live in webpack chunk `common.<hash>.js`
(`Y.y` / `m.Hi` map), NOT in main.js — bundle names above verified 2026-08-26.

- **Bulk CSV export** (NH twin):
  `POST /api/ExportData/GetExportPublicDownloadData`
  body `{"type":"CSV","filingYear":<YYYY>,"transactionTypeCode":"TCON"|"TEXP"}`.
  Synchronous CSV response. Verified years 2022–2026.
  2026 TCON = 92 MB / 304,193 rows / $36.3M; TEXP = 6 MB / 20,591 rows / $31.5M.
- **Filer search** (registration + server-computed totals):
  `POST /api/PublicFilerDetails/GetCandidateCommitteDetails`
  body `{"pageNumber":N,"pageSize":N,"filerName":"<text>"}` (the filter key is
  `filerName`; `searchText`/`lastName` are ignored). 10,733 registrations at
  probe. Rows: `filerEntityID` (stable numeric, = CSV "Filing Entity ID"),
  `guid` (registration), `filerType` ("Candidate", PAC, "SFI Filer", IEF...),
  `office`, `officeDistrictName`, `politicalParty`, `electionYear` (cycle),
  `filingYear`, **`totalRaised` / `totalSpent` / `balanceofFunds`**
  (server-computed, cycle-cumulative).
- **Transaction search**:
  `POST /api/PublicTransactionDetails/GetTransactionDetails`
  body `{"filerRegistrationGuid":"<guid>","transactionTypeCode":"TCON"|"TEXP",
  "pageNumber":N,"pageSize":N}` — pageSize 1000 OK. 1,425,693 rows all-time.
  Per row: guid, filerName, transactionAmount (numeric), transactionDate,
  sourceName, **employerName, occupation** (e.g. `Other(Chief Deputy Coroner)`),
  sourceAddress, transactionSource, reportName, transactionSubTypeDesc,
  transactionCategory, `hasChild`, filerRegistrationGuid.
- **Per-registration totals widget**:
  `POST /api/PublicFilerDetails/GetPublicFilerDetails…/GetCombinedFinancialTotalsDetails`
  body `{"filerRegistrationGuid":...}` → interest/loan/monetaryContribution/
  expenditure cumulatives + endBalance. CAUTION: for Sanders this returned
  numbers matching only ~one report period, NOT the cycle — semantics unpinned;
  the search-row totalRaised/totalSpent is the trustworthy cycle figure.
- Other mapped: GetTopDonorsAndPayees, GetContributionsCategoriesDetails,
  GetContributionsInStateAndOutStateDetail, GetPublicLoansAndDebtsDetails,
  GetAmendedSheetHistory, GetFilerDetailsById, PublicLookup/* (office,
  jurisdiction, election-year lookups). `Lookup/ToGetFilingYears` = 401 anon.

## CSV shapes

TCON columns: Filing Entity ID, Entity Name, FilerType, Transaction Type,
Transaction Sub Type, Funding Source / Loan Source Type, Source Name, Source
Address, **Employer Name, Occupation, Occupation Other**, Transaction Date,
Transaction Amount (`"$5,000.00"` strings), Transaction Description,
Transaction ID, Election Type, Election Year, Guarantor Name/Address, Report
Filed Date, Report Name, **Amended** (Y/N).

TEXP columns: ... Payee Type/Name/Address, Transaction Category (+ Others),
Election Type/Year, Report Filed Date, Report Name, Amended.

2026 TCON mix: Non-Itemized Monetary 204,543 / Itemized Monetary 97,303 /
Loan 1,059 / Itemized Nonmoney 845 / Return Contribution ~443.
Filer types: PAC 271,068 rows, Candidate 31,482 (921 distinct candidate
filers), County Party 1,566, IEF 74, Party 3.

## Totals reconciliation (Sanders, Governor, entity 1004)

Search-row cycle totals: raised **$7,870,507.53**, spent **$5,717,191.87**.
CSV sums filing years 2022–2026:

- Raised: monetary itemized 6,900,333.77 + non-itemized 884,346.62 + 2022 file
  85,614.08 ≈ overshoots target by ~$31k ≈ the nonmoney 31,458.43 → server
  "totalRaised" ≈ monetary only (± ~$213 residual). Close, not yet cent-exact.
- Spent: CSV 5,460,018.99 vs target 5,717,191.87 → **−$257,172.88 gap**.
  Cause identified: the TEXP CSV omits rows the search API has — API TEXP row
  count for her registration = 2,909 vs 1,527 CSV rows (`hasChild` parent/child
  credit-card style itemization suspected).

**Design consequence:** headline raised/spent should come from the
registration search row (`totalRaised`/`totalSpent`, server-computed,
cycle-scoped) — like Georgia's cover-arithmetic decision. CSVs/search rows
feed occupation & donor aggregates, not headline totals, until a plan-phase
fixture pins the CSV↔API↔cover reconciliation.

## Donor occupation (verified — shippable)

2026 TCON individual-source rows: 301,059; **occupation filled 261,131 (87%)**
(Occupation dropdown + `Occupation Other` free text; count either),
employer filled 261,628 (87%). Occupation itself is ~50k on the dropdown
column alone — must merge `Occupation` + `Occupation Other`.
Unlike NH, a real occupation chart is shippable; employer→industry pipeline
also usable.

## Outside spending (support/oppose) — PARTIAL

- Filer type "Independent Expenditure Filer" (code IEF) exists; 2026: 182
  expenditure rows, 74 contribution rows — almost all NRA Political Victory
  Fund.
- **No structured stance/target anywhere found**: not in TEXP CSV, not in
  transaction search rows. Stance lives in free text:
  `"SMS MESSAGING IN SUPPORT OF BRANDON ACHOR, AR-SD-13"`.
- No `transactionSearch:"TIE"` switch (NH's IE search) — AR build's
  GetTransactionDetails accepted the guid+type filters; TIE not applicable.
- v1: outside spending **null** (or IEF-aggregate without stance); a curated
  description parse ("IN SUPPORT OF/IN OPPOSITION TO <name>, AR-XX-nn") could
  be a follow-up — the NRA pattern is regular, but sample too small to trust.

## Coverage notes

- Local candidates file here too (Mayor, City Council rows seen) — AR
  counties/cities are in scope, unlike most states' state-only portals.
- Registration rows carry office + district + party → auto-link candidate →
  filerEntityID looks straightforward (office/district match like NH rules).
- Amended: CSV has explicit Amended Y/N (149 of 304k rows in 2026). Whether
  the export contains only-latest or original+amended versions is unpinned —
  cheap fixture (GetAmendedSheetHistory + one amended filer) before shipping
  totals from CSVs.
