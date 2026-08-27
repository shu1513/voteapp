# West Virginia campaign finance — feasibility (probed 2026-08-26, cross-validated against independent report)

Verdict: **buildable**; outside spending needs a document (PDF) path, not the structured grid.

| Need | Status |
|---|---|
| Total raised | YES — bulk CSV + API, cent-level rows |
| Total spent | YES — bulk CSV + API |
| Direct donor occupation | YES **via API only** (bulk CSV has employer, no occupation); full 2026 result: 95.4% occupation / 95.9% employer on >$250 single transactions (2,723 of 8,530 individual rows); statutory basis: W. Va. Code §3-8-5a requires occupation + major business affiliation once a donor's **cycle aggregate** exceeds $250 (single-gift >$250 is a proxy; `transactionTotalYTD`>$250 view: 91.1%, field semantics unproven) |
| Outside spending support/oppose | YES **via filed F-7b PDFs**; the structured IE grid is NOT the source of record — see below |

## System

- Public SPA: https://cfrs.wvsos.gov (React). WV SOS CFRS, hosted in AWS GovCloud.
- Public API: **https://cfrs.wvsos.gov/api/Public-Service** — anonymous JSON, no auth.
- **Two client gotchas, both mandatory:**
  1. The host serves an **incomplete TLS chain** (no intermediate; `Verify return code: 21`).
     Anaconda/Node default CA bundles reject it; macOS system curl works. Client
     strategy: try normal system trust first, fall back to a bundled intermediate for
     this exact host (never disable verification), and count fallback uses.
  2. A **browser User-Agent is required**. Default `curl`/`node-fetch` UA → `403 Forbidden`
     from the WAF on every path including `/api`.
- The API is ASP.NET and self-describes: an unknown body member returns
  `Could not find member 'x' on object of type 'GetXRequest'` — useful for probing.

## Bulk CSV downloads (raised / spent)

Three calls:

1. `GET /Common/getAllDataType` → CON (Contributions), EXP (Expenditures), LOAN, DEB,
   REG (Registrations), REPS (Reporting Schedules), FREP (Filed reports).
   `GET /Common/geAllYearForDataDownload` → 2004–2029.
2. `POST /AccessReport/getDataDownloadDataList` body `{"dataType":"CON","year":"2026","pageNumber":1,"pageSize":25}`
   → `{id, dataType, year, s3ReportFilePath}`. One file per (type, year).
3. `POST /AccessReport/getDataDownloadfile/{id}` (no body) → `{fileUrl}`, a **presigned S3
   URL, `X-Amz-Expires=3600`**. Fetch it without the WV UA/CA workarounds.

Files are **regenerated nightly** (2026 contributions file stamped `20260826030002`).
Encoding is **cp1252, not UTF-8** — decoding as UTF-8 throws on smart quotes (eight
`0x92` bytes in the 2026 file). The files are also **malformed beyond encoding**
(verified 2026-08-27): the contributions file has quote characters inside unquoted
fields (line 98: `…,Warren "Dean" Jeffries,…` — strict RFC-4180 parsers fail; lenient
parsing yields correct 12-column rows); the 2026 expenditures file has **428/10,736 rows
with 1–3 extra columns** (382×13, 25×14, 21×15) from unescaped commas in the recipient
name/address; and the **contributions files have the same defect class** (2025 file:
1 row — `Alonzio Perry, II`, a generational-suffix comma in the contributor name). All
bad-width rows keep a valid typed prefix and a recoverable trailing `FiledDate`.
Recovery rule (implemented in `westVirginiaCfrsParsers`): keep the typed prefix and the
trailing fixed columns, keep only the FIRST column of the damaged name/address span as
the counterparty name, discard the ambiguous tail (it mixes in the street address —
never retained), and flag the row `recovered`.
Catalog depth (verified): 81 artifacts — Contributions 2016–2026, Expenditures 2015–2026,
Debts 2017–2026, Loans 2016–2026, Filed reports 2014–2026, Registrations 2006–2026,
Reporting Schedules 2025–2029. Full catalog in one call with body exactly
`{"pageNumber":1,"pageSize":5000}` — a bare `{}` returns zero rows.

Contributions `Contributions_2026_*.csv` (3.1 MB, 18,396 rows at probe):
`RegistrantID, CommitteeName, CandidateName, TransactionType, TransactionCategory,
TransactionDate, TransactionAmount, ContributorPayeeType, ContributorPayeeName,
ContributorAddress, EmployerName, FiledDate`.
2026 to date: **$10,454,673.08 across 573 registrants**, dates 2026-01-01 → 2026-06-30
(through the Q2 report, due 2026-07-07).
Categories: Monetary 17,664 / In-Kind 471 / Other Income 164 / Transfer of Excess Funds 93 / Return 4.
Contributor types: Individual 15,058, Business/Org 1,653, PAC 1,036, Self 388, Candidate 231, Party 30.

Expenditures `Expenditure_2026_*.csv` (2.2 MB, 10,736 rows, **$14,073,952.38**):
`RegistrantID, CommitteeName, CandidateName, TransactionType, ExpenditureType,
ExpenditurePurpose, TransactionDate, TransactionAmount, RecipientType, RecipientName,
RecipientAddress, FiledDate`. **No IE flag, no target, no stance** — the payee column is
the vendor. Outside spending must come from the API (below).

Registrations `Registration_2026_*.csv` (428 rows): RegistrantID, CommitteeName,
CandidateName, CommitteeType, CommitteeSubType, RegistrationDate, CommitteeStatus.
2026: 383 State Candidate, 26 State PAC, 15 Independent Expenditure Committee, 4 Party.
**No office or district column** — do not link candidates from this file; use the
committee API instead.

### Amendment semantics — RESOLVED (Phase 0 probe, 2026-08-27)

**The nightly CSV holds current-version rows only.** Evidence from the live probe
(`npm run west-virginia-candidates:finance:probe`):

- Six committees reconciled CSV↔API, including two 2025 committees with amended
  reports on file (report versions 2 and 3): every one matched **cent-exact and
  multiset-exact** on (date, amount) — zero rows only-in-CSV or only-in-API.
- Version >1 rows are common in the API (2026: 2,114 of 11,506 rows on versions 2–5)
  with **zero** `amendedFlag` rows in 2026 and 47 in 2025 — if the CSV carried
  original+amended side by side, amended committees' CSV row counts would exceed the
  API's; they don't.
- Therefore the CSV is safe to aggregate directly, and the byte-identical duplicate
  rows (2026: 240; 2025: 1,049) are legitimate repeat transactions, not version
  residue.

The API's `amendedFlag`/`reportVersionID`/`transactionID` remain available for
diagnostics; no version-resolution staging layer is needed.

## Donor occupation — API only

The bulk contributions CSV has `EmployerName` **but no occupation column**
(employer filled on 2,974 / 3,135 individual rows over $250 = 95%).

`employerOccupation` exists only on the transaction search endpoint:

`POST /CommitteeTransactions/getAllPublicTransactionDataList`
```json
{"pageNumber":1,"pageSize":5000,"transactionCategory":"CON","orgTypeCode":"101","transactionYear":"2026"}
```
→ 11,506 rows for 2026 (3 pages). Full result: 8,530 individual transactions; 2,723
single transactions >$250, of which **occupation 2,597 (95.4%), employer 2,611 (95.9%)**.
Statute triggers on the donor's cycle aggregate, not single-gift size; the
`transactionTotalYTD`>$250 view gives 2,743 rows / occupation 2,500 (91.1%), but that
field's semantics (YTD vs cycle, per-donor vs per-committee) are unproven.

Values are a controlled vocabulary, not free text ("Construction/Engineering",
"Environmental Services", "General Business", "Unknown"). The shared contract's
`top_occupations` takes free-text names — publish WV's labels verbatim (whitespace
normalization only), excluding blanks and "Unknown". Employer strings separately feed
the existing employer/donor industry-label path (pre-2027 filings only — see §3-8-6a).

### Search-request gotchas (all verified; corrected 2026-08-27)

- `orgTypeCode` = the first three digits of `entityId`: **101** State Candidate,
  **102** State PAC, **104** Independent Expenditure Committee, party committees separate.
- **The selectors are unstable query modes, not filters.** Verified totals: no selector
  → 10,780; `transactionCategory:"CON"` alone → 18,685; `"EXP"` alone → 10,780; `"IE"`
  alone → 29,534; `transactionTypeCode:"CON"` → 29. With `orgTypeCode:"101"` +
  category `CON` → 11,506 rows that include Monetary, In-Kind, loan subtypes (Loans /
  Loan Payment / Loan Forgiveness), Transfers, Other Income, Returns.
- Client rule: always send `orgTypeCode`, then classify every returned row from its own
  category/type/purpose response fields under a pinned mapping — never trust the request
  mode or the (sometimes contradictory) `transactionTypeDesc` label alone.
- Paginate on `totalRecords` — a 5,000-row page 1 is not the result set.
- Other accepted members: `candidateNameAssocation` (sic), `stance`, `orgName`,
  `orgSubTypeCode`, `sourceFullName`, `contributorType`, `transactionBeginDate`,
  `transactionEndDate`, `transactionMinAmount`, `transactionMaxAmount`, `zipcode`,
  `electionYear`, `measure`, `petition`, `sortColumn`, `sortDirection`.
- `pageSize:5000` is served in one response (7 MB). Paginate on `totalRecords`.

## Outside spending (support / oppose) — document pipeline required

**Decisive finding (verified 2026-08-26):** IE reports filed on Form F-7b exist only as
scanned PDFs in the committee document store. They never become structured transactions.

Proof: Citizens for Better Communities (orgID 3981, entityId 1040003981) has **zero**
structured transactions in any category, zero filed reports via
`getAllFiledReportDataListForPublic` — but 3 "Independent Expenditure Report" PDFs in
`POST /Committee/getAllPublicOrgDocumentDataList` (`{"orgID":3981,...}`). Downloaded one:
a 2-page scanned F-7b (image-only, **no text layer** — no `/Font` objects), containing
exactly the fields VoteApp needs, all handwritten/typed on fixed form positions:
candidate "Robert Fluharty", checkbox **In Opposition of Candidate**, payee
"Mainstream Consulting, LLC", $4,000.00, May 7 2026, Primary. Page 2 = purpose-specific
contributor schedule (>$1,000 furthering-the-expenditure funders, occupation + employer
fields) — blank on this filing.

Scale of the document universe: **18 IE committees registered 2025–2026, 111 IE-report
PDFs total** (Mountaineer Conservative Action 29, Mountaineer Freedom Alliance 15,
School Freedom Fund 11, Make Liberty Win 10, …). Small enough for form-fixed extraction
with review; too big to ignore.

PDF retrieval chain (verified):
1. `POST /Committee/getAllPublicOrgDocumentDataList` → `documentID`, `documentType`,
   `documentName`, `receivedDate`, `s3DocName`.
2. `POST https://cfrs.wvsos.gov/api/Common-Service/AmazonCloudFront/getDownloadLinkWithoutCookies`
   body `{"s3FilePath":"<s3DocName>"}` → presigned S3 URL (1-hour expiry).
   (Note different service: **Common-Service**, not Public-Service.)

Statutes (fetched + verified 2026-08-26): §3-8-2(a) — IE >$1,000/calendar-year must
report candidate, support/oppose, amount per candidate. §3-8-2(d)/(e) — 24-hour report
inside 15 days ($5,000 statewide/legislative, $500 county/municipal); 48-hour report
$10,000+. §3-8-2(c) — SOS must publish candidate-by-candidate IE indices for/against;
asking SOS for that index as structured data is worth one email before building OCR.
§3-8-2b — electioneering communications, same candidate+stance disclosure, separate
category (3 ECC committees all-time).

### Structured IE grid — what it actually holds

IE rows: same endpoint, `{"transactionCategory":"IE","orgTypeCode":"104","transactionYear":"<yyyy>"}`.
Stance vocabulary: `GET /CommitteeTransactions/getStanceForPublic` → SU Support (1) / OP Oppose (0).

Per-row fields when populated: `stanceDescription`, `candidateNameAssocation` ("Last, First"),
`ballotMeasureDescription`, `petitionDescription`, `transactionAmount`, `transactionPurpose`
(e.g. "Direct Mail"), `committeeName`, `entityID`, `reportFileName`, `s3ReportFilePath`.

**Coverage (corrected 2026-08-27 — stance capture is filer-specific, not era-specific):**

| Year | IE rows (org 104) | Total | stance / target filled |
|---|---|---|---|
| 2022 | 467 | — | **11** (all West Virginia Strong, Inc.) |
| 2024 | 754 | $11,964,831.91 | 0 |
| 2025 | 50 | $46,668.67 | 0 |
| 2026 | 1 | $897.10 | 1 (WV Strong again: Support, "Criss, Vernon") |

One filer (WV Strong) e-files through the portal form and gets structured stance; paper
F-7b filers — everyone else — bypass the grid entirely.

Interpretation (corrected after the document-store finding): the grid rows are IE
committees' *general expenditure schedules* from quarterly filings; the candidate-specific
support/oppose money lives on F-7b PDFs that bypass the grid entirely. The single
stance-filled 2026 row (West Virginia Strong, "Criss, Vernon", Support, $897.10) is the
rare committee that e-filed through the online form. **The 2026 doc store already holds
111 IE PDFs while the grid holds 1 stance row — do not wait for October; the grid will
not fill in.** Build totals from documents; treat any structured stance rows as a bonus
to reconcile against.

Also present but unprobed: `/CommitteeTransactions/getIndependentExpenditureChartData`.

## Candidate → filer linkage

`POST /Committee/getPublicCandidatesCommitteeDataList` body `{"pageNumber":1,"pageSize":5000}`
returns all 2,967 committees in one call, no filters needed:
`orgID, entityId, orgName, candidateName ("Last, First M."), orgType, orgSubType,
registrationDate, orgAddress, election ("2018 Election"), office, district, party,
orgStatus, registrationYear, stance`.

`entityId` == `RegistrantID` in the bulk CSVs — that is the join key.
Registration-population counts (labels matter): `registrationYear=="2026"` → 262 rows;
**`election=="2026 Election"` → 427 State Candidate committees (353 active)**: House of
Delegates 315, State Senator 77, judicial ~15, Undeclared 21. Both populations include
primary losers, terminated committees, and (in the registration-year view) 2028
pre-candidates — the VoteApp roster, not the registry, decides the November cohort.
Judicial encoding: circuit lives in `office` ("Circuit Court Judge, Circuit 15"),
division in `district` (numeric; Supreme/Intermediate use strings like `"Division 1"`,
`"Division Undeclared"`). `stance` on the committee row is not usable: filled on 1 of 88
IE committees.

## 2026 filing calendar (from the REPS bulk files)

**Cycle-start caution (added 2026-08-27):** the 2025 REPS file shows the "2026 Candidate
/ Committee Election Cycle" already running in **2025 Q3 (begin 2025-07-01)** — the 2026
file's earliest listed period (2025 Q4) is not the cycle start. Never assume a start
date; scope by REPS cycle membership and harvest 2024–2026 artifact years.

2025 Q4 due 2026-01-07 · Q1 due 2026-04-07 · Primary (04-01→04-26) due 2026-05-01 ·
Q2 (04-27→06-30) due 2026-07-07 · Q3 (07-01→09-30) due 2026-10-07 ·
**General (10-01→10-18) due 2026-10-23**.

Sync cadence: lightweight daily catalog/document poll (late, amended, and final reports
arrive anytime), with burst sweeps in the 15 days before the primary and general for
24h/48h IE filings. The deadline list sets expectations, not the polling schedule.

## 2027 privacy boundary (W. Va. Code §3-8-6a, verified 2026-08-27)

For financial statements filed on or after **2027-01-01**: contributor street number +
street name and the individual's **major business affiliation** (the employer field) may
not be publicly released; **occupation is NOT restricted**; pre-2027 statements are
explicitly unaffected. Employer→industry processing therefore needs a filing-date
boundary and a fail-closed redaction test; occupation charts are unaffected.

## Phase 0 probe results (run 2026-08-27, all gates green)

`npm run west-virginia-candidates:finance:probe` (client + parsers + phaseZero in
`backend/src/pipeline/westVirginiaFinance/`). Verified live in one run:

- Transport: TLS trust-first + pinned-intermediate fallback works (fallback engaged and
  cached); catalog 81 artifacts; repeated mint+download byte-identical (determinism).
- Parsers: CON 2025 28,873 rows / $6,940,014.31 / 1 recovered; CON 2026 18,396 rows /
  $10,454,673.08 / 0 errors; EXP 2026 10,736 rows / $14,073,952.38 / 428 recovered —
  all cent-exact against the known totals.
- Join: all 705 CSV registrants (2025+2026) resolve in the committee registry (100%).
- Reporting cycles: 2026 Candidate/Committee cycles = 7 periods, 2025-07-01 ->
  2026-10-18 (cycle-membership scoping confirmed).
- Reconciliation: 6 committees (incl. embedded-quote fixture 1010003610 and two
  amended-report committees) all cent-exact + multiset-exact -> amendment semantics
  resolved (see above).
- Occupation (org 101 CON 2026): 8,530 individual rows; >$250 single-txn 2,723 with
  occupation 2,597 (95.4%) / employer 2,611 (95.9%); YTD>$250 2,743 / occ 2,500;
  **165 distinct occupation labels** (top: Attorney/Legal 844, Retired 601, Business
  Owner 346, Healthcare/Medical 328, Not Currently Employed 307).
- Outside: 18 IEC/ECC committees 2025-26, 142 docs, **111 IE docs**; sample F-7b PDF
  has no text layer (scan, as expected); sample filed-report PDF (2026 Q2, 1.3 MB)
  **has a text layer** -> cover extraction / cash_on_hand path is feasible.

Still open for Phase 1: LOAN-file vs CON-grid loan-subtype overlap; `transactionTotalYTD`
semantics; full category x contributor-type money matrix pinned against covers.

## Coverage boundary (statutory, verified)

SOS holds filings for statewide, State Senate, House of Delegates, non-magistrate
judicial, and multi-county districts. County candidates file with county clerks and
municipal with municipal recorders through 2026 — **CFRS is not a complete source for
2026 local races**. §3-8-5a: all financial statements for municipal/county/non-statewide
elections filed **after 2027-01-01** go to the SOS. Re-check CFRS + bulk files after that
date; do not assume back-migration of older local filings.

## VoteApp-side scope note

Local DB currently has almost no WV state races for 2026: 2 US House + 1 US Senate races
(FEC-covered) and a handful of municipal rows. **Zero House of Delegates / State Senate /
judicial elections are rostered.** A WV adapter has nothing to attach to until those
rosters exist — WV rostering is the real prerequisite, not the finance client.
