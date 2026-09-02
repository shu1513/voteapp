# North Dakota campaign finance — feasibility (probed 2026-08-26)

Verdict: **buildable for Nov 2026 — totals + outside spending stance + donor
occupation for $5,000+ donors**. (Correction 2026-08-26, second probe: occupation
IS filed and exposed — see the occupation section. The bulk CSV lacks it; the
transaction API carries it. Employer alone is sparse below $5k: 27% of individual
itemized rows in 2025, 8% in 2026 so far.)

## System

- New portal: **https://cfrs.sos.nd.gov** (React SPA, "CFRS", Civix-family;
  launched 2026-01-01). Old vip.sos.nd.gov portal is dead ("site no longer active").
- JSON API base: `https://cfrs.sos.nd.gov/api/Public-Service/...` — anonymous,
  no auth. **Node transport SOLVED (verified live 2026-08-26, all endpoints 200
  from plain Node fetch):** two requirements —
  1. **TLS**: the server sends a broken chain (the leaf certificate twice, no
     intermediate), so Node/curl fail with `UNABLE_TO_VERIFY_LEAF_SIGNATURE`.
     Fix: pin the Sectigo intermediate ("Sectigo Public Server Authentication
     CA OV R36", from the leaf's AIA URL
     http://crt.sectigo.com/SectigoPublicServerAuthenticationCAOVR36.crt) via
     `NODE_EXTRA_CA_CERTS` or an agent `ca` option. Browsers tolerate the
     broken chain, which is why browser-context calls always worked.
  2. **Headers**: `User-Agent: Mozilla/5.0` + `Origin`/`Referer` of the portal
     (NH Akamai lesson; plain curl UA gets WAF 403).
  Presigned S3 URLs need neither (normal chain, no WAF).
- No structured pre-2025 expenditure export was found in either system (the
  legacy archive exposes filed reports, not a bulk expenditure file; this is not
  a claim about pre-2025 disclosure duties). CFRS-era bulk data (2025+) includes
  expenditures. Bulk data exists only for 2025 and 2026 (registrations back to
  2014). Fine for the Nov-2026 use case.
- Statement thresholds: itemized > $200; unitemized appears as explicit lump
  rows (`Total - $200 or less` in contributions, `Lumpsum - $200 or less` in
  expenditures), so CSV sums are TRUE totals, not itemized-only.

## Bulk CSV downloads (raised / spent)

Discovery: `POST /api/Public-Service/AccessReport/getDataDownloadDataList`
(body `{"pageNumber":1,"pageSize":25}`) → rows with `id`, `dataType`
(Contributions / Expenditures / Debts / Loans / Filed reports / Registrations /
Reporting Schedules), `year`, `s3ReportFilePath`. Files regenerate daily.

Fetch: `POST /api/Public-Service/AccessReport/getDataDownloadfile/{id}` (id in
the PATH; query-param form 404s) → `responseData.fileUrl` = presigned S3 URL
(us-gov-west-1, 1-hour expiry). GET that URL for the CSV (Range requests work).

At probe time: Contributions 2025 (id 1018) / 2026 (1019), Expenditures 2025
(1016) / 2026 (1020). No Loans/Debts files listed yet.

- Contributions columns: `RegistrantID, CommitteeName, CandidateName,
  TransactionType, TransactionCategory, TransactionDate, TransactionAmount,
  ContributorPayeeType, ContributorPayeeName, ContributorAddress, EmployerName,
  FiledDate`. **No occupation column.**
- Expenditures columns: `RegistrantID, CommitteeName, CandidateName,
  TransactionType, ExpenditureType (Itemized - greater than $200 | Lumpsum -
  $200 or less), ExpenditurePurpose, TransactionDate, TransactionAmount,
  RecipientType, RecipientName, RecipientAddress, FiledDate`.
- `RegistrantID` prefix encodes org type: `101…` candidate committee, `102…`
  PAC, `104…` independent-expenditure committee (matches `orgTypeCode` 101/102/
  104 in the API).
- Probe sums: contrib 2025 = 2,914 rows / $4.15M ($1.41M candidate committees);
  contrib 2026 = 5,340 rows / $4.22M ($1.78M candidate); expend 2025 = 1,093
  rows / $2.49M ($465k candidate — matches portal chart to the cent); expend
  2026 = 635 rows / $1.67M with **0 candidate-committee rows**.

### Gotcha: candidate expenditures lag

2026 expenditure file has zero candidate-committee rows while 2026 contributions
have 1,850 — statute puts candidate expenditures on the year-end statement, so
no candidate spending exists mid-cycle. Keep `totalDisbursements` null (never
$0) until an authoritative year-end filing lands, with the static coverage note
explaining the timing — see the plan's hard fact 2. An empty 2026 candidate
slice is expected, not a defect.

**Shape of the year-end lumps (verified 2026-09-01):** in the 2025 file they are
the 562 rows with `ExpenditureType` = `Monetary`, all dated 2025-12-31, blank
recipient type/name, and an `ExpenditurePurpose` category (Advertising,
Campaign Loan Repayment, Miscellaneous, Operations, Travel) — 301 candidate
committee rows ($465,165.32, the exact "Candidate/Candidate Committee" slice of
the portal chart), 60 PAC, 201 party. The portal's "By Purpose Type" chart
series ($786,064.54) is exactly these rows. Candidate committees never appear
in the itemized/lump-sum types. So candidate `totalDisbursements` becomes
publishable from the year-end file as the sum of these category lumps.

## Registry / auto-link

`POST /api/Public-Service/Committee/getPublicCandidatesCommitteeDataList`
(`{"pageNumber":1,"pageSize":N}`) → 598 orgs at probe time with `orgID`,
`entityId`, `orgName`, `candidateName`, `orgType`, `orgSubType`, `election`
("2026 Election - Statewide"), `office`, `district`, `party`, `orgStatus`.
Everything needed for auto-link. Vocabulary pinned 2026-09-01 from 601 rows:
`orgType` = "Candidate/Candidate Committee" (376) / "Committee/PAC" (125) /
"Party Committee" (97) / "Independent Expenditure Committee" (3);
`orgSubType` CNDT "Candidate" (234, `orgName` null) vs CNCM "Candidate
Committee" (142); `candidateName` is "Last, First M" with an optional
honorific (Mr./Ms./Mrs./Dr./Hon., 77 rows) and a suffix on either side of
the comma ("Lippert, Donald Jr.", "Johnston Sr, Daniel"); legislative
`district` = "District N", District Court Judge = named judicial district
("Northeast Central District"), statewide null. Phase 1 auto-link
(2026-09-02, local roster of 53): 49 linked, 0 ambiguous, 4 fail-closed —
see the plan. Committee profile:
`GET /api/Public-Service/Committee/getCommitteeProfileByOrgId/{orgID}` — no
financial summary block, so totals must be summed from CSVs (itemized-sum plus
lump rows; cover-sheet reconciliation possible via Filed reports PDFs if wanted).

## Outside spending (support / oppose) — YES

- **Transaction API contract PINNED (verified live 2026-08-26 from Node):**
  `POST /api/Public-Service/CommitteeTransactions/getAllPublicTransactionDataList`
  with body `{pageNumber, pageSize, transactionCategory, sortColumn:
  "transactionDate", sortDirection: "DESC", transactionYear: ""}` where
  `transactionCategory` = `"CON"` (5,340 contributions), `"EXP"` (635
  expenditures), or `"IE"` **plus `orgTypeCode: "104"`** (52 IE rows — without
  orgTypeCode, `"IE"` falls through to all 6,027 transactions). Codes from the
  SPA enums: category CON/EXP/IE; orgTypeCode 101 candidate cmte / 102 SPAC /
  103 PAC / 104 IE cmte / 105 ECC — but live rows show `1030…` registrants
  with orgType "Party Committee" (2026-09-01), so codes are query selectors
  only; classify rows from their own `orgType`/`transactionTypeDesc` fields.
  Unknown members 400 with a
  could-not-find-member error naming them; unknown VALUES silently return the
  default dataset — distinguish 0-rows from filter-ignored by checking a known
  fixture. Contribution rows carry `employerOccupation` (populated on $5k+
  individual rows).
- Stances: `GET /api/Public-Service/CommitteeTransactions/getStanceForPublic`
  → `SU` Support / `OP` Oppose.
- IE rows: `stanceDescription`, `candidateNameAssocation` (sic),
  `committeeName`, `transactionAmount`, `transactionDate`, `electionYear`,
  `transactionTotalYTD`, distinct `transactionID` per row, `amendedFlag`,
  `reportVersionID`, per-filing PDF (`s3ReportFilePath`). 2026 at probe time:
  52 rows, 52 distinct transactionIDs, 3 committees, 26 candidates, all
  Support.
- IE chart: `GET .../getIndependentExpenditureChartData`.
- **Gotcha: `transactionTotalYTD` is the committee x PAYEE year-to-date
  aggregate** (corrected 2026-09-01 by the Phase 0A probe; the earlier
  "report total" reading fit StrongND only because each of its filings used a
  different vendor). North Dakotans for Public Schools paid one vendor across
  three filings and the control climbs 2,414.57 -> 4,332.23 -> 6,716.09;
  StrongND's May 29 ($44,281.36, Targeted Creative) and June 4 ($153,999.98,
  Edgerton) filings each equal their own vendor total. On contribution rows
  the same field is the donor's running aggregate to that committee.
  Reconciliation rule: per (committee, payee) within a year, max
  `transactionTotalYTD` == sum of unique rows (4/4 groups cent-exact live);
  committee totals = sum of unique `transactionID`s; never publish any single
  YTD value as a committee total.
- **Rows are per-candidate ALLOCATIONS (corrected 2026-08-26 after review).**
  StrongND Fund's June rows sum exactly to the vendor's YTD control: 7 × $16,857.14 +
  18 × $2,000 = $153,999.98 = `transactionTotalYTD`, each row with its own
  sequential `transactionID`. The equal amounts are an even split of one buy
  ($117,999.98 / 7), so summing rows once per row identity IS the committee
  total. Rules: sum unique `transactionID`s; never dedupe by
  spender+vendor+date+amount (equal slate allocations are legitimate);
  `transactionTotalYTD` is a per-payee reconciliation control, never a spend
  row.
- IE CSV is not in the data-download list; the API/portal is the source.

## Donor occupation — YES at $5,000+ (API only, not CSV)

Correction to the first probe (which sampled IE rows, where `employerOccupation`
is always null). Verified 2026-08-26 against statute + FAQ + live API:

- **Law**: NDCC 16.1-08.1-02.3 — for each individual whose aggregate reaches
  **$5,000 in a reporting period** (aggregate test, not per-transaction),
  filings must include occupation, employer, and the employer's principal place
  of business. SOS FAQ and the legacy-archive upload spec (fields "Required if
  amount is equal to or greater than $5000") confirm. **Exemption (statute
  verbatim)**: the duty applies to filers "other than a candidate for judicial
  office, county office, city office, or school district office" — judicial
  candidates get NO occupation data by law (statutory-unavailable note, never
  an empty chart). 48-hour supplemental statements (>$500, last 39 days)
  legally omit occupation — it arrives in the next cumulative report.
- **Live proof**: CON rows from `getAllPublicTransactionDataList` populate
  `employerOccupation` / `employerName` / `employerAddress` on $5k+ individual
  contributions (observed: $15,000 donor → "Healthcare/Medical" / Essentia
  Health / employer address).
- **Surface split**: the bulk Contributions CSV has NO occupation column
  (`EmployerName` only, sparse — 2025: 468/1,749 individual rows; 2026:
  307/3,749). Occupation ingestion must come from the transaction API or filed
  reports.
- **Product consequence**: eligible population is only $5k+ donors — publish
  the occupation chart behind a coverage gate with a threshold disclosure, and
  exclude sub-$5k donors from any Unknown bucket (their occupation was never
  required). See `plan-north-dakota-finance.md` hard fact 3.

## Portal chart endpoints (reconciliation controls)

Bare `GET` on `/api/Public-Service/CommitteeTransactions/getContributionChartData`,
`getExpenditureChartData`, `getIndependentExpenditureChartData` (POST 405s; any
query parameter 400s). Each returns `responseData: [{ name, totalAmount,
data: [{ description, amount }] }]` with ALL-YEARS totals (2025 + 2026 today)
sliced "By Contributor Type" / "By Recipient Type" (blank CSV counterparty type
= chart "Lumpsum"), "By Committee Type" (registry `orgType` of the registrant),
"By Contribution Type" (CSV `TransactionCategory`) and, for expenditures, "By
Purpose Type" (the year-end `Monetary` lumps by purpose). Every slice matched the
summed bulk CSVs to the cent on 2026-09-01 — see the probe results below.

## Other public endpoints (mapped from `publicEndpoints-*.js` bundle)

Transaction search, contributor/payee search, filed-report list + PDFs
(`getAllFiledReportDataListForPublic`, `getDataDownloadfile` pattern), election/
office/district/party lists, violations. Full list in the bundle chunk
`assets/publicEndpoints-*.js`.

## Phase 0A probe results (run 2026-09-01, all seven gates green)

`npm run north-dakota-candidates:finance:phase-zero` (client + parsers +
phaseZero helpers in `backend/src/pipeline/northDakotaFinance/`, 45 unit
tests). One live run, ~2 s between requests, JSON report with `ok` +
`gate_failures`, exit 1 on any failure:

- **Gate 1 transport (local Node):** default trust fails with
  `UNABLE_TO_VERIFY_LEAF_SIGNATURE`, the bundled Sectigo OV R36 intermediate
  fallback engaged on all 20 requests (leaf served twice, valid to 2026-12-05;
  intermediate to 2036-03-21); browser UA + portal Origin/Referer (curl UA →
  403 HTML). Catalog 19 artifacts (Contributions/Expenditures 2025–2026, Filed
  reports 2026, Registrations 2014–2026, Reporting Schedules 2026–2027; a bare
  `{}` body returns only 10). Presign envelope carries CloudFront fields plus
  `fileUrl` (S3 us-gov-west-1, 3600 s). Repeated mint+download byte-identical.
  Production-runtime replay still open (local-acquisition model by default).
- **Gate 2 IE contract:** `transactionCategory: "IE"` + `orgTypeCode: "104"` →
  52 rows / 52 distinct `transactionID`s / 3 committees / 26 candidates, all
  Support, all typed "Independent Expenditures"; without `orgTypeCode` → 6,027
  rows (silent fall-through confirmed); unknown category value `ZZZ` silently
  returns the EXP dataset (635); unknown member → 400 naming it. Unique-row
  total $208,647.42 == IE chart total; per-payee YTD control 4/4.
- **Gate 3 CSV schema + vocabulary + checksums:** all four bulk files parse with
  0 errors (CON 2025 2,914 rows / $4,150,792.03; CON 2026 5,340 /
  $4,218,948.64; EXP 2025 1,093 / $2,486,667.48, 9 recovered bad-width rows;
  EXP 2026 635 / $1,674,944.06, 5 recovered), pure ASCII, CRLF. Pinned
  vocabularies: contributions Monetary / In-Kind / Reimbursement of Expenditure
  / Total - $200 or less / Total - $100 or less; expenditures Itemized -
  greater than $200 / Itemized - greater than $100 / Lumpsum - $200 or less /
  Lumpsum - $100 or less / Monetary (year-end lumps). Chart reconciliation:
  contributions $8,369,740.67 and expenditures $4,161,611.54 cent-exact, every
  slice of every compared series cent-exact.
- **Gate 4 cycle window:** Reporting Schedules map "2025 REPORTING CYCLE" and
  "2026 Reporting Cycle" to "2026 Election - Statewide" (4 periods, 2025-01-01
  → 2026-12-31); 103 candidate committees with 2025 activity, all registered to
  "2026 Election - Statewide" (0 other). 2025 rows belong to the 2026 candidacy
  → factory `minElectionYear` 2026.
- **Gate 5 identity + amendments:** the API CON dataset equals the bulk file
  row for row (2,914 / 5,340 per year, same category labels). Nine
  committee-years reconciled CSV↔API cent-exact and multiset-exact, including
  committees with 1, 1, 1 and 7 Amended filings on record (Filed reports 2026:
  807 CFS Original / 92 Amended, 194 48-hour Original / 15 Amended, 6 IE) →
  **the daily CSV holds current-version rows only**; the 232 / 337
  byte-identical duplicate rows are legitimate repeats. API `amendedFlag` is
  rare (5 rows 2025, 2 rows 2026) and did not mark the amended committees'
  rows. IE `transactionID` digest recorded for cross-day stability
  (`dad36398…`). Donor-YTD check on CON rows: 2,188/2,215 (2025) and
  2,678/2,742 (2026) committee×donor groups match; mismatches are lump rows
  (no counterparty identity) and a few name-keyed donors — pin in Phase 0B.
- **Gate 6 occupation:** 2026 individual Monetary/In-Kind rows to candidate
  committees, aggregated per committee×donor for the year (periods are
  cumulative from Jan 1, so the year aggregate is the period aggregate).
  Donors ≥ $5,000: statewide 16, only **2 with occupation** ($26,069 of
  $176,026); legislative 16, 12 with occupation ($73,405 of $97,905);
  **judicial 2 of 2 WITH occupation** ($44,295.87) — judicial filers supply
  occupation despite the statutory exemption, so the "statutory-unavailable"
  assumption is wrong as a data claim. 22 distinct labels (Retired 36,
  Attorney/Legal 21, Business Owner 20, Agriculture 10, …, "Unknown" 3).
  Display gate (≥20% of individual dollars + ≥3 occupation donors): 1 of 12
  statewide, 10 of 89 legislative, 0 of 3 judicial committees pass today.
- **Gate 7 resolver gold set:** registry 601 orgs (376 candidate committees,
  125 PAC, 97 party, 3 IE); 579 on "2026 Election - Statewide", 22 null. All
  376 current-cycle candidate committees carry office + election (+ district
  for the 278 legislative seats); 33 statewide across 12 offices, 65 judicial
  (District Court Judge, Supreme Court Justice), 363 Active. CSV registrants
  join the registry 422/422.

## Phase 2 — direct totals (built 2026-09-02, local)

Candidate-committee (RegistrantID `101…`) rows in the bulk Contributions files,
counted from the 2026-09-01 downloads, pin the money model's vocabulary:

| TransactionCategory | ContributorPayeeType values seen | 2025 rows / $ | 2026 rows / $ |
| --- | --- | --- | --- |
| Monetary | Individual, Business or Organization, Committee/PAC, Party Committee, Candidate, Self | 500 / 1,359,559.77 | 1,265 / 1,505,604.51 |
| In-Kind | Individual, Self (2025); + Business or Organization, Candidate, Committee/PAC (2026) | 5 / 2,492.99 | 46 / 111,749.02 |
| Reimbursement of Expenditure | Committee/PAC, Individual (2025); Candidate, Self (2026) | 2 / 3,200.00 | 3 / 3,168.38 |
| Total - $200 or less | blank | 50 / 40,583.93 | 536 / 156,613.80 |

No negative or zero amounts in either year; `Total - $100 or less` appears only
on non-candidate filers but stays in the pinned set. Money model (see the plan's
Phase 2 entry): total receipts = every row; Raised = Monetary + In-Kind from the
four donor classes plus the lump rows; Candidate/Self and reimbursements are
excluded; size buckets = itemized individual Monetary rows. A committee with no
rows in the window gets NULL money, never $0 — the file cannot separate "filed
with no contributions" from "first cumulative report not yet due" (Nelson and
Adams, both registered after the pre-primary cutoff, are the live examples).

Live run 2026-09-02 (`raw:refresh` then `sync-due --stale-after-days=0`):
schedules 2026 + 2027 → window 2025-01-01 → 2026-12-31; CON 2025 = 2,914 rows,
CON 2026 = 5,340 rows, API harvests the same counts; 49/49 links synced, every
committee-year CSV↔API multiset-exact; four committees recomputed by hand from
the raw CSVs matched to the cent (Wrigley $160,525.00; Bachmeier $105,237.14
total / $72,826.54 raised / $32,410.60 self; Howe $140,266.07 / $136,516.07;
Haugen-Hoffart $123,279.55 / $89,889.00).

Not in Phase 0A (deliberately): filed-report PDF download (verified once via
`Common-Service/AmazonCloudFront/getDownloadLinkWithoutCookies` with
`{ s3FilePath }` → presigned URL), the 48-hour overlap (Phase 0B, after
Sep 25–Oct 2), any schema or publication.
