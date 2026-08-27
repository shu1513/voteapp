# North Dakota campaign finance — feasibility (probed 2026-08-26)

Verdict: **buildable for Nov 2026 — totals + outside spending stance + donor
occupation for $5,000+ donors**. (Correction 2026-08-26, second probe: occupation
IS filed and exposed — see the occupation section. The bulk CSV lacks it; the
transaction API carries it. Employer alone is sparse below $5k: 27% of individual
itemized rows in 2025, 8% in 2026 so far.)

## System

- New portal: **https://cfrs.sos.nd.gov** (React SPA, "CFRS", Civix-family;
  launched ~2025). Old vip.sos.nd.gov portal is dead ("site no longer active").
- JSON API base: `https://cfrs.sos.nd.gov/api/Public-Service/...` — anonymous,
  no auth. **WAF blocks plain curl** (403 + TLS interception weirdness from this
  network); calls made from browser context work. An adapter should try Node
  `fetch` with browser-ish UA first; if 403, the bulk-CSV route below goes
  through presigned S3 URLs which are WAF-free once obtained.
- ND historically had NO expenditure disclosure. CFRS-era data (2025+) includes
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
have 1,850 — candidate spending appears to surface on later reports (year-end),
so "total spent" for candidates will read $0/stale until those reports land.
Show spent as of latest filed report; don't treat $0 as a defect.

## Registry / auto-link

`POST /api/Public-Service/Committee/getPublicCandidatesCommitteeDataList`
(`{"pageNumber":1,"pageSize":N}`) → 598 orgs at probe time with `orgID`,
`entityId`, `orgName`, `candidateName`, `orgType`, `orgSubType`, `election`
("2026 Election - Statewide"), `office`, `district`, `party`, `orgStatus`.
Everything needed for auto-link. Committee profile:
`GET /api/Public-Service/Committee/getCommitteeProfileByOrgId/{orgID}` — no
financial summary block, so totals must be summed from CSVs (itemized-sum plus
lump rows; cover-sheet reconciliation possible via Filed reports PDFs if wanted).

## Outside spending (support / oppose) — YES

- Stances: `GET /api/Public-Service/CommitteeTransactions/getStanceForPublic`
  → `SU` Support / `OP` Oppose.
- IE transactions: `POST
  /api/Public-Service/CommitteeTransactions/getAllPublicTransactionDataList`
  (the trackfinance?tab=IE page's call) → rows with `stanceDescription`,
  `candidateNameAssocation` (sic), `committeeName`, `transactionAmount`,
  `transactionDate`, `electionYear`, `orgType:"Independent Expenditure
  Committee"`, `transactionTotalYTD`, and a per-filing PDF
  (`s3ReportFilePath`). 52 rows / $208,647 for 2026 at probe time.
- IE chart: `GET .../getIndependentExpenditureChartData`.
- **Rows are per-candidate ALLOCATIONS (corrected 2026-08-26 after review).**
  StrongND Fund's rows sum exactly to the filing's YTD control: 7 × $16,857.14 +
  18 × $2,000 = $153,999.98 = `transactionTotalYTD`, each row with its own
  sequential `transactionID`. The equal amounts are an even split of one buy
  ($117,999.98 / 7), so summing rows once per row identity IS the committee
  total. Rules: sum unique `transactionID`s; never dedupe by
  spender+vendor+date+amount (equal slate allocations are legitimate);
  `transactionTotalYTD` is a reconciliation control, never a spend row.
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

## Other public endpoints (mapped from `publicEndpoints-*.js` bundle)

Transaction search, contributor/payee search, filed-report list + PDFs
(`getAllFiledReportDataListForPublic`, `getDataDownloadfile` pattern), election/
office/district/party lists, violations. Full list in the bundle chunk
`assets/publicEndpoints-*.js`.
