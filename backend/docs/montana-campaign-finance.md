# Montana campaign finance — feasibility (probed 2026-08-26)

Verdict: **buildable for Nov 2026**. Totals raised/spent, primary/general split, and
donor occupation + employer are all available. Outside-spending targets are available
as free text; support/oppose stance is **not structured**.

## System

- CERS (Campaign Electronic Reporting System), Montana COPP:
  `https://cers-ext.mt.gov/CampaignTracker/` — old Spring MVC + DataTables app,
  server-rendered, session-based, **no auth, no CSRF, curl works**.
- 2026 registrations at probe time: **1,090 candidates** (legislature, Supreme Court,
  county offices, JP, school). Federal races (US Sen/House) are FEC, not CERS.
- The site states there is NO bulk all-entities download — harvest is **per entity**,
  like our per-candidate sync states.

## Session flow (all endpoints under `/CampaignTracker/public/`)

Every data call is session-scoped: cookie jar + this order, or you get someone
else's cached results. **Use a fresh session per entity** — stale search state from a
prior flow silently overrides later `retrieve*` posts (verified: committee report
list kept returning the previous candidate's reports in a reused session).

1. `GET search/candidateSearch` (seeds JSESSIONID)
2. `POST searchResults/searchCandidates` — form fields `lastName, firstName,
   middleInitial, candidateTypeCode, officeCode, countyCode, partyCode, electionYear`
3. `GET searchResults/listCandidateResults?sEcho=1&iColumns=9&iDisplayStart=0&iDisplayLength=100&iSortCol_0=1&sSortDir_0=asc`
   → DataTables JSON `{iTotalRecords, aaData:[{candidateId, candidateName, officeCode,
   officeTitle, partyDescr, candidateTypeDescr, resCountyDescr, electionYear,
   candidateStatusDescr, filingStatusDescr, c3FiledInd, ...}]}`
   - **`iSortCol_0` is required** — omitting it throws `IllegalStateException` (HTML error page).
   - Gotcha: form action resolves relative to `/CampaignTracker/public/` (the page URL
     has no trailing slash), NOT `/public/search/`.

### Transactions (raised / spent)

1. `POST searchResults/searchFinancials` with `financialSearchType=CONTR`
   (`contrSearchTypeCode=CANDIDATE`, `contrSearchFromDate/ToDate` MM/DD/YYYY, name optional)
   or `financialSearchType=EXPEND` (`expendSearchTypeCode=CANDIDATE|COMMITTEE`).
   For COMMITTEE searches send `electionYear` and **empty date fields** (the committee
   variant of the form hides dates; sending both made validation silently bounce back
   to the search page — response `<title>` says `(search)` instead of `(searchResults)`).
2. `POST searchResults/viewFinancialEntities` `{candidateId:<id>, committeeId:0}` (or
   committeeId for committees) → `{resultCount, totalContrLessThan35, primaryTotal, generalTotal}`
3. `GET searchResults/listViewFinancialEntityResults?...` (same DataTables params) →
   full transaction JSON: `transTypeDesr` (Individual Contributions / Independent
   Committee Contributions / Loans / Independent Expenditure / ...), `cashAmt`,
   `inKindAmt`, `amountTypeDescr` (**Primary/General split**), `datePaid`,
   `candidateIssue`, `purposeDescr`, nested `entityDTO` (name, address, entity type).
4. CSV export: `POST searchResults/prepareDownloadFile` `{candidateId:<id>, committeeId:0}`
   → `{fileName}` → `GET searchResults/downloadFile?fileName=<name>`.
   **Pipe-delimited**, columns: Candidate ID|Candidate Name|...|Contributor Name|
   Contributor Address|City/State/Zip|**Occupation|Employer**|Date Paid|Purpose|
   Description|Line Item|Amount|Election Type|Amount Subtype(Cash/In-Kind)|Office Title.
   **Date caveat: the CSV export's `Date Paid` on CONTRIBUTION rows is the report-period
   START date, not the transaction date** (verified: distinct values = exactly the
   period boundaries). Expenditure-export dates ARE real transaction dates. Real
   per-transaction contribution dates exist in `viewFinanceReport/financeRepDetailList`
   JSON (`datePaid`; 17 distinct dates inside one Bedey period). Rows are entry-level,
   not donor-period aggregates (same donor can have 2 same-amount rows in one period),
   but entry granularity is filer behavior — validate before shipping contribution-size
   buckets.

Sample (Bedey, SD-43, 2025–2026): 210 individual contributions $59,347.67 + committee
$6,780.45 + $10k loan = $76,310.28; expenditures 162 rows $61,467.18. Cent-exact sums.

### Donor occupation: YES — best probed state so far

- CSV export and report-detail JSON both carry **Occupation AND Employer**.
- Fill rate in sample: **210/210 (100%)** of individual contribution rows.
- Note: the transaction-search JSON (`listViewFinancialEntityResults`) leaves
  `entityDTO.occupationDescr` null — occupation only appears in the **CSV export**
  and in `viewFinanceReport/financeRepDetailList` rows. Use those.

### Official report totals (C-5 covers)

- `POST publicReportList/retrieveCampaignReports` `{candidateId, searchType:, searchPage:public}`
  → 302 → `GET publicReportList` → `GET publicReportList/listFinanceReports?...`
  (DataTables JSON): one row per filed report — `reportId`, `formTypeCode`
  (C5/C4/C7/C7E), period, status (Filed/Amended), `primCashBeg`, `genCashBeg`,
  and unitemized lumps `totalContrLessThan35` / `grandTotalLessThan35{,Primary,General}`.
  Committee variant: `retrieveCommitteeReports` `{committeeId, ...}`.
- Report detail: `POST viewFinanceReport/retrieveReport` `{candidateId|committeeId,
  reportId, searchPage:public}` → `POST viewFinanceReport/financeRepDetailList`
  `{listName}` with listName ∈ candidate, committee, debtLoan, expendIndependent,
  expendOther, fundraisers, individual, loan, payment, pettyCash, refunds.
- The rendered cash-summary (lines 1–4) is NOT publicly exposed at all — re-verified
  2026-08-27: the public viewFinanceReport HTML renders the summary cells EMPTY and
  listFinanceReports rows carry no receipts/disbursements totals (`grandTotal: null`).
  Official control = the cash-begin chain: begin(N) + receipts(N) - disbursements(N)
  must equal begin(N+1) (`primCashBeg`/`genCashBeg`, primary and general separately);
  detail sums define the totals. Itemization threshold is $50 cumulative; smaller
  money sits in the per-report `...LessThan35` lump fields (legacy name).

### Outside spending (independent expenditures)

- `searchFinancials` EXPEND + `expendSearchTypeCode=COMMITTEE` +
  `independentExpendSearch=true` (+`electioneeringCommSearch` for electioneering)
  → 46 committees all-years, 11 tagged 2026 (AFP-Montana, MT Citizens for Right to
  Work, party committees, incidental committees...).
- IE transaction rows and C-4/C-7E report rows carry **`candidateIssue`** — free-text
  target, e.g. `ZACK WIRTH (SD-9)`; plus purpose/platform/quantity (C-7E adds the
  pre-election notice trail).
- **Stance semantics (verified 2026-08-26 from primary sources):** there is no
  support/oppose field, but by rule the `candidateIssue` field ALWAYS names the
  candidate the expenditure was intended to BENEFIT:
  - ARM 44.11.502(6)(b) (current numbering; COPP's 2018 guidance PDFs cite the same text as (8)(b)): filer "shall report the name of the candidate or committee
    the independent expenditure was intended to benefit".
  - COPP CERS 101 guidance (verbatim): "In the event an Independent Expenditure is
    made primarily to oppose a specific candidate, the candidate benefitted needs to
    be listed ... as a supported candidate ... as expenditure activity opposing their
    opponent inherently benefits them." IE guidance doc: "always report the name of
    the candidate SUPPORTED [in] the candidate/issue tab" (opposed name MAY appear in
    the free-text purpose only).
  - **Practice diverges from the rule (Phase 0, 2026-08-27):** many filers write the
    stance INTO candidateIssue — "Oppose George Nikolakakos", "Support Barry Usher".
    Explicit oppose = ~$686k of 2025-26 IE dollars (mostly School Freedom Fund - FEC).
    So: bare-name rows → support (benefit rule); "Support X" → support; "Oppose X" →
    oppose (filer-declared). `outside_oppose_total` = explicit oppose rows when
    present, NULL otherwise (never 0; writer `preserveWhenNull`).
  - Coverage reality: only ~36% of 2025-26 IE dollars auto-attribute; "see attached"
    + blank = 41% (Conservatives4MT $1.75M alone) — recover via report attachments
    (`viewFinanceReport/attachmentList`) manually for the big spenders.
- candidateIssue VALUES are free text with real noise — resolution needs a parser +
  quarantine: clean `NAME (SD-9)` (AFP, the biggest 2026 spender), multi-candidate
  single-amount rows ("Jones, Bedey, Rindal, Wirth, Love" — no allocation), ballot
  issues ("CI-132"), "see attachment" / "See Quantity field" (MT Citizens for Right
  to Work mailers), blanks, and at least one misfiled committee-donation row labeled
  IE. Also name-quality drift across rows (independent report observed LYN BENNET /
  LYN BENNETT) — resolve by name+office+district+year, never bare name.
- The committee-expenditure CSV export does NOT include candidateIssue — IE harvest
  must use the JSON rows (`listViewFinancialEntityResults` or report-detail
  `financeRepDetailList`), which also carry real transaction dates.
- Electioneering: rows carry `electioneeringInd`; C-7E/electioneering money must be
  kept out of support totals (MCA 13-1-101 electioneering = no advocacy).

## Build notes

- Adapter shape: per-candidate CONTR + EXPEND export (cent-exact, occupation included)
  + `listFinanceReports` for unitemized lumps and period bookkeeping. Primary/General
  split native (`Election Type` column / `amountTypeDescr`).
- Loans are a separate Line Item (like Denver's 4th subtype); refunds flagged
  `refundInd`; in-kind separated via Amount Subtype.
- Committee funders: same flow with `searchCommitteeContributions` for IE-committee
  donor rollups.
- Amounts in CSV are plain numbers (no $ or commas); dates MM/DD/YYYY; **delimiter `|`**.
