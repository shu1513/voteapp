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
   committeeId for committees) → `{resultCount, totalContrLessThan35 (always 0 publicly), primaryTotal, generalTotal}`
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

Sample (Bedey, SD-43, 2025–2026, re-verified live 2026-08-27): 210 individual
contributions $59,347.67 + committee $6,780.45 + $10k loan + $182.16 "Debts and
Loans Not Yet Paid" = $76,310.28 (cent-exact); expenditures 162 rows $61,467.18.
The debts row is a FOURTH line-item family in the CONTR export — debt tracking, not
cash; it feeds neither directContributionTotal nor the cash-begin chain.

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
  and unitemized lumps `totalContrLessThan35` / `grandTotalLessThan35{,Primary,General}`
  (lump fields always 0 publicly — dead, do not use).
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
  money is NOT exposed anywhere: the per-report `...LessThan35` lump fields (legacy
  name) render always-zero in the public flow (Phase 0: all 24 harvested C-5s) — dead
  fields; derive the unitemized amount from the cash-begin chain residual instead
  (see the plan's derived-lump rule).

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

## Phase 2a facts (verified live 2026-08-28, full Bedey cycle: 8 canonical C5s)

- **`payment` detail list = payments on debts and loans**, not campaign
  spending: Bedey's `payment` total equals his `debtLoan` total to the cent
  ($10,182.16 = the $10k loan + $182.16 debt, both repaid). The EXPEND CSV
  export EXCLUDES it (only line item observed: "All Other Expenditures",
  cent-exact equal to the `expendOther` list). So: chain outflow = payment +
  expendOther + pettyCash cash (money leaving the bank), but the published
  "spent" total = expendOther + pettyCash only (matches the state's own
  export presentation).
- Cross-checks enforced fail-closed by the aggregator: committee CSV == JSON
  to the cent; EXPEND CSV == JSON `expendOther` + `pettyCash` (after the
  side-transfer partition below). **The CONTR CSV is a threshold-filtered
  public view**: it drops or rolls sub-threshold entries into a
  "Contributions Less Than $35 Each" family (MCA 13-37-229's $50 cumulative
  rule; Eddy's CSV runs $12,916.81 short of her fully-itemized JSON, all
  explained by sub-$50 rows; Bedey's is complete). Gate: CSV individual
  (+ less-than-35 family) may only be ≤ JSON, and the shortfall must fit
  inside the sum of the JSON's sub-$50 rows. Totals ALWAYS come from the
  JSON; the CSV feeds only the occupation breakdown.
- The unquoted CSV can split one logical row across physical lines (a field
  with an embedded newline — observed on an Eddy contributor row); the
  parser reassembles short lines until the 18-column width lands exactly.
- `searchFinancials` works with an **empty lastName** (CONTR and EXPEND):
  `prepareDownloadFile`'s `candidateId` selects the entity, so acquisition
  needs no name. The `(searchResults)` title assertion still gates the flow.
- Full-cycle chain closes across all 8 canonical C5s; residual lumps are
  small positives (Bedey total $389.45 unitemized), Incorporated C7s excluded.
- **Inter-side fund transfers are booked as ordinary expenditures** with the
  receiving side booking a matching misc receipt (`refunds` list). Observed
  on BOTH probed filers: Eddy "Transfer of primary funds to general, no
  primary" $241,307.00 (arriving as $241,169.05 — a $137.95 fee absorbed;
  her `primCashBeg` drops to 0 next report) and Bedey "Transfer of Primary
  funds to General Funds" $249.04. They are real cash flows for the chain
  but NOT campaign spending: the aggregator partitions them out of "spent"
  on BOTH surfaces by the exact filer idiom `transfer of (primary|general)
  funds` (deliberately precision-over-recall — an ordinary payment
  mentioning "transfer" must never be dropped, since the identical filter
  runs on both surfaces and the cross-check could not catch it); the
  EXPEND CSV includes them (Bedey verified), so the docs' earlier "162 rows
  $61,467.18" figure includes his $249.04 transfer — published spent is
  $61,218.14.
- Should a rollover ever appear WITHOUT booked transfer flows (per-side
  equations cannot close), chain reconciliation falls back to the COMBINED
  conservation equation for that link (same lump gate; link
  `side: "combined"`). Both probed filers close per-side.
- Detail rows can be zero-amount placeholders with `amountTypeDescr: ""`
  (observed: an all-zero `Loans` row on Eddy's first report). The parser
  accepts the empty side ONLY when every amount field is zero.
- Candidate-search `officeTitle` shapes (full 1,089-row 2026 list):
  `Senate District No. 43`, `House District No. 12`, `Supreme Court Justice
  No. 03`/`No. 04` (zero-padded seats), `Public Service Commission District
  No. 1`, `District Judge, District 4 Dept. 2`. The full year list is a
  single DataTables page (~3 MB) — one fetch serves a whole auto-link batch.

## Phase 2b facts (IE sweep pinned live 2026-08-28)

- **Committee sweep flow**: the search page's `searchExpendituresForm` POSTs
  to `searchResults/searchFinancials` with `financialSearchType=EXPEND`,
  `expendSearchTypeCode=COMMITTEE`, `independentExpendSearch=true`,
  `electioneeringCommSearch=false`, `electionYear`, and EMPTY name/date
  fields (`expendCanLastName`, `expendCanFirstName`, `expendCommitteeName`,
  `payeeLastName`, `payeeFirstName`, `expendSearchFromDate/ToDate`); title
  marker `(searchResults)` gates the bounce. Committee list:
  `GET searchResults/listFinancialCommitteeResults` (DataTables; 49
  committees for 2026, `committeeId`/`committeeName`/`committeeTypeDescr`).
  Per committee: `POST searchResults/viewFinancialEntities`
  `{candidateId: 0, committeeId}` (capture `resultCount`), then
  `GET searchResults/listViewFinancialEntityResults`.
- **The year search does NOT scope transactions**: each committee's list is
  its FULL IE history (the "2026" search surfaced $14.4M back to 2020) and
  row-level `electionYear` is always null. Cycle scoping is by `datePaid`
  against [Jan 1 year-1, Jan 1 year+1) — 1,380 rows / $5.50M in-window for
  2026 at sweep time.
- **IE transaction rows carry NO committee identity** (`committeeId`,
  `committeeName`, `candidateId` all null; `entIdFrom` is the PAYEE entity).
  Fresh session per committee is the binding; the sweep artifact records the
  requested committeeId plus the `resultCount` cross-check. Row invariants
  enforced: `transTypeDesr == "Independent Expenditure"` (filtered
  otherwise), `totalAmt == cashAmt + inKindAmt` cent-exact (holds on all
  2,147 live rows), real epoch `datePaid`, `electioneeringInd` present.
- **candidateIssue grammar** (312 distinct in-window values): bare
  `NAME (SD-9)` (AFP), `Support/Oppose <name>` (School Freedom Fund files
  explicit oppose at scale), `Last SD43` / `Name HD 55` / `Name/SD 34` /
  `SD 14; Name` token forms, `Name for PSC District 4`, `Name for Montana
  Supreme Court`, chamber-only `Name for Montana State Senate`; plus
  quarantine classes: attachments/addenda ("See attached", typo "se
  attachment", "See Quantity field"), blanks, multi-candidate joins
  (commas/semicolons/and/or/slash, dual-stance "Support X / Oppose Y"),
  ballot issues (CI-126/CI-132, levies), municipal/federal targets.
- **Resolution**: exact name (+ nickname expansion on the issue side only —
  the shared `personFirstNameNicknames` groups; corpus files "Ken Walsh"
  against "Walsh, Kenneth M") within the year's registration list,
  office-token constrained. **CERS mints a NEW candidateId per race**, so a
  race-switcher holds several same-year registrations (George Nikolakakos:
  SD-11 Closed, HD-22 Closed, SD-12 Reopened) — multiple name matches
  tie-break to the single live (not Closed/Withdrawn) registration, the
  same row auto-link binds. "Closed" alone is NOT disqualifying (primary
  losers close their registrations and still carry real IE money). A
  contradicting office token quarantines ("KATHY LOVE (SD-9)" vs her SD-43
  registration — filer typo), as do typo'd names ("Buttery" for Buttrey,
  $170k — alias-table headroom). Measured resolution: **43.1% of in-window
  2026 dollars** (vs 35.9% at Phase 0); the rest: attachments $1.69M,
  blanks $543k, multi $477k, typos $321k.
- Published data: resolved rows only. Support = bare + `Support` rows;
  oppose = explicit `Oppose` rows; totals NULL (never 0) when a stance has
  no resolved rows. Outside data rides the regular sync (writer
  `preserveWhenNull` keeps prior outside snapshots when the sweep bundle is
  missing); when the sweep IS present it is authoritative, and a follow-up
  guarded UPDATE clears any stale total for a stance that resolved nothing
  (preserveWhenNull alone would keep an old dollar figure alive next to the
  emptied groups). The batch refreshes the sweep once per election year.
  Outside group/display source URLs are always the stable dashboard URL —
  the DataTables harvest endpoints are session-scoped and answer empty to a
  plain GET. The ballot-lookup loader ships the Montana outside footnote
  (`outside_coverage_note`, rendered by web + mobile) — the plan's hard gate.
- Quarantine report: `npm run montana-candidates:finance:outside-report --
  --year 2026 [--refresh]` — per-committee resolved/quarantined dollars by
  reason; the input for attachment recovery (Conservatives4MT holds $1.55M
  of attachment-referenced rows alone).

## Build notes

- Adapter shape: per-candidate CONTR + EXPEND export (cent-exact, occupation included)
  + `listFinanceReports` for unitemized lumps and period bookkeeping. Primary/General
  split native (`Election Type` column / `amountTypeDescr`).
- Loans are a separate Line Item (like Denver's 4th subtype); refunds flagged
  `refundInd`; in-kind separated via Amount Subtype.
- Committee funders: same flow with `searchCommitteeContributions` for IE-committee
  donor rollups.
- Amounts in CSV are plain numbers (no $ or commas); dates MM/DD/YYYY; **delimiter `|`**.
