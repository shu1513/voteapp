# Alabama Campaign Finance (FCPA) — Feasibility Findings

Probed 2026-08-26. Verdict: **BUILDABLE for totals (raised + spent). Donor occupation: NOT AVAILABLE. Outside-spending stance: NOT AVAILABLE.**

## System

- Alabama migrated off the old `fcpa.alabamavotes.gov/PublicSite` (PCC) system to a **Tyler Technologies entellitrak** app ("AL Campaign Finance System", internal name ACF) at `https://fcpa.alabamavotes.gov/`. Old PublicSite URLs redirect to a login page.
- **TLS caveat:** the server sends an incomplete certificate chain — plain `curl` fails with `SSL certificate problem: unable to get local issuer certificate`. Probe used `-k`; a production adapter should pin/append the intermediate CA instead.
- No auth, no API key, no CAPTCHA on any endpoint below. Search/list endpoints are JSON; dropdown-id maps, filing details, and the financial-summary page are HTML; extracts are zipped CSVs.

## Bulk extracts (size-bucket ingest; superseded as totals source — see Addenda + Phase 0 results)

Public "Download Data" page serves annual zipped CSVs, **2013–2026, 4 types, refreshed daily** (observed last-updated = same day at 02:32 AM).

- File list (JSON):
  `GET /page.request.do?page=com.acf.common.page.transactiondatadownloadsresults&pageSize=100&pageNumber=1&sortDirection=ASC&sortBy=state`
  → `{data:{totalRecords:56, list:[{DATATYPE, YEAR, LASTUPDATED, DOWNLOAD:<id>}]}}`
- Download: `GET /page.request.do?page=getTransactionData&id=<DOWNLOAD>` → zip with one CSV.
  2026 ids: Cash=54, Expenditure=55, In-Kind=53, OtherReceipts=56 (ids are per-file, re-read the list each run).

Columns (verified against files; layout PDFs at `page=getResource&resource=cashContributionsExportLayout` / `expendituresExportLayout` match):

- `CashContributionsExtract`: CommitteeId, ContributionAmount, ContributionDate, LastName, FirstName, MI, Suffix, Address1, City, State, Zip, ContributionID, FiledDate, ContributionType, ContributorType, CommitteeType, CommitteeName, CandidateName, Amended
  - ContributionType: Cash (Itemized/Non-Itemized), In-Kind (Itemized/Non-Itemized), Non-Itemized Employee Payroll Contribution — the cash file *also* contains in-kind rows; dedupe against the separate In-Kind file before summing.
  - ContributorType: Individual, Group/Business/Corporation, PAC, Other, Returned (Cash Only), blank.
- `ExpendituresExtract`: same shape + Explanation, Purpose, ExpenditureType (Itemized, Non-Itemized, ±Line of Credit variants).
  - Purpose vocabulary (2026 full set): Advertising, Administrative, Contribution, Other, Food, Fundraising, Consultants/Polling, Transportation, Charitable Contribution, Refund, Qualifying Fee, Lodging, Reimbursement, Duties of the Office, Loan Repayment, Interest, Inagural (sic).
- **Data quality:** rows with unescaped quotes/commas break naive CSV parsing — 2026 expenditures: 828/34,457 rows ragged (~2.4%); 2026 cash: 12/116,808. Adapter needs a tolerant parser (or python csv + repair pass), not strict split.

## Committee / candidate linking

- Principal Campaign Committee search (JSON):
  `GET /page.request.do?page=com.acf.common.page.committeesearchresults&pageNumber=1&pageSize=50&sortDirection=ASC&sortBy=candidateLastName&criteria=<urlencoded JSON>`
  criteria = array of `{field_key, comparison_type, comparison_value_1}`; **must include** `{"field_key":"committeeType","comparison_type":"equalTo","comparison_value_1":"1"}` (1=PCC; PAC search uses its own page/type).
  Filterable: candidateLastName/FirstName, committeeId, party (id), office (id), jurisdiction (id), committeeStatus (3=Active, 1=Dissolved).
  Returns: internal `id`, `committeeId` (matches extract CommitteeId), candidate name, office, party, jurisdiction, place, status, registeredDate.
  Dropdown id maps (office 46 entries, party 9, jurisdiction 251) live in the search page HTML (`page=page.acfPublicPrincipalCampaignCommitteeSearch`).
  Example: office=23 (Governor) + status Active → 22 committees (Ivey, Doug Jones, Blanchard, …).
- Committee details page: `page=page.acfPublicCommitteeDetails&type=cGNj&id=<b64 internal id>`; filings list via `page=com.acf.common.page.committeeelectronicfilingsresults`.

## Report-cover summary (verification anchor)

`GET /page.request.do?page=page.acfPublicCommitteeFinancialSummary&committeeIdStr=<internal id, PLAIN not base64>` → HTML with embedded JS literal `financialSummaryData` = lifetime + per-year `{cashContributions, inKindContributions, otherReceipts, expenditures, lineOfCreditExpenditures, beginning/endingBalance}`, plus latest report name/filedDate/periodEndDate.

- **Cent-exact cross-validation:** Doug  Jones (committeeId 32837, internal id 7962): summary totalCashContributions 588,344.73 / in-kind 10,017.08 == 2025 cash extract sum (378,675.76 itemized + 209,668.97 non-itemized) and in-kind exactly.
- Summary lags to the last **filed** report period (extracts are fresher, daily). 2026 extract already shows Jones +$3.05M more.
- Open question for build: endingBalance didn't reconcile to contributions−expenditures on this sample (155,546.38 vs 576,016.78 computed); page has a "Reported items not affecting cash balance" section — resolve during Phase 0.

## The two NO answers

- **Donor occupation/employer: not collected anywhere.** No column in any extract, no field in contribution search UI, none in layout PDFs. Alabama FCPA does not require it. Occupation-based features impossible for AL (like Mississippi).
- **Outside spending support/oppose: no data.** No independent-expenditure record type, no target-candidate field, no support/oppose flag; Purpose vocabulary has no IE category. PAC *direct* giving to candidates is fully visible in receipts (ContributorType=PAC), but true outside spending stance must be null for AL (Montana-style oppose-null, but here both directions null).

## Suggested v1 scope

**Superseded — final contract (per Addenda 2–3 + §Phase 0 results and `plan-alabama-finance.md`):** totals-only adapter; roster = VoteApp Nov-2026 candidates matched to political-race-search rows (committee search is metadata fallback only); totals authority = race API, validated cent-exact against filed report covers (`totalReceipts` = `MONETARYCONTRIB + NONMONETARYCONTRIB + OTHERSOURCES`; `directContributionTotal` excludes `OTHERSOURCES`; extracts can undercount and are never the totals source); size buckets from itemized cash extract rows with a reported coverage ratio; occupation null; outside null.

Original suggestion (kept for the record, do not implement): candidate committees via committee search (office/party/status), raised = cash + in-kind + other receipts from extracts (dedupe in-kind rows in cash file), spent = expenditures extract, verify against financialSummaryData covers; occupation null; outside null.

## Addendum — cross-check vs external feasibility report (2026-08-26)

An independent feasibility report (dated 2026-08-02) was verified point-by-point. All checked citations were real and accurate:

- EDI spreadsheet spec (`page=getResource&resource=ediSpecification`) and EDI XML spec (`resource=ediXmlSpecification`): 0 hits for occupation/employer — confirms the gap at the filing-schema layer, not just the extract layer.
- FCPA FAQ (`resource=faq`): "An amendment is automatically required if you edit or add a transaction after a report has already been filed" — verbatim.
- Filing detail pages (`page=page.acfPublicFilingDetail&filingId=<n>`) carry full structured period summaries: beginning/ending balance, itemized/non-itemized splits for cash, in-kind, other receipts, expenditures, line-of-credit. Verified Woods filingId 187758: beginning $358,862.76, itemized cash $10,000.00, expenditures $18,340.18 + $21.23, exact. **These are the correct balance anchors** — the CommitteeFinancialSummary page anchored to a Major Contribution Report is not (explains the earlier endingBalance mismatch on Doug Jones).
- 2026 Candidate Filing Guide: $100 itemization threshold (per-source cumulative); electronic filing required for all candidates and PACs (incl. municipal since 2023); electioneering-communication spend >$1,000 triggers filing (§17-5-8) but with NO target/direction disclosure; municipal candidates exempt until >$1,000 raised or spent (§17-5-4.1, HB 156 2024) — so missing municipal filings ≠ zero activity; federal candidates file with FEC only.
- Interactive contribution/expenditure searches cap exports at 20,000 rows — bulk extracts are the ingest path, searches are for verification.
- **Political race search API** (`page=com.acf.common.page.politicalracesearchresults&election=<id>&office=<id>&...`; election 160 = "2026 ELECTION CYCLE") returns per-candidate race-level summaries: BEGINNINGFUNDS, MONETARYCONTRIB, MONETARYEXP, NONMONETARYCONTRIB, OTHERSOURCES, ENDINGFUNDS, YEAR. Verified live (2026 Governor: 9 rows, Boyd $72,795.63 raised / $64,092.68 spent). Best cycle-totals anchor for reconciliation.

Gaps in that report (found here, not there):

- **In-kind double-count hazard:** the cash extract embeds in-kind rows (2026: 2,873 rows, $3.94M) that also appear in the in-kind extract (2,911 rows, $3.97M). CORRECTION (2026-08-26 second pass): the shared rows carry the SAME transaction id in both files (`ContributionID` == `InKindContributionID`, 2,873/2,873 composite-verified), and in-kind ids never collide with pure-cash ids — so id-union dedupe is safe, or simply ingest the cash file alone (it contains nearly all in-kind; 38 rows were only in the in-kind file at probe time).
- **Ragged CSV rows** (~2.4% of 2026 expenditures) require a tolerant parser.
- TLS incomplete-chain issue on fcpa.alabamavotes.gov.
- Unverified minor claim: PAC-to-PAC transfer ban (real law, Ala. Code §17-5-15, but not confirmed from a primary source in this pass).

## Addendum 2 — deep verification of the terse implementation spec (2026-08-26)

- **Race API is lifetime/cycle-scoped and reconciles cent-exact with extracts.** `politicalracesearchresults` (election=160, office=23): Jones MONETARYEXP 1,984,347.70 == 2026 extract 1,962,002.67 + 2025 report 22,345.03 exactly; NONMONETARYCONTRIB 10,017.08 exact. Raised showed a $20,000.00 gap vs extract sum (3,662,947.93 vs 3,642,947.93) — extracts regenerate ~02:32 AM daily, race API is live; likely a same-day Major Contribution Report. Verify in build; treat race API as fresher.
- **Accounting identity** holds: ENDINGFUNDS = BEGINNINGFUNDS + MONETARYCONTRIB + OTHERSOURCES − MONETARYEXP (Tuberville exact; Jones off by $500.00 residual — investigate). In-kind and line-of-credit are outside the cash balance, matching the portal warning.
- **`year` query param is broken** (returns 0 rows) — never pass it; the no-year response is the cycle aggregate.
- **Roster: race API beats committee search.** 2026-cycle Governor: 9 race rows (incl. TUBERVILLE $14.0M raised / $4.59M spent, Jones, and dissolved Flowers) vs 22 "Active" PCC committees (stale registrations, e.g. Ivey 2022 leftovers). Use race rows as roster+totals; committee search only as metadata fallback.
- **Extract ids are unique** (0 duplicate ContributionID/ExpenditureID/InKindContributionID) → extracts are current-state snapshots; `Amended=Y` marks a transaction's current post-amendment version, so KEEP those rows. 2026: 810 cash / 1,259 exp / 217 in-kind amended rows.
- **Ragged-row accounting reconciled:** 34,505 data lines; python csv parses 34,457 rows of which 829 have wrong field count, and 48 lines get swallowed into neighbors by embedded newlines — 829 + 48 = 877 defective, matching the spec's 877/34,505.
- **Line-of-credit materiality:** 2026 statewide LOC = $312,710.75 across 475 rows vs $616.7M regular — 0.05%. Excluding LOC from `spent` matches state cash accounting and race-API identity.
- **Mapping nit:** "raised = cash + in-kind, exclude other receipts" fits `directContributionTotal`; `totalReceipts` should ADD `OTHERSOURCES` (real money in; Tuberville $125,319.78). Both fields exist in the standard writer.

## Addendum 3 — corrections from third verification pass (2026-08-26)

- **`year` on race search works** — it expects the `financialYear` dropdown's internal option id (1 = 2026, 12 = 2025), not a literal year (literal years return 0 rows; Addendum 2's "broken" claim was wrong). Year-scoped attribution differs from extract contribution-date years (Jones 2025: $105,546.38 vs extract $588,344.73) — omit the param and use the cycle aggregate.
- **`ENDINGFUNDS` validated via the filing chain**: Jones July monthly cover ending balance $1,659,100.23 + Major Contribution Report filed 08/26/2026 08:41 PM ($20,000.00) = race `ENDINGFUNDS` $1,679,100.23 exactly. This also confirms the raised-total drift mechanism: race API includes same-day filings; extracts regenerate ~02:32 AM. The $500.00 Jones residual sits in the aggregate contribution/expenditure columns, not the balance.
- **LOC ratio corrected**: Addendum 2's 0.05% used a $616.7M "regular spend" figure poisoned by malformed-row amounts. Well-formed 2026 rows only: regular $76,776,637.28, LOC $312,710.75 → ≈0.41%. Materiality conclusion (small, exclude per state accounting) unchanged; the poisoning is itself a lesson — quarantine malformed rows before ANY aggregation.
- **Cash-file defect unit fixed**: the 2026-08-26 snapshot has 4 mis-fielded rows (earlier "12" counted cells). Files regenerate daily; assert defects by method, not exact count.
- **Returned/negative rows**: `ContributorType = Returned (Cash Only)` rows carry POSITIVE amounts (5 in the 2026 file); one genuinely negative `Cash (Itemized)` row (−$500.00) exists. Whether `MONETARYCONTRIB` counts returned rows positively is unconfirmed (the natural test committee, 33927, has no filed cover yet) — Phase 0 item. Either way: signed-amount parsing, returned rows out of buckets (matches the Florida aggregator's exclusion of IN KIND/REFUND/RETURNED/LOAN/TRANSFER).
- **In-kind stays OUT of size buckets** — house precedent (`floridaDirectContributionAggregator.ts` exclusion regex); a reviewer suggestion to include it was rejected for cross-state consistency.
- **Filings-list endpoint**: `page=com.acf.common.page.committeeelectronicfilingsresults&committeeId=<internal id>&pageNumber=1&pageSize=10&sortDirection=DESC&sortBy=dueDate` → report descriptions, periods, filed dates, amended flags, filing ids (Jones: 23 filings).
- **Launch-checklist addition**: new sources also need a `FINANCE_SOURCE_HOME_URLS` entry in `packages/api-client/src/finance.ts` (missed by the label-only checklist).
- Per-state finance snapshot tables are never promoted to prod — prod data comes from running the sync against production (promote scripts only carry `finance_committee_labels`).

## Phase 0 results (2026-08-26, probe `npm run alabama-candidates:finance:phase-zero`)

**PASSED — all gates green** (`ok: true`). Code: `backend/src/pipeline/alabamaFinance/` + `backend/src/scripts/probeAlabamaCampaignFinance.ts`.

- **Authority contract established and gated**: a race row's MONETARYCONTRIB / NONMONETARYCONTRIB / MONETARYEXP each equal the sum of the committee's filed report covers, cent-exact — verified on Jones (23 filings, incl. a Major Contribution Report filed mid-probe), Tuberville (99 filings), Boyd (15 filings, 10 amended). This replaces extract-sum reconciliation as the Phase 0 gate.
- **Extracts can UNDERCOUNT**: rows present in filed covers can be absent from every annual extract file (Tuberville: $150,000.00 cash; Boyd: $244.40 expenditures — checked 2017–2026). Extract completeness is a coverage ratio (observed 0.989–1.0), not an exactness invariant. Totals must come from the race API / covers; extracts serve contribution-size buckets only, with coverage reported.
- **2024 extract needed**: a 2025-registered committee reported a 2024-dated contribution (Boyd, $20.00) that lives in the 2024 file. Sync extract-year window = transaction-date years, not committee life.
- **Amendment semantics safe**: the filings list returns one row per report (current version); Σ covers == race even with 10 amended filings. `Amended=Y` extract rows are current versions (kept).
- **Major Contribution Report cover layout** differs: `Total Cash Contribution / Total In-Kind Contributions / Total Receipt from Other Sources`, no expenditures/ending balance. Parser handles both layouts.
- **Portal intermittently serves System Exception pages** on filing-detail fetches — retry (3 attempts) required; with it, zero unparsed covers.
- **Other Receipts extract == race OTHERSOURCES cent-exact** (Tuberville $125,319.78 = 2025+2026 files; Boyd $220.11). Interest/refund receipts live there, not in cash.
- Identity residuals (report-only): ENDINGFUNDS − (begin+cash+other−exp) = $500.00 (Jones), $324.70 (Boyd), $0.00 (Tuberville) — all three components individually cover-exact, so the residual sits in the balance columns; `cashOnHand` should prefer the filing-chain ending balance if this matters at sync time.
- Quarantine counts by the probe's parser match the earlier python probes exactly (2026 exp: 829 of 34,457; 2026 cash: 4).
- **Phase 4 live-run correction (2026-09-01) — the race row's aggregate columns are NOT cycle-scoped.** The Phase 0 fixtures (Jones, Tuberville, Boyd, later Robertson) were all 2025-registered committees, so "Σ all filed covers" and "2026-cycle money" were the same set. On an incumbent they are not: Larry Stutts (State Senate 6, committee registered 2014, 105 filings) shows MONETARYCONTRIB $655,220.53 under election 160, and a subset search over his per-filing covers reproduces it exactly as **2026-tagged reports + every Annual Report + every Major Contribution Report the committee ever filed** (cash, in-kind and expenditures all match on that same subset). Annual and Major reports carry no election tag, so the portal lumps them into whichever cycle is queried. Consequence: 127 of 183 linked candidates had extract-coverage ratios far outside tolerance (0.26–18×) in the first dry run — the extracts were right and the race row was wrong. The sync now sums the committee's filed covers whose **period begins on/after January 1 of the first term year** (`alabamaCommitteeCycleCovers.ts`; 2023-01-01 for four-year offices, 2021-01-01 for the appellate courts). On that window Stutts is $171,102.00 raised / $158,660.37 spent, equal to his extract rows, and the balance identity holds cent-exact: opening balance of the earliest window filing (any kind) + cash + other − expenditures = race `ENDINGFUNDS` = latest cover ending balance. The race row remains the roster/identity gate and the cash-on-hand source; `raceRowTotalReceipts` is kept in sync results for comparison. Two more portal facts surfaced by the full run: overdrawn balances render accounting-style, `($220.23)` (nine committees; parser now accepts it), and Major Contribution Reports are separate ledger entries that also carry a running beginning balance — a major filed before a committee's first periodic report is already inside that report's beginning balance (Robertson: $1,065,000), so the identity must open from the earliest window filing of any kind. Extract-side gaps are real but smaller than they looked: with cover-window totals, 148/203 candidates pass the coverage gate.
- **Re-run 2026-08-28 (PASSED)** after review round: enumeration extended beyond statewide offices — State Representative 193 race rows, State Senator 68, Supreme Court Associate Justice 4. **Race rows carry no district column**; a new district-join gate proved the linking path: 193/193 State Rep race rows join committee-search rows by internal id, and `jurisdiction` (e.g. "HOUSE DISTRICT 68") is populated on 192/193 — the one gap is a 06/2026 registration (Beech) with no jurisdiction or place, so the resolver fails closed to manual review on missing district. Gate tolerates ≤5% missing jurisdiction; unjoined rows stay a hard failure. Also fixed: a zero-race-total committee with nonzero extract rows now fails the probe explicitly instead of skipping the coverage check.
