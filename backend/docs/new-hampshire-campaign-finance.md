# New Hampshire campaign finance — feasibility (probed 2026-08-19)

Verdict: **buildable for Nov 2026**. All four data needs are covered by the NH SOS
Campaign Finance System (Civix CFIS — same vendor family as New Mexico's system).

## Build status (phases)

- Phases 0–2 MERGED: `backend/src/pipeline/newHampshireFinance/` — CFS client,
  CSV artifact cache + reader, raw-refresh CLI
  (`new-hampshire-candidates:finance:raw:refresh`), filer resolver, direct and
  outside-spending aggregators, snapshot writer over the `nh_candidate_finance_*`
  tables (migration 249), per-candidate sync `syncNewHampshireCandidateFinance`,
  eligible-office list, ballot-lookup loader.
- Phase 3 BUILT 2026-09-03 (operator wiring, Idaho template): auto-link
  (`newHampshireCandidateFinanceAutoLink.ts`), due list
  (`newHampshireCandidateFinanceDueList.ts`), batch sync
  (`newHampshireCandidateFinanceBatchSync.ts`), CLIs
  `npm run new-hampshire-candidates:finance:auto-link` and
  `npm run new-hampshire-candidates:finance:sync-due`
  (`--dry-run --force --max-candidates --lookback-days --lookahead-days`; the
  sync also takes `--no-auto-link --stale-after-days`). No schema change, no new
  flags: both CLIs make live CFS calls, so they use the existing live-call gate
  `NEW_HAMPSHIRE_CAMPAIGN_FINANCE_ENABLED` + `NEW_HAMPSHIRE_CFS_RAW_DATA_REFRESH_ENABLED`
  (`--force` bypasses only the second, as the raw-refresh CLI does).
  Rules: the auto-link pulls the cycle list and the cycle's filing-entity
  registry once per run, links only an unambiguous match whose registration is
  `Active` (`link_source = 'cfs_registration'`); ambiguous, unmatched, and
  non-Active registrations are reported, never written. The batch keeps the
  per-candidate sync's contract (it still re-resolves the filer live and reads
  the search API, not the bulk CSV cache) and shares the cycle list, registry,
  and IE list across links through a memoizing client; a failed link is
  recorded and the batch continues; a link whose filer no longer resolves
  counts as failed; the CLI exits 1 when any link failed. Defaults: 25 links,
  stale after 7 days, lookback 22 + 7 + 1 = 30 days (last 2026 R&E report due
  11/25), lookahead 730.
  Dry runs 2026-09-03 (local, `--dry-run --force`): both exit 0 with 0
  attempted — no Nov-2026 New Hampshire state rosters exist yet, so the roster
  gap, not the pipeline, caps coverage.
- Next: Nov-2026 NH rosters, then a real local run (auto-link, then sync-due),
  then the manual-link CLI for the unmatched, then prod.

## System

- Public SPA: https://cfs.sos.nh.gov (Angular). Akamai blocks non-browser clients
  on this host (curl → 403), but the API host is open.
- API host: **https://cfsapi.sos.nh.gov/api** — anonymous JSON, no auth, no token.
  Verified working from plain Node `fetch` (no special UA needed).
- 2026 registrations at probe time: 470 candidates, 574 political committees,
  10 candidate committees, 3 political advocacy committees.
- R&E filing deadlines 2026: 06/17, 08/19, 09/02, 09/16, 10/14, 10/28, 11/25.

## Bulk CSV downloads (money raised / spent / donor employers)

`POST https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData`
Body: `{"type":"CSV","filingYear":<YYYY>,"transactionTypeCode":"TCON"|"TEXP"}`
Filing years 2016–2026. This mirrors New Mexico's `GetCSVDownloadReport` artifact
pattern (`CON_<year>.csv` / `EXP_<year>.csv`) — reuse the NM cache/reader design.

- Receipts (`TCON`), 2026 file: 48.5 MB, 160k rows, ~$31.4M total.
  Columns: Filing Entity ID, Candidate Name, Committee Name, Committee Subtype,
  Transaction Type/Sub Type, Election Period, Election year, Date of Receipt,
  Amount of receipt, Contributor Type, Contributor Name, address fields,
  Contributor occupation, Contributor Employer, Contributor Principle place of
  Business, Description, Timed Report.
- Expenditures (`TEXP`), 2024 file: 6.2 MB, 29k rows.
  Columns: Filing Entity ID/Name/Type, Transaction Type ("Expenditure",
  "Independent Expenditure", "Return Expenditure"), Sub Type, Payee source
  type/name/address, Amount, Date, Election Type, Description, Timed Report.
- Amounts are formatted strings (`"$5,700.50"`); dates MM/DD/YYYY; quoted
  multi-line fields — use a real CSV parser.

### Donor occupation caveat (verified, gate resolved)

RSA 664:6, I requires occupation + employer + employer city once a contributor's
election-cycle aggregate exceeds $200 (statute text verified 2026-08-19). In the
export, the requirement lands almost entirely in the employer column:

- `Contributor occupation`: **0 filled even on >$200 transactions**
  (0 of 12,292 in the 2026 file; 1 of 86k across all individual rows).
- `Contributor Employer`: **96% filled on >$200 transactions** (11,776 / 12,292);
  ~80% across all individual rows (68,748 / 86,075).

Conclusion: an occupation-title chart is not shippable; employer-derived
industry classification is. Route employer strings through the existing
employer/donor industry-label pipeline; never label the result "occupation".

### Unitemized / withheld rows

Contributors at ≤$50 aggregate appear as `Under threshold - Name Withheld` —
**43% of 2026 receipt rows** (69,544 / 160,109). Keep them in totals as
unitemized money; never create donor identities from them.

### Filing year ≠ election cycle

The CSV year selector is the **filing year**. The 2024 file contains 2023
transaction dates; the 2026 file contains 2025 dates. A Nov-2026 build must
fetch at least the 2025 + 2026 filing-year artifacts, then filter: receipts CSV
has `Election year` / `Election Period`; the expenditures CSV has only
`Election Type` (no cycle column) — cycle scoping for outside spending comes
from the search API's `electionCycle` field instead.

### Amendment semantics (open item — cheap fixture required)

The bulk CSVs carry **no report identity or version column**, so an amended
report's original rows cannot be distinguished if both are exported. Signal:
2.6% of named 2026 receipt rows (2,352 / 90,146) are byte-identical duplicates
(ambiguous — could be legitimate repeat donations). The transaction search API
DOES expose `reportName`, `reportVersion`, `isAmended`, `reportVersionFilter`,
`transactionID`, and `electionCycle` per row. Before shipping totals, reconcile
a handful of filers' CSV sums against the search API (current versions); if the
CSV over-counts, resolve versions via the API. Do not build a general staging
layer for this — one fixture decides the join strategy.

## Outside spending (support / oppose)

The bulk `TEXP` CSV tags IE rows (`Transaction Type = "Independent Expenditure"`,
2,721 rows in 2024) but does **not** carry target candidate or stance — the payee
column is the vendor. Use the transaction search API instead:

`POST https://cfsapi.sos.nh.gov/api/PublicTransactionDetails/GetPublicExpenditureDetails`
with `"transactionTypeCode":"TEXP"` plus **`"transactionSearch":"TIE"`** (the IE
switch), paginated (`pageNumber`/`pageSize`, `sortBy:"TransactionDate"`).
Filters include `candidateNameByIE`, `measureNameByIE`, `fromDate`/`toDate`,
`electionCycle`.

Returns per-IE JSON with numeric `transactionAmount`, `stance`
("Support"/"Oppose"), `candidateMeasure` (target, "Last, First"), filer name +
entity id, `electionCycle`, `transactionCategory` (purpose), report linkage.
7,966 IE rows all-time at probe; 2026-cycle rows present and current
(e.g. Citizens Alliance of New Hampshire canvassing IEs filed 08/2026).

Caveat: legacy rows (~2018) often have stance/target blank — scope aggregation
to the 2026 cycle.

Note: `PublicIndependentExpenditureDetails/GetIndependentExpendituresDetails`
exists but returns `401 Bad Request.` for anonymous callers — not needed; the
TIE search above covers it.

## Candidate-to-filer registration linkage (probed 2026-08-21)

Use `POST PublicFilerDetails/GetFilingEntityDetails`, filtered by the exact
numeric election-cycle ID. Candidate and candidate-committee rows expose the
stable `filerEntityId` plus structured candidate first/last name, office, and
district fields. Candidate committees may appear as `PAC` / `PACCC` rows.

Do not link candidate committees from the bulk receipt `Candidate Name` field:
live 2026 candidate-committee exports put the committee name in that column.
Do not parse committee display text (`Friends of`, `Committee to Elect`, etc.).
Return an exact link only when official registration data agrees on candidate,
cycle, office, and every required district. Some live candidate-committee
registrations omit district; those remain unmatched rather than guessed.
State House district numbers repeat by county, so match both the registration's
county (`town` in the API response) and district number to VoteApp's
county-qualified House district name. County-office links likewise require the
official county; county-commissioner links also require its numbered district.

## Other observed API families (unprobed depth)

`PublicTransactionDetails/GetPublicReceiptsDetails` (receipts search),
Other `PublicFilerDetails/*` profile/summary routes, `Lookup/GetDropdownLookup`
(enum values), `PublicFiledReportAndDownload/*` (download-page metadata).

## Legal facts verified against RSA text (2026-08-19)

- RSA 664:6, I — occupation/employer/employer-city required above $200
  cycle aggregate (quoted clause checked at gc.nh.gov).
- RSA 664:6, III — IEs over $1,000 aggregate: file within 48 hours, again per
  additional $1,000; must name the candidate, state behalf-of/against, and
  allocate multi-candidate expenditures on a reasonable basis. Matches the
  portal's Stance / Candidate-or-Measure fields; use the official stance field,
  never parse stance from purpose text; exclude blank-stance rows from totals.
- RSA 664:6, VII — 501(c)(4)/(5)/(6) committees may withhold donor identities.
  Only matters if outside-group funding profiles are built later; label as
  legally withheld, never backfill from outside sources.
- RSA 664:9-a — online filing mandatory for governor / executive council /
  state senate and political committees since 2024-11-27, but for **state house
  and county offices only from 2026-11-25 — i.e. AFTER the Nov 2026 election** —
  plus a waiver for <$3,000 filers. Expect real coverage gaps for House and
  county candidates this cycle (470 candidates registered in CFS vs a much
  larger House field). Show "no filing in state system" rather than $0 for
  unmatched House candidates.

## Suggested build shape

Clone the New Mexico module layout (`backend/src/pipeline/newMexicoFinance/`):
year-keyed CSV artifact cache → direct-contribution aggregator (employer → 
industry labels) → outside-spending aggregator fed by the TIE search API →
snapshot writer. Candidate linkage via roster auto-link against official filer
registration, as in other standard states. Never use bulk receipt committee-name
text as candidate identity evidence.
