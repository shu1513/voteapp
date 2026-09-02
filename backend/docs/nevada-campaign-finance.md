# Nevada campaign finance — feasibility (probed 2026-08-26)

Verdict: **partially buildable.** Money raised and money spent are solid and
reconcile to the official report summaries. **Donor occupation does not exist in
Nevada at all** (not collected by statute), and **outside spending cannot be
attributed to a candidate or to a support/oppose stance** — Nevada's expense
schedule has no target-candidate and no stance field.

## System

- Single statewide system: **AURORA**, NV Secretary of State. State, county and
  city filers all file here (jurisdiction dropdown lists Carson City, Clark,
  Washoe, Las Vegas, Reno, Henderson, …), so one adapter covers every NV office.
- Public search: `https://www.nvsos.gov/SOSCandidateServices/AnonymousAccess/CEFDSearchUU/Search.aspx`
  ASP.NET WebForms (Telerik grids), no auth, no API. Four search modes selected
  by the plain `search_type` form field:
  `#individual_search`, `#group_search`, `#contribution_search`, `#expenditure_search`.
- Coverage at probe: **1,956 individuals** for election year 2026; group type
  `Indep` (independent-expenditure filers) has 104 registrations all-time.
- 2026 deadlines: Annual CE 01/15/2026; CE#1 04/15 (Jan–Mar); CE#2 07/15
  (Apr–Jun); CE#3 10/15 (Jul–Sep); CE#4 01/15/2027 (Oct–Dec).

### Blocker: Imperva/Incapsula WAF

`www.nvsos.gov` sits behind Imperva. Plain `curl` and plain Node `fetch` with
full browser headers both get the 212-byte `_Incapsula_Resource` JS challenge
(verified 2026-08-26). Everything below was proven from **page-context `fetch`
inside a real browser**, which works fine. A server-side adapter needs a browser
tier (or the manual-research browser route); no documented public API was
found. The challenge response's size varies between probes (212 and 929 bytes
observed same-day) — detect the `_Incapsula_Resource` marker, not a byte size.

## Money raised / money spent — GOOD

Two independent paths, and they reconcile exactly.

### 1. Report summary (authoritative, includes unitemized)

`ViewCCEReport.aspx?syn=<token>` renders the filed C&E as HTML with numbered
summary lines, each with **This Period** and **Cumulative from report period #1**
columns:

- 1 monetary contributions > $100, 5 in-kind > $100, 7 all contributions ≤ $100,
  **8 total of all contributions**
- 9 monetary expenses > $100, 10 in-kind expenses > $100, 11 expenses ≤ $100,
  **12 total of all expenses**
- 13 ending fund balance

Live example (Joseph Lombardo, Governor, 2026 CE Report 2, filed 07/15/2026):
line 8 = $2,284,488.47 this period / $4,611,656.69 cumulative-since-CE#1
(election-year-to-date, NOT the cycle: his 2025 money sits in the separate
2026 Annual CE Filing, line 8 $4,479,163.91 — see Addendum 3 for the
cycle-total rule); line 12 = $3,338,289.89 / $4,442,270.12; line 13 balance
$9,197,364.58.

Report tokens are found on `CandidateDetails.aspx?o=<token>&y=<year>`, reachable
from the individual-search results grid.

### 2. Itemized CSV export (donor/payee level)

Both transaction searches have an **"Export Results to CSV Only"** checkbox
(`chkContExportOnly` / `chkExpExportOnly`). Posting the form with it returns
`text/csv` directly — no download UI needed.

- Contributions CSV: `Contributor, Date, Amount, Type, Recipient, Report`
  (`Type` = `Monetary Contribution` | `In Kind Contribution`)
- Expenditures CSV: `Payee, Date, Amount, Type, Payer, Report`
  (`Type` = `Monetary Expense` | `In Kind Expense`)

Reconciliation check (Lombardo, 04/01–06/30/2026): CSV = 864 rows summing
**$2,275,074.69**, exactly line 1 ($2,169,835.15) + line 5 ($105,239.54). The
gap to line 8 is line 7, the $9,413.78 of ≤$100 unitemized money. So: itemized
CSV for donor lists, summary lines for headline totals.

### Mechanics / gotchas

- **Result cap.** Over-broad searches return "This search returns too many
  results." *including in CSV mode*. Slice by date range: 4,299 rows in one
  request succeeded, so the cap is above that. Quarter-by-quarter slicing per
  candidate is comfortably safe.
- **Amendments replace, they do not duplicate.** A 2022 Q3 export returned only
  `2022 CE Report 3 (Amended)` rows — the superseded original is not in the
  result set. No version-resolution layer needed (recheck on a second fixture).
- **Legal Defense Fund money is separate** and tagged in the `Report` column
  (`… (Legal Defense Fund)`). Exclude it from campaign totals — it is what makes
  a naive expense sum overshoot line 9+10.
- Amounts are formatted strings (`"$5,000.00"`), dates `M/D/YYYY`.
- The CSV has **no address**; addresses appear only in the rendered report HTML.
- The CSV has **no expense category**; the report HTML carries NRS 294A.365
  category codes. **The letter map is per-form-version**: the live 2026
  candidate form uses A–O (A office, B volunteers, C travel, D advertising,
  E paid staff, F consultants, G polling, H special events, I legal defense
  fund, J personal security, K contributions to other candidates/nonprofits/
  PACs/recall committees, L candidacy filing fees, M loan repayment/
  forgiveness, N disposal of unspent contributions, O miscellaneous), while a
  2012-form report renders A–L. Never derive letters from statutory paragraph
  order; read the category table off each report.
- Date filters post as Telerik RadDatePicker triples (`…$dateInput` plus the
  `_dateInput_ClientState` JSON); the amount filters are RadNumericTextBox and
  are fiddlier — prefer date slicing.

## Donor occupation — NOT AVAILABLE, and not fixable

**NRS Chapter 294A contains zero occurrences of "occupation" or "employer"**
(full chapter text checked 2026-08-26). Nevada requires only name and address of
each contributor over $100. The rendered itemized schedule confirms it: the
column header is "NAME AND ADDRESS OF PERSON, GROUP OR ORGANIZATION WHO MADE
CONTRIBUTION" plus date and amount — nothing else. The CSV export has even less.

Do not ship an occupation chart for Nevada, and never label anything derived
here as "occupation".

Partial substitute — **entity-donor industry labels**. Nevada allows direct
corporate/union/PAC giving, so a large share of the money arrives under an
organization name that the existing employer/donor industry-label pipeline can
classify (`labelType: "donor"`, as in Austin). Measured on Lombardo 04–06/2026
with a conservative suffix regex (LLC/Inc/Corp/PAC/Union/Fund/…): **26.9% of
rows and 39.3% of dollars** are entity donors; true share is higher, since many
Nevada casino/company donors carry no legal suffix. Individual donors remain
permanently unclassifiable — there is no employer string to work from.

## Outside spending support/oppose — NOT AVAILABLE

Nevada registers IE filers (`group type = Indep`, 104 all-time) and NRS 294A.210
requires anyone making independent expenditures over $1,000 to report them —
subsection 11 even says a multi-candidate IE "must be itemized by the candidate."
But the filed artifact does not carry it:

- IE filers file the same **EL 202 C&E report** as everyone else. Its monetary
  expense schedule is *payee name/address + NRS 294A.365 category code + date +
  amount*. There is **no target-candidate field and no support/oppose field**
  anywhere in the form (verified on IE-type filers AFSCME 2012 CE#4 and Educate
  Nevada PAC 2024 CE#4).
- The expenditure search returns the same shape: `Payee | Date | Amount | Type |
  Payer | Report`. The payee is the vendor, so an ad buy attacking a candidate is
  indistinguishable from office rent.

Consequences:

- A Nevada state-race candidate page can show raised/spent, and can show which
  organizations gave **to** the candidate, but cannot show outside money for or
  against them. Do not synthesize a stance from payee or category text.
- Federal NV races (US Senate / US House) are unaffected — FEC independent
  expenditures already carry `support_oppose_indicator` through the existing
  OpenFEC path.
- The one legitimate NV-state signal is direct PAC/party contributions to the
  candidate, which are already in the contributions data as ordinary donors.

## Suggested build shape (SUPERSEDED — see `docs/plans/nevada-finance.md`; the
plan uses statewide month-sliced CSVs, not the per-candidate quarter slices
sketched below)

1. Roster/link layer: individual search (`search_type=#individual_search`,
   `txtLastName`, `ddlElectionYear`) → results grid gives Name | Party |
   Jurisdiction and a `CandidateDetails.aspx?o=<token>&y=<year>` link; the detail
   page adds Office. Link only on candidate + office + jurisdiction + year
   agreement. Token stability across sessions is **unverified** — re-resolve by
   search rather than persisting `o=` as a long-lived key.
2. Totals: parse the newest non-LDF report per cycle for summary lines 1–13
   (cumulative column = cycle to date).
3. Donor detail: quarter-sliced contribution CSV exports, LDF rows dropped,
   entity donors routed to the donor/industry classifier.
4. Expenses: quarter-sliced expenditure CSV exports (aggregate only; no stance).
5. Outside spending: **out of scope for NV state races** — record it as a
   documented data-source gap, not a TODO, and show "not disclosed in Nevada"
   rather than $0.
6. Transport: browser tier, because of Imperva.

## Addendum — cross-validation (2026-08-26)

A second independent feasibility report (researched 2026-08-03) was checked
point-by-point against live AURORA. All of its cited figures reproduced exactly:

- Cannizzaro (AG) 2026 CE#2: all eight summary figures cent-exact.
- Save Nevada NOW (IE filer) 2026 CE#2: $1,220,000.00 raised /
  $1,166,702.62 spent; itemized funders present (RSLC $50k, HAWK PAC $60k, …);
  no support/oppose/target field anywhere.
- Better Nevada PAC 2026 CE#2: $12,012,500.00 / $4,392,296.21. The string
  "INDEPENDENT EXPENDITURE" appears only as free text typed inside a payee
  name block — filer annotation, not a structured field.

Corrections/additions to this doc from that pass:

1. **`syn=` report tokens are stable across sessions** (2026-08-03 links still
   resolved 2026-08-26). Safe to persist as report identifiers. (Supersedes the
   earlier "token stability unverified" note for report links; candidate
   `o=` tokens still unverified.)
2. **Two form layouts.** Candidate form: $100 itemization threshold, summary
   lines 1–13, includes a ≤$100 unitemized aggregate line. PAC/IE/party form
   (EL 202): $1,000 threshold, summary lines 1–8, **no unitemized line**.
   Parsers must branch on filer type — line numbers do not align.
3. **Result pages also expose Excel / CSV / PDF exports** via
   `lbExportExcel` / `lbExportCSV` / `lblExportPDF` postbacks, in addition to
   the CSV-only checkbox on the transaction searches.
4. Adopted recommendations from the second report: phase-0 email to NV SOS
   (sanctioned bulk route + whether any nonpublic candidate-target/stance field
   exists — NRS 294A.210 does not require one, so expect "no"); stage
   outside-group funders (NRS 294A.140 rows are real and itemized ≥$1,000) at
   group level only, never as candidate edges; take cash-on-hand only from the
   report summary, never computed from itemized rows; keep donor street
   addresses out of public API/UI; gold-set reconciliation gate before publish
   (statewide + legislative + judicial + county + city + loans + in-kind +
   amended + LDF filer).

Gaps in the second report (it missed, this doc keeps): the Imperva/Incapsula
WAF (server-side fetch is blocked — its "session-aware client" plan does not
work), the result cap firing in CSV-export mode (date slicing mandatory), the
Legal Defense Fund parallel filings that must be excluded, and the
transaction-search behavior of returning only amended rows (report *lists*
show both versions; the search path self-resolves).

## Addendum 2 — deep probe for the implementation plan (2026-08-26)

Plan doc: `docs/plans/nevada-finance.md`. New verified facts:

- **Cookie replay does not defeat the WAF**: a curl session that accepts the
  Incapsula cookies from the challenge response still gets the 212-byte
  challenge on the next request. Browser-tier acquisition is mandatory.
- `ddlJurisdiction` posts **numeric option values** (`36` = NV SOS, `21` =
  Clark County, `19` = Carson City, …) — posting the display text silently
  returns no grid.
- Individual search, year 2026 + jurisdiction 36: **195 filers** (vs 1,956
  across all jurisdictions). SOS-jurisdiction scope covers statewide +
  legislature + state judicial.
- `CandidateDetails.aspx` shows office **with district** ("State Assembly,
  District 32") — enough for exact roster linking; the results grid itself has
  only Name | Party | Jurisdiction.
- Transaction searches work **date-only** (no name filter): 6/15/2026 alone =
  134 rows statewide. Full-state harvest by month slice is therefore possible.
  June 2026 = 4,322 rows exported fine; Apr–Jun 2026 hits "too many results"
  (cap binds in CSV mode too, somewhere above ~4.3k rows). Month slices,
  halved on cap, are the harvest unit.
- Second amended fixture (Alexis M Hansen, Assembly 32, 2024 CE#4 amended
  1/10/2026): date-sliced search returned **only** `2024 CE Report 4
  (Amended)` rows — replacement semantics reconfirmed.
- `Report` column zoo observed: `… (Amended, Legal Defense Fund)` combined
  tag, `Annual CE Filing` rows mixed into contribution results, and a filer-
  typed junk name (`Tick`). Treat report names as opaque; only the
  `Legal Defense Fund` suffix is parsed (for exclusion).
- Group/PAC/IE form confirmed to have **no ending-balance line** (candidate
  form line 13 only).
- The results-page CSV/Excel/PDF export postback did not reproduce via a
  reconstructed fetch POST (returned HTML); the checkbox-based CSV export on
  the transaction searches is the reliable export path.

## Addendum 3 — Phase 0 fixtures + semantics answers (2026-08-26)

Fixtures harvested into `backend/tests/fixtures/nevadaFinance/` (see its
README). Five candidates: Lombardo (Governor, LDF filer), Cannizzaro (AG),
A. Hansen (Assembly 32), Krasner (Senate 16), Herndon (Supreme Court Seat D).
**Exit criterion met: 5/5 reconcile cent-exact** once the ≤$100 rule below is
applied.

Answers to the two Phase-0 questions:

1. **Annual CE Filing semantics**: period == cumulative on every line (checked
   Lombardo + Cannizzaro annuals). Annual filings are self-contained and sit
   outside the CE#1–4 cumulative chain; the chain restarts at CE#1 each
   election year (CE#1 cumulative == CE#1 period; CE1+CE2 periods == CE2
   cumulative, verified twice). So cycle totals = Σ "This Period" lines 8/12
   across annual filings + CE reports in the window; cash = line 13 of the
   report covering the latest period end (rule tightened in Addendum 4 — the
   newest *file date* is not safe, since a late amendment to an old quarter
   can be the newest filing).
2. **CSV↔summary reconciliation**: 4/5 candidates exact at lines 1+5 (and
   9+10). Krasner over by exactly line 7 ($264.01) / line 11 ($364.22) —
   **filers may itemize ≤$100 transactions**, so the gate is
   `lines(1+5) ≤ CSV Σ ≤ lines(1+5+7)` (resp. 9+10 vs 11). CSV sums are never
   headline totals. Lombardo's LDF expense rows separately equal the LDF
   report's line 9 ($64,307.93).

New facts found during harvesting:

- Detail-page report grid columns are **Report Name | Year | File Date |
  Office | Report Link** — Year is the period/election year (the "2026 Annual
  CE Filing" carries Year 2025), and Office is stamped **per report row**.
- **Profile Office ≠ candidacy office**: Cannizzaro's profile says "State
  Senate, District 6" (current seat) while her 2026 report rows say "Attorney
  General". Roster linking must read the office off the Year-2026 report rows
  (or the FDS row), never the profile header.
- Cannizzaro's 2026 Annual CE Filing was amended 8/25/2026 with identical
  totals (itemization-only amendment) — amended selection must key on report
  name + Year with newest file date, not on totals changing.
- Herndon (judicial): zero contributions; empty CSV export = header-only file.
- Expenditure CSV payees can carry doubled-quote escapes (`"""Anedot"""`) —
  use a real CSV parser; legitimate duplicate rows exist; refunds show as
  `(REFUND)` expense rows; "Accrued Account Interest" appears as a contributor.
- June 2026 full-state exports: contributions 4,322 rows / $19,463,778.06 /
  464,234 bytes; expenditures 4,207 rows / $13,324,706.56 / 420,222 bytes
  (sha256 in the fixtures metadata). Date-only exports include ALL
  jurisdictions and filer types, so one harvest serves candidates and PACs.

## Addendum 4 — external review cross-check (2026-08-26)

A second reviewer's critique was checked point by point; body text above was
corrected where it was stale. Verified outcomes:

- **Expense categories**: live 2026 candidate form = **A–O** (verified on the
  Lombardo CE#2 form; body text corrected). The A–L table earlier in this doc
  came from a 2012-form report. Letters are per-form-version; the plan doesn't
  consume categories, so impact is documentation-only.
- **Challenge size is unstable** (212 and 929 bytes observed the same day;
  reviewer saw 1,037). Detect the `_Incapsula_Resource` marker.
- **NRS 294A.370** ("Media to make certain information available") and
  **NRS 294A.3733** (electronic-filing exemption) both exist.
  `CEFDSearchuu/BrowseReports.aspx` (paper/PDF archive browse) responds 200
  with current-year folders. V1 scope = searchable electronic filings only;
  an unmatched or exempt filer shows as "no filing found in the electronic
  system", never $0.
- **Loans**: the itemized monetary schedule carries a loan checkbox and routes
  loan rows to summary lines 1/2/3; the CSV export has **no loan flag**. All
  five Phase-0 fixtures have lines 2/3 = 0, and plain (unguaranteed) loans
  land inside line 1 invisibly. The shared state-finance summary contract has
  no loan or debt field, and existing adapters don't strip loans — so
  `totalReceipts` = line 8 (the state's own "Total Amount of All
  Contributions") stands, with a Phase-2 gate: find at least one loan-flagged
  fixture, measure whether lines 2/3 or flagged rows are material for
  Nov-2026 candidates, and only then decide whether any adjustment is worth
  building. There is no `debts_owed` field to populate.
- **Outside-spending phrasing**: NRS 294A.210 does require IE reporting (and
  per-candidate itemization for group IEs); what's missing is the field in the
  public EL 202 artifact. Standard phrasing everywhere: "candidate target and
  support/oppose direction are not available in Nevada SOS report data" (not
  "not disclosed in Nevada"). NRS 294A.370 advertiser records held by media
  sellers could in principle carry candidate linkage — manual, decentralized,
  out of scope.
- **Counts**: 1,956 / 195 are search-result filer records for the election-year
  filter (some records elsewhere carry UNKNOWN jurisdiction), not a candidate
  census. The VoteApp roster remains the linking authority; AURORA is matched
  from it, never the reverse.
- **Cash-on-hand rule tightened** (plan updated): line 13 comes from the report
  covering the **latest period end** (latest effective version of that
  period), not the newest file date — a late amendment to an old quarter can
  be the newest filing (Hansen's CE#4-2024 was amended 1/10/2026).
- Result-grid Excel/CSV/PDF export links exist but the chosen path doesn't use
  them (checkbox CSV export + report HTML only); reconstructed postbacks were
  unreliable.
- Group-level IE-funder staging (floated in Addendum 2) is **deferred**: with
  no candidate edges, candidate UI can't use it; revisit only if standalone
  group pages ever exist.
- Rejected review point: "Phase 0 must run from the deployed hosting
  environment." The design has no deployed fetcher — artifacts are harvested
  in an interactive browser session and imported by a local CLI; nothing in
  production ever fetches nvsos.gov.

## Addendum 5 — Phase 2 harvest + local run (2026-08-27)

Full-cycle harvest (roster 287 filers, 218 reports.json, 270 ViewCCEReport
pages, 36+ month-sliced CSVs 2025-01..2026-06) and local import: **95 of 108
eligible Nov-2026 candidates imported** (97 auto-linked, 2 quarantined).
New live facts, all handled in code or artifacts:

- **The CSV export SILENTLY truncates at 5,000 rows.** A capped export returns
  exactly 5,000 rows with NO error (proven: 2026-04 full-month = 5,000 vs
  2,321+2,994 split; 2026-05 = 5,000 vs 6,352 split). The louder "too many
  results" error fires only at some higher threshold (~8k). Harvests must
  treat rows >= 5,000 as capped and split the range.
- **Jurisdiction filter misses filers.** ddlJurisdiction=36 (y2026: 196 rows)
  covers mostly statewide/judicial filers; sitting legislators register under
  their COUNTY (Cannizzaro = CLARK COUNTY) and challengers under UNKNOWN.
  Roster harvest = jurisdiction-36 sweep PLUS last-name searches (all
  jurisdictions) for unmatched candidates; 91 supplemental entries added.
- **Candidate `o=` and report `syn=` tokens contain literal %xx sequences**
  (canonical value is the single-encoded-looking string). Grid hrefs carry
  them double-encoded; requests must send the double-encoded form. Sending
  the single-decoded form renders a DIFFERENT page or an empty profile —
  verify lblCandidateName on every fetched detail page.
- Report-row Office strings are filer-typed free text off jurisdiction 36
  ("Nevada State Assembly District 6", "Treasurer, State of Nevada",
  "legislature district 27"); the resolver's parser accepts the variants and
  fails closed on chamber-ambiguous or out-of-jurisdiction text.
- **Negative amounts**: itemized rows use accounting parentheses
  ("($2,500.00)" = rejected deposit/refund; 70 rows in the cycle CSVs), and
  summary lines can net negative ("$ -2,375.00" on line 7). Parsers accept
  both; reversal rows net into totals but stay out of breakdowns.
- **Written commitments** appear as CSV Types "Written Commitment" /
  "In Kind Written Commitment" (14 rows; summary lines 4/6) — excluded from
  received-money sums and breakdowns.
- **Loans sit in the CSV as unflagged Monetary rows** (Gomez: CSV == lines
  1+5+2 cent-exact). Reconciliation bounds are now
  [lines 1+5 + min(0, 7),  lines 1+2+3+5 + max(0, 7)] with a 1% tolerance
  (filers misdate rows across cycle bounds and file schedules that exceed
  their own summaries by tens of dollars). **Loan gate: 2 of 97 linked filers
  carry line-2 loans (Duncan $2,000, Gomez $13,195)** — material only to
  their own small totals; totalReceipts stays the official line 8.
- **Same-day duplicate amendments exist** (Jovan Jackson filed CE#1 and CE#2
  amendments twice on the same date). The transaction-search rows identify
  the effective document (CE#2: $60,015 doc matches, $120,030 doc is a
  superseded double-count); the artifact keeps one row per logical report
  with a tie_note.
- Unlinked/quarantined (13 of 108, documented fail-closed): Jauregui (2026
  AURORA rows still carry her Assembly seat, VoteApp candidacy is Lt. Gov),
  Pickering (goes by middle name), Torres-Fossett (married name vs AURORA
  maiden name), Davila (compound surname), Willett + Vaughan (no 2026
  filings), Cortes/Eady/Fuentes/C.Miller/Sandoval (no chamber+district in
  free-text office rows), Barnhill (negative ending balance -$4,000; schema
  keeps cash_on_hand >= 0), Carvalho (reports all zero but search rows exist).
