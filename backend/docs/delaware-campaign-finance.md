# Delaware campaign finance — feasibility (probed 2026-08-26; cross-validated vs independent 08-03 report)

Verdict (updated after the Phase 0 run, 2026-08-26): **buildable for totals
raised/spent — proven cent-exact against report covers.** **Occupation:
voluntary + partial (~24% of statewide 2024 individual rows)** — no legal duty
(§ 8030(d)(2); HB 291's employer/job-title requirement deleted by HA 2 before
signing 10/23/24), but the free-text field is genuinely populated by many
filers; publish disclosed-only with a coverage note (this corrects two earlier
drafts that called the field dormant based on one committee). **Outside-group
stance: committee-level structured, transaction-level MISSING** — see below.

## System

- Portal: **https://cfrs.elections.delaware.gov** — "Campaign Reporting
  Information System", PCC/CRIS family (same vendor as Maryland MDCRIS).
  Classic ASP.NET MVC + Telerik grids. No auth, no CAPTCHA, curl-friendly.
- Flow is session-based: POST the search form (cookie jar required), then GET
  the CSV export, which streams the **full result set** of the stored search.
- Data back to ~2003. 2024 receipts statewide: 36,718 rows.

## Endpoints (all verified 2026-08-26)

- Registrant autocomplete: `GET /Public/FindRegistrants?q=<name>` →
  `Name(Status)|MemberId` lines. `Meyer for Delaware(Active)|558171`.
- Receipts search: `POST /Public/ViewReceipts?theme=vista` with the full field
  set (`txtReceivingRegistrant`, `MemberId`, `FilingYear`, `ContributorType`,
  `ddlEmployerOccupation`, `dtStartDate/dtEndDate`, …). Missing fields →
  `Unable to process the request.` — send every field, blank if unused.
- Receipts CSV: `GET /Public/ExportCSVNew?page=1&orderBy=~&filter=~&Grid-size=15&theme=vista`
  → full CSV of the session's search. Columns include Contribution Date,
  Contributor Name/Address, Contributor Type, **Employer Name, Employer
  Occupation**, Contribution Type, Amount, CF_ID, Receiving Committee, Filing
  Period, Office, Fixed Asset.
- Expenses search: `POST /Public/OtherSearch?theme=vista` (NOT /ViewExpenses —
  that 302s to HandleUnknown). Fields: `txtRegistrant`, `MemberId`,
  `filingYearData`, `expenseCategoryData`, `hdnTP`, ….
- Expenses CSV: `GET /Public/ExportExpensestoCsv?...same query...`.
- Committee registry: `POST /Public/Search?theme=vista` (from /Public/ViewCommittees),
  then JSON `POST /Public/_ViewCommittees?theme=vista` (`page`,`size` form
  body) → full registry rows: MemberID, CommitteeName, CommitteeType,
  OfficeSought, DistrictName, County, status, treasurer. CommitteeType codes:
  01 candidate, 02 PAC, 03 political, **04 = 3rd Party Advertiser**, 05 cert-of-intention.
- Filed reports list: `POST /Public/ViewFiledReports?theme=vista` (works with
  the full field set). Report PDFs download directly:
  `GET /Public/FiledReports?FileName=<opaque>.pdf&CommitteeID=<memberID>&FilingCalendarID=<id>`
  — real text-extractable PDFs (not scans), page 2 = STATEMENT OF ACCOUNT
  BALANCE (beginning balance, receipts, expenditures, ending balance) and
  schedules carry GRAND TOTAL lines → official cover totals available as a
  reconciliation source for the itemized sums.
- Grid JSON endpoints (`/Public/_ViewReceiptsCustom`, `/Public/_ViewExpensesCustom`)
  work for paging but their `total` field is **unstable/garbage** (varies with
  page size and repeats). Use the CSV export as source of truth — its row count
  matched the search page's `total:` exactly (7,425 for Meyer).
- Grid JSON rows DO expose a stable internal `Transaction_id` (receipts) /
  `Transaction_Id` (expenses); the CSV export does not. If row identity
  matters, join CSV rows to grid JSON or fingerprint
  (date+name+address+amount+type) with collisions resolved by report version.

## Totals raised / spent — YES (cent-exact, PROVEN against covers 2026-08-26)

Phase 0 result: Meyer's receipts CSV ($3,961,664.59) and expenses CSV
($3,388,153.05) reconcile to the cent against the summed canonical report
covers (max PDF-footer `Version:` per `FilingCalendarID`, 9 sequential
non-overlapping periods, continuous balance chain). The transaction search
returns **current-version rows only** — no amendment dedup needed on the
itemized side. "View Current Report" on the filed-reports search returns only
the single most recent report (not per-period). CSV exports are NOT RFC CSV
(unquoted fields, literal `"` data) — parse by literal line/comma split.


Delaware filings itemize everything, including aggregate rows
(`Contributor Type = "Total of Contributions not exceeding $100"`), so summing
the receipts CSV = complete total raised; same on the expense side.
Verified on Meyer for Delaware (Gov 2024, CF_ID 01005311):

- Receipts: 7,425 rows, **$3,961,664.59** (6,939 Individual; also Corporation,
  PAC, Candidate Committee, Self, Labor Union, national sub-committee types).
- Expenses: 1,254 rows, **$3,388,153.05**. Columns: payee name/address/type,
  amount, category, purpose, method, filing period, fixed asset.

## Donor occupation — PARTIAL, voluntary (~24% of 2024 individual rows)

- Law: § 8030(d)(2) requires contributor name, mailing address, aggregate,
  amount, date — **no occupation/employer duty**. HB 291 (2024) originally
  added employer + job title; House Amendment 2 (passed 6/18/24) deleted that
  before enactment.
- **REVISED 2026-08-26 (Phase 0 statewide export)**: the fields are NOT
  dormant. 2024 statewide: **6,681 of 27,974 individual rows (23.88%) carry
  Employer Occupation**, 5,211 carry Employer Name — free text, voluntary,
  extremely uneven per committee (Meyer for Delaware: 0 of 6,939 — the
  earlier "dormant" verdict generalized from that one committee). The 29-code
  occupation DROPDOWN undercounts massively (Government 2024 = 79 rows)
  because it filters on the coded taxonomy, not the free-text column.
- Module decision (plan hard fact 1): publish the occupation breakdown from
  non-blank values verbatim with a coverage note (voluntary disclosure);
  store employer text unpublished; never classify occupation text into
  industries.

## Outside spending / stance — registration edge YES, expenditure edge NO

- 71 registered 3rd Party Advertiser committees (type 04), e.g.
  "A Better Delaware PAC-IE" (MemberId 535968, 129 TP expense rows).
- **Structured stance EXISTS at the registration level.** The TP statement of
  organization (`GET /Public/ShowReview?memberID=<id> &memVersID=<v> &cTypeCode=04
  &ftype=SO &fpath=` — note the literal spaces before `&`) renders an
  "Affiliated Candidate Information" table: candidate committee, candidate
  name, office, party, **Position = Support | Oppose**, status. Verified:
  Citizens for a New Delaware Way (memberID 642221, CF 04006103) lists
  Oppose Bethany Hall-Long / Support Matthew Meyer (Gov 2024).
- **The expenditure→candidate edge is missing.** TP expense rows (search:
  `OtherSearch` with `chkTP=3&hdnTP=1`; value must be `1`, not `on`; broad
  year-only TP searches return total:0 — search by registrant) carry payee /
  amount / category only, no candidate, no stance. Verified in the filed PDFs
  too: CNDW's 07/22/2024 report = $243,160 ($228,160 Tusk Strategies +
  $15,000 Shine Creative) with NO per-candidate split despite registering
  opposite positions on two candidates. Full-report allocation to affiliated
  candidates would be inference — a two-affiliation report can be contrast
  ads, separate ads, or shared overhead.
- **Affiliation table can be empty even when spending targets a candidate.**
  DLGA PAC (memberID 643731, CF 04006142): affiliation "No records to view",
  yet its 07/18/2024 report shows $475,000 in from People for a Healthy
  Delaware and $90,000 to Fortune Media Inc with the free text "Bethany for
  Governor" in the payee cell. Never parse payee text as stance.
- Legal note: 15 Del. C. § 8031(a)(2) DOES require TP reports to name each
  candidate on whose behalf the expenditure was made — the duty exists, the
  CFRS filing artifacts just don't structure it. Worth asking the Dept of
  Elections for a transaction-level extract (expenditure id → candidate +
  position); that would unlock per-candidate outside totals.
- Safe policy if built: publish outside support/oppose ONLY where a single
  active affiliation (or an official extract) uniquely attributes the amount;
  otherwise null (never $0). Single-affiliation committees are usable as a
  provisional rule at best.
- HS1 HB 216 (153rd GA) would expand TP/funder disclosures with a proposed
  2028-07-01 implementation; pending in House Appropriations as of 2026-05 —
  not a current source contract.
- Note scope: federal 2026 races (US House, US Senate) are FEC territory, but
  DE CFRS covers real 2026 STATE statewide races — Attorney General, State
  Treasurer, Auditor of Accounts (all on the official 2026 general candidate
  list) — plus General Assembly + county/muni/school races.
- TP funders ARE disclosed (§ 8031(a)(3), contributors > $100): verified
  Schedule A rows in both test PDFs — group-funder staging is feasible.

## Adapter sketch (if built)

1. Committee registry sweep (type 01, active) → auto-link by OfficeSought/
   DistrictName + candidate name.
2. Per committee: POST receipts search by MemberId (no date filter) → CSV →
   sum + subtype buckets by Contributor Type; same for expenses.
3. Amount filters (`txtAmountRangeFrom/To`) returned total:0 even for valid
   ranges — avoid them; filter client-side.
4. Skip occupation aggregates and structured IE stance (see above).
