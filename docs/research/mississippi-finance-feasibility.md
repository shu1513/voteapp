# Mississippi campaign finance — feasibility findings

Probe date 2026-08-19. Everything below was verified live against the
Secretary of State portal and the official forms; nothing is inferred from
another state's module.

**Verdict: do NOT build a Mississippi finance adapter.** The disclosure law is
strong, but the machine-readable data stops in 2023 and every filed report is a
scanned image. See "Recommended route" for the manual alternative.

## Why not

1. **Itemized data cliff.** Contribution rows by transaction year: 2019 =
   48,625 rows / $78.3M, 2021 = 4,735, 2022 = 6,371, 2023 = 11,990 / $6.3M
   (thin for a statewide cycle), **2024 = 0, 2025 = 1, 2026 = 0**. Expenditures
   match (2023 = 2,402 rows, 2025 = 0). The SOS stopped converting filed PDFs
   into searchable rows; the portal itself only promises searchability for
   electronically filed reports.
2. **The biggest races are the holes.** E-filing is optional. The 2023
   Reeves/Presley governor race (~$25M combined) has no itemized rows —
   Reeves' rows stop in 2019, Presley's in 2020. Branning's 2024 Supreme Court
   race returns 0 rows for 2024.
3. **No machine-readable covers.** Five filed reports sampled across 2020–2026
   (annual, periodic, independent expenditure) are all image-only PDFs with a
   zero-length text layer, and the content is **handwritten**. There is no
   text-extraction path and deterministic OCR templates will not survive
   handwriting, so no cover-total reconciliation and no automated summary
   (contrast Georgia/NC, where covers carry the authoritative arithmetic).
4. **No structured outside-spending edge.** The expenditure service returns
   filer / recipient / description / date / amount only — no target candidate,
   no support-oppose field. Stance exists only inside the scanned IE form.

## What the source does offer

- **Public JSON service**, unauthenticated, no WAF trouble, no server paging
  (a full 2019 query returns all 48,625 rows in one response):

  ```text
  POST https://cfportal.sos.ms.gov/online/Services/MS/CampaignFinanceServices.asmx/{op}
  Content-Type: application/json
  ops: CandidateNameSearch, ContributionSearch, DistrictSearch, ExpenditureSearch,
       GetFiledFilingsV2, GetRelatedEdocuments, GetViewDocumentLink,
       LateAndNonFilersSearch, RelatedEdocumentsSearch
  ```

  Responses are double-encoded: the ASP.NET `d` property holds a JSON string
  whose `Table` array carries the rows. Payload shapes are readable in
  `/Online/UserControls/Searches/CampaignFinance/CampaignFinanceSearchScript.js`;
  `ContributionSearch` takes `{EntityName, Description, BeginDate, EndDate,
  AmountPaid, InKindAmount, CandidateName, CommitteeName, ContributionType}`.
- **Entity + filing history** (undocumented, but stable enough to enumerate):

  ```text
  /Online/ViewXSLTFileByName.aspx?providerName=CF_CandidateDetails&EntityId=<uuid>
  /online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=<uuid>
  ```

  The details page carries party / office / election year plus every filing with
  date, report name, status and `FilingId` (in the `data-val` attribute of the
  View Filing link). The workflow URL returns the report PDF.
- **Occupation** is present and well populated where rows exist — 9,581 of
  10,381 individual contributions in a 2019 H1 sample. There is no employer
  column, and `ContributorType` is dirty converted text (real values include
  `Small (See PDF Image) Aggregator` and `United Liability Company`).

## Facts that are easy to get wrong

- **An empty transaction search is not $0.** Filing history stays current past
  the data cliff: Presley's 2023 periodic reports, an `- Amended` row and a
  `Not Filed` placeholder are all listed while his 2023 contribution search
  returns nothing. Any future work must check filing history before writing a
  total.
- **Occupation vs employer is disjunctive for candidates.** Miss. Code Ann.
  § 23-15-807(d) requires "the occupation or employer of the contributor" on
  candidate itemized receipts — which is why the portal has one mixed
  `Occupation` column rather than a lossy merge of two fields. Independent
  expenditure reports are stricter: § 23-15-809 requires occupation **and**
  employer. The blank schedule form labels both "(Required)" and overstates the
  candidate-side statute.
- **Independent expenditure filings do exist and are current.** They are a
  distinct filing type on the details page, still being filed through 10/2025
  (Improve Mississippi PAC: 65 filings 2013–2026, including the 2024 Supreme
  Court and 2025 special cycles). The form carries support/oppose checkboxes,
  candidate name, spender, period and calendar-YTD totals, itemized receipts and
  itemized disbursements. It is readable by eye, not by parser.
- **One IE filing can carry two candidates and both stances.** A live
  10/28/2025 Improve MS PAC filing checks both boxes and writes
  "Jen Lancaster - Support / Justin Crosby - Oppose" on the candidate line.
  The handwriting is ambiguous: the official SOS candidate record and sample
  ballot identify the first candidate as **Jon Lancaster**. A one-stance-per-
  filing contract would either drop or mislabel it.
- **Purpose of Disbursement is optional on the form** and is frequently blank.
- **Filing office splits by race.** Statewide, state-district, legislative and
  state judicial candidates file with the Secretary of State. County and
  county-district candidates file with the Circuit Clerk, municipal candidates
  and committees with the Municipal Clerk, and the campaign finance guide says
  electronic filing is not available there. Central coverage can only ever mean
  SOS-filed races.

## Recommended route

For the Nov-2026 SOS-filed Mississippi races (judicial plus specials), use
manual research rather than a pipeline: enumerate filing history for each linked
candidate or committee, read the scanned PDFs, and inject through the
`voteapp-manual-research` finance payloads. The volume is manual-scale — a busy
PAC is tens of filings of a few pages each.

Revisit the adapter question only if the SOS resumes converting filings into
searchable rows, or publishes a bulk extract. Neither exists today.

## Sources

- <https://cfportal.sos.ms.gov/online/portal/cf/page/cf-search/Portal.aspx>
- <https://www.sos.ms.gov/elections-voting/campaign-finance>
- 2026 Campaign Finance Guide —
  <https://www.sos.ms.gov/content/documents/elections/2026/2026%20Campaign%20Finance%20Guide-%20Final.pdf>
- Itemized Contributions schedule (Rev. 02-2020) —
  <https://www.sos.ms.gov/sites/default/files/elections/Itemized%20Contributions%282020%29.pdf>
- 2026 Judicial Independent Expenditure Report (Rev. 11/2025) —
  <https://www.sos.ms.gov/sites/default/files/elections/2026%20Judicial%20IE%20Report.pdf>
