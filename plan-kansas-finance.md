# Kansas Campaign Finance Plan

Written 2026-08-26, revised same day after adversarial review. Based on a live probe of the Kansas SOS CFR viewer and the KPDC scanned archive, with every load-bearing claim verified against primary sources (statute text, filed PDFs, live systems, VoteApp's own ballot data). Companion facts doc: `backend/docs/kansas-finance-feasibility.md`.

Verdict: buildable. Viewer-first architecture — NOT PDF-first. The "last updated March 15, 2022" search on the KPDC site is a legacy app (`kansas.gov/ethics/EthicsSite/`); the live source is the SOS CFR viewer, which is public and current (contribution rows verified through 7/23/2026, IE filings listed through 8/20/2026).

## Verified sources (probed live 2026-08-26)

### Primary: SOS CFR viewer (`https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx`)

ASP.NET WebForms postback app. Access rules proven end-to-end with curl:

- Default curl UA → 403. A browser UA works. Keep a cookie jar.
- Every POST must round-trip `__VIEWSTATE`, `__VIEWSTATEGENERATOR`, `__EVENTVALIDATION` from the previous response, HTML-unescaped before re-encoding.
- Report identity lives in server session state — no report id appears in any URL, and grid postback targets (`grdviewCfrResults$ctl02$…`) are row positions on one result page, NOT durable identifiers. Persist the *search recipe* (name, office, district, filing type, filed/amendment dates) plus the artifact itself; each sync re-runs the search and re-walks rows. Single session per walk, concurrency 1 within a session. Parallel sessions are unproven — Phase 0 tests 2–3 concurrent sessions before the plan assumes them.

Categories and what each yields:

1. **Contribution search** (`ddlViewerOptions=Contribution` → `cfr_examiner_contribution.aspx`). Statewide candidates only (mandatory e-file since 2010-01-10). Fields: `txtContributorName, txtCandidateName, txtContributorCity, ddlStates, ddlContributionType, txtCashAmount, txtStartDate, txtEndDate` (MM/DD/YYYY). Results page `btnExport` → `Contributions.xls`, an HTML `<table>` of the search's result set — verified complete at 4,285 rows / 6.9 MB. **No cap was observed at that size; that does not prove no cap exists.** Phase 0 tests a much larger result set; the fallback is per-candidate-per-period exports (the natural shape anyway). Row span-ids: `lblCandName, lblContributor, lblAddress, lblAddress2, lblCity, lblState, lblZip, lblOccupation, lblIndustry` (observed always empty), `lblDate, lblTypeofTender, lblAmount, lblInKindAmount, lblInKindDescription, lblStartDate, lblEndDate`.
2. **Expenditure search** — same shape for statewide Schedule C (not yet exercised; Phase 0 item).
3. **Candidate Campaign Filings** (`ddlViewerOptions=Candidate` → `cfr_examiner.aspx`). ALL offices: `txtFirstName, txtLastName, drpdownOffice` (State Representative, State Senator, Governor, State Board of Education, District Attorney, …), `txtDistrictNo, drpdownFilingType` ("Receipts and Expenditures Report", "Appointment of Treasurer", "Affidavit of Exemption Candidate", "Last Minute Contribution", "Termination Statement", …), filed-date range. Enumeration verified: 354 State Rep R&E reports filed 7/1–8/26/2026. Result rows open either:
   - **native HTML report** (e-filed): `reports/exp_report_main.aspx` cover with SUMMARY lines 1–7, then `__doPostBack('lnkbtnSchedule{A,B,C,D}View','')` → itemized schedules incl. occupation column and unitemized-total lines (verified on Helwig, State Rep D1), or
   - a PDF icon (paper filing) → scanned PDF, same artifact as the KPDC tree.
4. **Individual Entity** → filing type "Individual Expenditures": lists dedicated IE statements (316 records all-time; Aug-2026 filings appear here before the KPDC scan tree). The statements themselves are scanned PDFs even here.

### Secondary: KPDC scanned archive (`https://www.kansas.gov/ethics/CFAScanned/...`)

Plain-HTML link trees per office family and cycle: `House/2026ElecCycle/HLinks2026EC.htm`, `StWide/2026ElecCycle/SWLinks2026EC.htm`, `Senate/2026SpecialElection/SLinks2026SpecialElection.htm` (exists — Seat 24 filings live now), `Others/<cycle>/IndependentExpendLink.htm` (IE), `PACs/...`. Broad archive incl. paper filings, but **completeness is not guaranteed** — scanning lags (Aug IE filings visible only in the viewer) and the PAC index itself warns some reports appear on the site only after scanning. Treat the viewer's filing enumeration as the freshness/completeness reference and the tree as an artifact source. Every PDF here — including e-filed reports — is print-then-scan with a noisy OCR layer. Filename grammar: `<officecode><initials>_<period>.pdf` (`SW01CH_202607`, `S24TA_AT`), `_AT` appointment, `_2026PLF` last-minute, `Aff` affidavit, `amend` prefix. `kansas.gov` 302s to `www.kansas.gov` — always follow redirects.

### Historical only: legacy contributor search

`kansas.gov/ethics/EthicsSite/` search, linked from KPDC "Campaign Contributor Data" — frozen 2022-03-15, data back to 1993. Never mix into the current pipeline. Optional fixture source.

## Statutory + calendar facts the code must encode

- **K.S.A. 25-4148(a)**: reporting periods per election phase. 2026 calendar (official KPDC due-dates sheet): report due 1/10/2026 covers 1/1/2025–12/31/2025; due 7/27/2026 covers 1/1/2026–7/23/2026; due 10/26/2026 covers 7/24/2026–10/22/2026; due 1/10/2027 covers 10/23/2026–12/31/2026. Candidate last-minute contribution reports ($300+): windows 7/24–7/29 (due 7/30) and 10/23–10/28 (due 10/29). Off-years: annual January-10 report only — **a year with no periodic reports can be `not_required`, not `missing`**.
- **K.S.A. 25-4148**: itemize contributions over $50 aggregate; ≤$50 may be an unitemized lump. Itemized sums therefore NEVER equal cover totals — totals come from covers only (verified gap: Holscher itemized $389,246.47 + $4,803.67 in-kind vs cover $412,630.21).
- **K.S.A. 25-4148a**: occupation filed for individual contributions over $150; if the contributor is not employed for compensation, the SPOUSE's occupation is filed. Caption: "Occupation reported for the contributor, or the contributor's spouse when Kansas law requires it." Occupation sometimes appears voluntarily below $150 (Helwig: "Retired" on $100) — use when present; absence below $150 is not missing data.
- **K.S.A. 25-4150** (rewritten by HB 2206, 2025): dedicated IE statement from any person other than a candidate/party/political committee at $1,000+ aggregate per calendar year; vendors itemized above $500 aggregate; each row carries candidate, office, and explicit supported/opposed.
- **K.S.A. 25-4148(c)** (subsection of the report statute): PAC/party *regular* reports name each candidate who is the subject of an in-kind contribution over $300 or an independent expenditure over $300 — but the blank Schedule C has one shared column with no direction field and no independent-vs-in-kind marker.
- **K.S.A. 25-4148c** (distinct statute, easy to confuse with the above): party/political committees making $300+ of independent expenditures during the final 11-day pre-election window must file *last-minute IE reports* — one due the Thursday before the election, plus DAILY reports for the Thursday–Sunday immediately preceding. Names the candidate whose nomination/election/defeat is expressly advocated. These rows later reappear in the committee's next regular report.
- **K.S.A. 25-4154(d)**: no one may copy contributor names from filed reports and use them for a commercial purpose (class A misdemeanor). This plan makes NO judgment about whether any VoteApp use is commercial — especially with paid memberships launching. Engineering posture: contributor names stay in restricted raw staging only, never in published surfaces, breakdowns, or API output; occupation/size aggregates carry no names; any future name-bearing feature (e.g. outside-group funder lists) requires counsel and, ideally, KPDC guidance FIRST. Get the counsel/KPDC review of the whole ingestion pattern started in Phase 0, not at ship time.
- Affidavit of exemption: candidates expecting under $1,000 in and out per campaign phase file an affidavit instead of reports — `affidavit_exempt`, never a synthetic $0.

## Scope

November 2026, in priority order:

1. **Statewide** (Governor, AG, SOS, Treasurer, Insurance Commissioner) — mandatory e-file, contribution/expenditure export. Cycle window 1/1/2023–12/31/2026.
2. **State House** (all 125) — cycle window 1/1/2025–12/31/2026. Mixed e-file and paper.
3. **State Senate SPECIAL elections** — 2026 has them (VoteApp DB already carries D24 and D25 special primaries on 8/4/2026; KPDC has a live `Senate/2026SpecialElection` index for Seat 24). Exact district list comes from final ballot data at sync time, not from this plan. Regular Senate cycle is 2028.
4. **State Board of Education odd districts** and **District Attorneys** where VoteApp carries the race.

Out of scope for v1: county/municipal offices (filed with county election officers, no central repository), outside-group donor/funder name lists (25-4154(d) gate + noncommittee IE forms disclose no receipts), committee-to-committee transfer tracing.

## Identity and matching rules

Candidate link resolution (fail closed):

1. Normalized name or verified alias, exact office family, exact district when districted, compatible cycle/special.
2. Anchor on a viewer Candidate-filings search recipe (or KPDC index row for paper-only filers); persist recipe, filed name/address, artifact URLs/hashes, and per-period filing status. Never persist postback coordinates as identifiers.
3. Two plausible matches → no link. Never match on name alone.

Every money row passes a person/entity classification gate before aggregation: individuals (occupation-eligible) vs organizations (PAC/party/firm/union — excluded from occupation buckets). Ambiguous names quarantine rather than guess.

Outside-spending target resolution: candidate name + compatible office; exact district when the row gives one (`HD101 CASEY SLAUGHTER SUPPORT`). District absent + more than one plausible candidate → quarantine. Direction only from the filed Supported/Opposed value (or explicit support/oppose/elect/defeat purpose text) — never inferred from committee name, ideology, or slate.

## Aggregation rules

All money arithmetic in **integer cents**. Reconciliation checks are exact equality, no float tolerance.

- **Totals raised/spent = cover SUMMARY lines** (2 receipts, 4 expenditures, 6 in-kind), summed across every authoritative period in the office's cycle window. Self-check every cover: line 1+2=3 and 3−4=5 exactly, else quarantine the report. Cash on hand = line 5 of the latest authoritative report, never summed.
- **Publish gate is per candidate**: a candidate ships only when 100% of their money rows parsed cleanly and every period is accounted for (`report_filed | amended | affidavit_exempt | not_required | terminated | not_yet_due`). Any `missing_or_late | failed_extraction | ambiguous` row → that candidate stays on last-good snapshot. No global "99% is fine" bar.
- **Amendments**: the one verified example (Perry `H003DP_amend2607`, in-kind $223.28→$463.28) was a full replacement report, and the *working hypothesis* is replace-not-add — but the pipeline VERIFIES it per report (amendment's own cover must reconcile; supersede only when the amendment is a complete report for the same period) instead of assuming it. A partial/delta-shaped amendment fails closed pending a form-specific rule.
- **Last-minute filings** (candidate PLF and 25-4148c committee reports) duplicate into the next regular report. Use for freshness; dedupe on arrival of the regular report.
- **Occupation breakdown** from itemized individual rows only. Aggregate dollars, not counts. Preserve `Retired`, `Not Employed`, `Homemaker`, `Student`, `Farmer`, `Self-employed`; blanks/illegible → Unknown; conservative normalization only (free-text typos like "Ownere" stay unless the fix is unambiguous). Report coverage alongside: occupation-covered itemized-individual dollars over (a) itemized-individual dollars and (b) all direct dollars — the unitemized lump is excluded from buckets and surfaces as coverage metadata, never as an "Unknown occupation" bucket.
- **Contribution size buckets** from itemized transactions only.

### Outside spending — THREE paths, one dedupe

1. **Dedicated IE statements** (25-4150, noncommittee spenders; `Others/` tree): explicit per-row Support/Oppose. **"Total this Period" is a cumulative control total WITHIN one reporting period and RESETS at period boundaries** — verified: Kansas Comeback statements inside 1/1–7/23 run 370,443.63 → 378,943.63 → 383,943.63, then the first statement of the 7/24–10/22 period (`IE_KC4_2607`) resets to 138,270.00. Sum unique rows; validate against the within-period running totals; never add total lines; never treat the reset as a correction. Masterson oppose fixture through 8/26: **$522,213.63**.
2. **Committee last-minute IE reports** (25-4148c; final-11-day window, Thursday + daily filings): candidate named, express advocacy. Rows reappear in the committee's next regular report — dedupe forward.
3. **Regular PAC/party Schedule C** (25-4148(c)): candidate named over $300 but no direction and no independent-vs-in-kind marker; verified duplicating path 1 (PAC869 repeats OnMessage $359,633 / O'Donnell $10,810.63 / $8,500 / $5,000 against "Ty Masterson" with no direction).

Policy: dedicated statements are authoritative for candidate+direction. Paths 2 and 3 are inventory: matched on filer+date+vendor+amount+candidate they are duplicates (corroboration, not money); unmatched rows enter totals only with explicitly directional AND explicitly independent purpose text; otherwise quarantined. Multi-candidate spends without per-candidate amounts are excluded (Koch GA fixture: $1,544.08 / 34 unnamed candidates ≈ $45.41 each, unitemized per KPDC advice). Coverage state per candidate: `complete_for_explicit_rows | partial_unresolved_direction | partial_unallocated | none_found | source_unavailable`; only the first feeds ordinary totals; `none_found` ≠ zero.

IE statements are scanned PDFs with disqualifying OCR noise (`$58.741.00`) — v1 transcribes them with verification (amounts checked against zoomed page images, checksummed against within-period running totals). Volume small: 28 PDFs on the 2026 index, 316 filings all-time.

## Architecture

```
backend/src/pipeline/kansasFinance/
  kansasCfrViewerClient.ts            # session mgmt, UA, viewstate round-trip, category flows, export fetch
  kansasCfrViewerParsers.ts           # export table, cover HTML, schedule A/B/C/D HTML, lookup grids
  kansasKpdcIndexClient.ts            # CFAScanned link trees
  kansasFinanceArtifactCache.ts       # url, sha256, fetched_at, kind, period, supersession; immutable versions
  kansasFinancePdfExtractor.ts        # OCR-layer reader for paper filings + IE statements; page confidence; quarantine
  kansasReportInventory.ts            # per-candidate period ledger and statuses
  kansasCandidateCommitteeResolver.ts
  kansasCandidateFinanceAutoLink.ts
  kansasDirectContributionAggregator.ts
  kansasOutsideSpendingAggregator.ts  # three-path dedupe, allocation/coverage states
  kansasFinanceEligibleOffices.ts
  kansasFinanceWriter.ts              # createStandardStateFinanceSnapshotWriter wrapper
  kansasCandidateFinanceSync.ts
  kansasBallotLookupFinanceLoader.ts
  index.ts
```

Inventory statuses: `report_filed | amended | affidavit_exempt | last_minute | terminated | not_required | not_yet_due | missing_or_late | failed_extraction`. `not_required` covers off-year annual-only periods and pre-candidacy periods; `terminated` follows a termination statement. A blank historic period is only `missing_or_late` when the calendar says a report was actually due.

Snapshot writer populates the standard tables via a new migration (`ks_candidate_finance_links`, `ks_candidate_finance_summaries`, `ks_candidate_finance_direct_breakdowns`, `ks_candidate_finance_outside_groups`, `ks_candidate_finance_outside_group_breakdowns`; identifiers ≤63 chars; next free migration number — check open PRs, Nevada #885 may take 257; never renumber). Kansas staging additionally keeps: per-period cover ledger (integer cents), unitemized totals, coverage-state enum, OCR confidence, supersession graph. No outside donor/industry rows in v1.

CLI scripts follow house convention: `kansas-candidates:finance:probe`, `kansas-candidates:finance:link`, `kansas-candidates:finance:sync-due`, `kansas-candidates:finance:raw:refresh` (mirror Missouri/RI/NH naming).

## Phases

### Phase 0 — DONE 2026-08-26 (live run green, zero failures)

Implemented: `backend/src/pipeline/kansasFinance/` (`kansasCfrViewerClient`, `kansasCfrViewerParsers`, `kansasFinancePdfText`, `kansasPhaseZero`) + `backend/src/scripts/probeKansasCandidateFinance.ts`, npm script `kansas-candidates:finance:phase-zero`, 39 unit tests. Live results:

- **Exports**: Holscher contributions 4,285 rows = page count, $389,246.47 itemized, occupation dollar-share 85.5%; Schmidt (Insurance Comm.) 759 rows / $269,517.89 / 80.1% (includes two `($4,000.00)` credit-card refund rows — amounts can be accounting-style negatives; the probe fails on any unparsed amount). **Expenditure flow works** (form fields `txtEntity/txtCandidateName/txtCity/ddlStates/ddlExpenditureType/txtAmount/txtStartDate/txtEndDate`, results page `cfr_examiner_expenditure_results.aspx`; Holscher 311 rows $527,362.41).
- **Cap test**: all-statewide 1/1–7/23/26 search = 15,876 records, export returned all 15,876 (25.8 MB, 23 candidates). No cap observed at that size.
- **Enumeration**: House 354 R&E filings; grid shows 20 rows/page (pagination via `__doPostBack('grdviewCfrResults','Page$N')` — page-1-only in Phase 0, **Phase 1 client must page**). Senate: 9 filings (specials confirmed in viewer), 7 e-file / 2 paper. Paper rows carry `<img id="…_paper_N" title="Paper Filing">`; e-file rows carry `lblDate_N` + name postbacks.
- **Helwig walk**: cover arithmetic + Schedule A/C totals cent-exact. Schedules are plain GETs (`reports/schedule_{a,b,c,d}_report.aspx`) once the report is in session; the cover's schedule postbacks 500 but are unnecessary.
- **OCR covers**: rotation-aware pdfjs extraction added (scans are 90°-rotated; y-grouping scrambles lines without it). Label-anchored recovery + identity check: **2/5 recovered** (both e-file-print scans; one via an uncertain-read validated by the identities). The 3 paper-leaning scans failed → per the plan gate, **paper filers go to a bounded manual-transcription queue**, not automated OCR.
- **Concurrency**: 2 parallel sessions both succeeded.
- **Perry fixture**: both covers recover, receipts identical ($2,550.00) → full-replacement confirmed mechanically.
- **Kansas Comeback fixture**: within-period running totals + period reset validated; oppose total exactly **$522,213.63**; all four artifacts contain the transcribed amounts and Oppose/Masterson.
- **Koch GA fixture**: confirmed unallocated (no district-coded rows), $1,544.08 present.
- Client facts locked in code comments: UA must start with `Mozilla/5.0` (403 otherwise — honest compatible token used); POSTs must echo ALL hidden fields (`__VIEWSTATEENCRYPTED` omission = 500); navigation is POST→302→GET with session cookies; 500 "Runtime Error" = bad postback, never retried; record count read from `<span id="lblRecordCount">`.
- Remaining from the Phase 0 list: legal/KPDC 25-4154(d) review kickoff (user action).

### Phase 0 — original checklist (for reference)

1. Contribution export for two statewide candidates; assert full parse and occupation coverage math.
2. **Expenditure search + export** (unexercised flow). If export is absent/capped → per-report Schedule C HTML fallback; record the answer here.
3. **Export cap test**: run a search returning ≥20k rows (e.g. all contributions in a wide date range); confirm export completeness against the on-screen record count.
4. Candidate-filings enumeration for all 125 House districts + Senate specials: classify e-file vs paper per filer; measure the real paper rate (probe sample was 3/12).
5. Walk one e-filed legislator end-to-end (cover + Schedules A–D), reconcile cover arithmetic in integer cents.
6. Parse 5 paper-filer PDFs: how many covers reconcile from OCR alone vs need transcription.
7. **Concurrency test**: 2–3 parallel viewer sessions; record whether the app tolerates them.
8. Fixtures, each asserted: Helwig (Schedule A occupation + cover reconcile), Perry amendment pair (replacement verified, no double count), Kansas Comeback four statements + PAC869 (**oppose total exactly $522,213.63**, within-period running totals validated, period reset honored, cross-source dedupe, no cumulative-total addition), Koch GA (excluded, `partial_unallocated`).
9. Kick off the counsel/KPDC 25-4154(d) review of the ingestion + publication pattern.

Gate: covers reconcile exactly on every e-filed fixture; the Comeback number lands exactly; export completeness confirmed at scale. Paper-cover OCR reconcile below ~90% → paper filers become a bounded manual-transcription queue, not a lowered bar.

### Phase 1 — clients + artifact cache — DONE 2026-08-28 (live smoke green)

Implemented: grid paging (`collectKansasCfrGridPages`), `kansasKpdcIndexClient` (index trees, PDF fetch, filename grammar), `kansasFinanceArtifactCache` (immutable versions + supersession, `scratch/kansas-campaign-finance/` gitignored). Facts locked in live:

- **Pager is a sliding window** (1–10 + "..."): a far page's link is not rendered, so the walk is sequential — each `Page$N+1` postback fires from page N's own hidden fields and answers 200 (not 302) with fresh state. Row indexes restart per page and a row's postback target is only valid against its page's hidden state, so pages are returned whole. Verified: House R&E 07/01–08/26/2026 = 18 pages / 354 rows exact (304 e-file / 50 paper), 18 distinct page-firsts. Grid has no period column — one candidate's same-day filings for two periods parse identically.
- **KPDC trees carry dead-host links**: 91 of the House tree's 810 links (all `amend*`, all `Aff*`, a few reports) are absolute `http://ethics.ks.gov/CFAScanned/...` URLs; the artifacts are live at `www.kansas.gov/ethics/CFAScanned/...` (verified), so the client rewrites those hosts instead of dropping them. Filename grammar classified all 810 live House links with zero unknowns (AT 261+5 amend, report 377+78 amend, PLF 78, Aff 10, Term 1); IE tree = 27 links incl. Oct (`_2610`) filings.
- **Cache**: byte-identical re-store is a no-op keeping the original manifest; changed bytes write a new immutable version with `supersedes` = prior sha; sha256 verified on every read; 0700/0600 modes (exports carry 25-4154(d) PII).

### Phase 2 — parsers + inventory — IN PROGRESS

Every parsed report carries: period, type, amendment relation, filing channel (viewer-html | kpdc-pdf), extraction confidence. Period ledger driven by the official due-date calendar above.

**Step 1 DONE 2026-09-01 — Schedule A itemized rows** (`parseKansasScheduleARows` + `checkKansasScheduleA` in `kansasCfrViewerParsers.ts`). Validated live on 11 e-filed reports (8 House, 3 statewide incl. a 3,349-row / 4.5 MB Governor schedule), every one cent-exact: row Amounts = `lblTotalItemized`; itemized + unitemized + political materials + unknown = `lblTotalReceipts` = cover line 2. Test fixtures use invented names only (25-4154(d)). Facts locked in live:

- Schedule A renders ALL rows on one page (no pager at 3,349 rows). One `<tr>` of seven `<td>`s per row; only address/zip carry id-stamped spans (`Repeater2_lblAddress_N` / `Repeater2_lblZip_N`); the name is the free text before the first `<br />` (a person filed through the form's first/last fields renders as two source lines, an entity as one — a hint, not a classification).
- "Primary Total" / "General Total" are the contributor's running phase aggregates; row money is the "Amount" column only.
- Tender values seen: `Cash`, `Check`, `Credit Card`, `E Funds`, `Loan`, `Other`, `Refund`. `Refund` rows are POSITIVE receipts (vendor money back to the campaign; 14 rows / $2,951.42 on one Governor report, no occupation) and `Loan` rows are candidate loans — both count in receipts but are not contributions, so Phase 4 classifies by tender before occupation/size buckets. No negative Amount was seen on Schedule A.
- A report can carry every receipt on "Sale of Political Materials (Unitemized)" with zero itemized rows (HD116, $2,538.28).
- Candidate last-minute reports appear in the "Receipts and Expenditures Report" grid as ordinary rows whose cover period is the PLF window (HD5, 7/24–7/29/2026) — the ledger classifies a filing by its cover period, never by grid filing type.
- The results page's hidden state stays valid for successive row postbacks (11 covers opened from one results page) — a sync needs one search per candidate, then one postback + one schedule GET per report.

**Step 2 DONE 2026-09-02 — period ledger** (`kansasReportInventory.ts`: `kansasReportingPeriods`, `kansasLastMinuteWindows`, `buildKansasReportLedger`). Calendar derived from K.S.A. 25-4148(a) (verified against the statute text on ksrevisor.gov): pre-primary 1/1 → primary−12 due primary−8, pre-general primary−11 → general−12 due general−8, post-general general−11 → 12/31 due 1/10, and one annual report for every other cycle year ("only the annual report ... for those years when the candidate is not participating in a primary or general election") — reproduces the official 2026 sheet exactly. Election dates computed (primary = first Tuesday in August, general = Tuesday after first Monday in November; the 2026 Senate specials share them). A special election runs on the short cycle KPDC files it under — the `Senate/2026SpecialElection` archive starts at the 2025 annual, not 2023 — so the sync passes `cycleStartYear` for specials instead of the office's term length. Validated live against every Governor-office filer 2023–2026 (27 filers, 76 report rows, 31 AT rows, 2 affidavits) and House districts 1–5 (20 filers): every 2026 candidate's ledger is complete; the incomplete ones are non-candidates, paper filers (period comes from the KPDC filename, not yet wired), and one cross-office history (a 2024 legislative post-general report listed under Governor) that correctly stays in manual review. Facts locked in live:

- **Every version of a report is its own grid row**: an amended e-filed report keeps the ORIGINAL file date in `lblDate`, carries `lblAmendmentDate`, and its cover has `chkAmended` checked; two amendments can share one day (O'Hara: original + two 1/12 and 1/15 amendments). Versions are ordered by amendment date (else file date), amendment before original on a tie; the latest is canonical. Two unflagged originals, an original after an amendment, or two amendments on the same day (Ward, 2/9/2023 — one with the termination box, one without) cannot be ordered and are `ambiguous`.
- A **termination** (`chkTermination`, may sit on an amendment) counts only from a period's CANONICAL version — a superseded original's checkbox proves nothing. It closes the committee; periods after it are `terminated` only until a later original Appointment of Treasurer or filing reopens it (Colyer: terminated with an amended 2023 annual, reappointed 5/12/2025, filed 2025 annual + 2026 pre-primary → 2024 annual `terminated`, everything else filed).
- The Appointment of Treasurer grid's **"Amendment No." column** (`lblAmendmentNo_N`) is blank on the ORIGINAL appointment and numbered on every later change (Rogers 8/28/2024 blank, 9/18/2024 #1; Kelly's in-window appointments are #6 and #7). Periods that ended before the first in-window ORIGINAL appointment are `not_required`; amended appointments prove nothing about when the committee began, and a filed report that ended before the original appointment also makes it continuing — either way every period is owed.
- Filings for periods that ended before the cycle start (2022 reports filed January 2023) are `outOfCycleFilings`, not unexpected; their termination flag still counts (Ward). A filing matching no period and no last-minute window is `unexpectedFilings` and fails the ledger.
- An affidavit exempts unfiled periods whose due date is on/after the affidavit's file date (Reinecker: AT 6/9/2026, affidavit 7/26/2026 → all three 2026 periods exempt); a period already overdue when the affidavit arrives stays `missing_or_late` (manual review).
- The e-file system gives a 2024 candidate election-year periods (`10/25/2024–12/31/2024` = 2024 post-general) — a filer who switches office keeps that history under the new office in the viewer.

**Step 3 DONE 2026-09-02 — ledger wiring** (`kansasFilingSearch.ts`: shared enumeration whose rows keep an `openReport` handle + per-run pool loader; `kansasCandidateLedger.ts`: `buildKansasCandidateLedger` per linked candidate; `kansasCandidateFinanceDueList.ts`: standard due list on the `ks_*` tables; read-only CLI `kansas-candidates:finance:ledger` (`--force`, `--max-candidates`, `--lookback-days`, `--lookahead-days`, writes nothing). Identity at sync time is the link's recipe, not the roster name: the resolver runs with "FIRST SURNAME" from `committee_id` under the same fail-closed rules, so operator (manual) links work unchanged; nickname families are symmetric so a recipe built from `BRUNK STEVEN` still folds `BRUNK STEVE`. Live run 2026-09-02 on 12 of the 128 House links (13 min incl. one House enumeration): 12 resolved, 0 unresolved, 0 errors; 9 complete; the 3 incomplete are exactly the paper filers. Facts locked in live:

- **One enumeration per office serves every candidate**: a results page's hidden state stays valid for row postbacks after other reports were opened, across pages and in any order (Governor, 4 pages, opened 1-3-2-1-4). So the sync reuses the Phase 3 office-wide pool (57+15+3 House pages) and opens only the linked candidate's own rows — no per-candidate search.
- **Paper rows carry no period in the viewer.** Their NAME link answers 500 (no HTML report — never fire it). Their pdf link's postback answers 200 with a `window.open('https://sos.ks.gov/srvimages/campaignfinance/filings/cyYYYY/cmMM/<id>.pdf')` whose path is the FILING month, not the period, and whose bytes differ from the KPDC CFAScanned artifact of the same filing (Muter HD2: 383 KB vs 1,057 KB; no hash pairing). The viewer also leaves `lblAmendmentDate` blank on both Muter rows although KPDC holds `H002DM_202607` + `H002DM_amend2607`. Paper reports are therefore returned unopened as `paperReports` and keep the candidate incomplete until the **KPDC index row** (district + "Last, First" + filename tokens) is wired as their inventory — next step.
- Cover names render "First [Middle] Last" (`Stacy  Rogers`, `Charlotte I O'Hara`); the grid row is the identity, the cover is only checked for landing URL + period.
- **PR #1053 review fixes (2026-09-02)**: (1) the ledger's identity is the recipe PLUS the link's `committee_name` (`kansasLedgerCandidateName` -> "SURNAME, FIRST [MIDDLE...]"): the recipe alone aligned "HOLLOWAY JOHN B" for a link verified against "HOLLOWAY JOHN A" and went ambiguous; free-text committee names fall back to the recipe. (2) `kansasCfrCycleStartYear` derives the cycle start from the office calendar: Senate even off-years (2026 D24/D25) are specials on KPDC's short cycle (2026 -> 2025); both the enumeration window and `kansasReportingPeriods` default to it, so no caller passes `cycleStartYear`; other off-year races throw (fail closed).

**Step 4 DONE 2026-09-02 — paper inventory from the KPDC trees** (`kansasPaperInventory.ts`; `parseKansasKpdcCandidateRows`, `kansasKpdcCandidateTreePath`, `kansasKpdcStatewideFilerPrefix` in the KPDC client; date-less versions in the ledger). Facts pinned live:

- **Tree paths** (KPDC "View Submitted Forms and Reports" page): `House/<y>ElecCycle/HLinks<y>EC.htm`, `Senate/<y>ElecCycle/SLinks<y>EC.htm` (presidential years) or `Senate/<y>SpecialElection/SLinks<y>SpecialElection.htm` (2014/2018/2022/2026), one `StWide/<y>ElecCycle/SWLinks<y>EC.htm` for all five statewide offices, sectioned by heading with filer codes SW01 Governor / SW02 AG / SW03 Insurance / SW04 SOS / SW05 Treasurer (NOT the viewer codes). The 2026 Senate special tree holds only three ATs (D24).
- **The tree lists e-filers too** (Helwig HD1 e-files; his 202601/202607 scans are in the tree), and a report token is the DUE month (202601 = 2025 annual due 1/10/2026). So the tree is a per-filer inventory of every report, and an e-filer's opened covers are subtracted by (due month, amended) before the remainder counts as paper.
- **Both sources miss filings**: Stiens HD39's 202607 + PLF scans are in the tree but absent from the viewer's R&E grid; Muter HD2's 1/13/2026 paper row is in the viewer while the tree shows N/A for 202601. Rule: tree links give the periods (a scan is a filing); the candidate is complete only when the tree explains at least every viewer paper row (`viewer rows <= tree versions due inside the window`, no unmapped filenames).
- **Hand-authored markup drops `<tr>` between some filers** (2026 House: "33 Smith, Romona ... 33 Woody, Eli"; "111 Wasinger / 112 Brantley / 112 Froetschner"), so rows are walked cell by cell: a 1-3 digit cell starts a filer, a "Last, First" cell names it (or starts the next on the statewide tree), links attach to the current filer. Verified on all eight trees: zero orphan links, 267 House-2026 filers vs 263 `<tr>` rows. Filer-code digits are NOT the district (Shultz HD73 files as H108LS; Woody moved to D31) — the district cell is.
- **Filename grammar completed**: `2amend`/`3amend`/`4amend` prefixes number later replacements (ordinal orders date-less versions; two same-ordinal undated versions, or an undated one against a dated one, are a tie -> ambiguous), `YYYYGLF` is the general last-minute report beside `YYYYPLF`, affidavits also appear lowercase (`aff2407`). Zero unknowns over all eight trees; odd tokens (`202404`, `200422`) stay unmapped and keep that filer incomplete.
- **Prior-cycle tree is read too** (cycle start year - 1): the prior post-general (due January 10 of the cycle's first year) is filed inside the enumeration window, so the viewer shows it and it must be explained; only versions due on/after the window start are taken from either tree. Paper amendments in the viewer carry blank `lblAmendmentDate` (Simmons: two 1/10/2026 rows = 202601 + amend2601), so the tree is the only ordering.
- **Live run 2026-09-02 (all 128 House links, 71 min incl. one enumeration retry)**: 116 complete / 12 incomplete / 0 unresolved / 0 errors. 24 candidates carry paper rows; 17 reconcile (Simmons 4=4 with the 2025-annual amendment ordered by the amend prefix; Stiens 2<=3; Ballard 4=4; Ward and Ruiz mixed-channel with 2 and 3 covers subtracted). The 7 paper incompletes are tree gaps, each reported: Shultz/Melton/Estes have viewer rows the tree never links (Estes's row has no links at all), Amyx/Bryce list `amend2601` twice and no original, Harlin has no 2025 annual in either source, and 5 e-filers plus Schmoe (two unflagged pre-primary versions) are viewer-side. The first full run failed closed on `collected 1129 rows but page reported 1128`: a filing landed during the 57-page walk and shifted every later page by one; the immediate rerun was clean, so Phase 4's sync should retry that one mismatch once.
- Not covered (fail closed, reported): the trees are consulted only when the viewer shows paper rows (the 5 e-filers missing a 2025 annual have nothing extra in the tree either); paper affidavits/appointments still come from the viewer grids only; a filer with more viewer paper rows than tree versions, an unmapped filename, or no tree row stays incomplete.

Remaining: Schedule B rows (in-kind individuals) and Schedule C rows (only for the Phase 5 PAC path); KPDC PDF classification (cover recovery exists from Phase 0; itemized rows from OCR are out of scope — paper filers get totals only and no breakdowns).

### Phase 3 — resolver + auto-link — DONE 2026-09-01 (live dry run + real run green)

Implemented: `kansasFinanceEligibleOffices` (v1 office map -> viewer office codes, cycle windows), `kansasCandidateFilerResolver` (pure matcher), `kansasCandidateFinanceAutoLink` (list query + per-office viewer enumeration + link writes), `kansasFinanceWriter` (standard writer wrapper, manual-link protection, `cfr_viewer` supersession), migration 265 (`ks_candidate_finance_*`), script `kansas-candidates:finance:auto-link` (`--dry-run`, `--force`, `--max-candidates`, `--lookback-days`, `--lookahead-days`), flags `KANSAS_CAMPAIGN_FINANCE_ENABLED` / `KANSAS_CAMPAIGN_FINANCE_SYNC_ENABLED` (auto-link shares the sync gate; both `=false` in `.env.example`). Facts locked in live 2026-09-01:

- **Kansas has no filer id.** The viewer's grids list FILINGS, and each filing carries the name as typed on it: the same House filer appears as `BRUNK STEVEN`/`BRUNK STEVE`, `WILLIAMS MARY`/`WILLIAMS MARY T`, `CLINTON JERRY`/`CLINTON JERY` (1,127 report rows -> 458 distinct spellings for ~350 filers). A few filings leave the district blank; statewide rows may carry a stray district (`ROGERS STACY`, Governor, district 4). Link identity is therefore the deterministic **search recipe** `committee_id = <officeCode>:<district>:<SURNAME>:<FIRST>` (`7:85:BRUNK:STEVEN`; statewide `1::KELLY:LAURA`), suffix-stripped and uppercased, with `committee_name` = the most frequent filed spelling. The sync re-runs the search from the recipe and re-resolves rows by name.
- **Enumeration** = one viewer session per office per run, three Candidate-filings searches over the cycle window (statewide/Senate from 1/1 of election year − 3, House from 1/1 of election year − 1, through today): `Receipts and Expenditures Report` -> grid `grdviewCfrResults`, `Appointment of Treasurer` -> `grdviewApptOfTreas`, `Affidavit of Exemption Candidate` -> `grdviewAffidavitResults`. A blank filing type re-renders the form with "Filing Type Required" (fails closed); `Termination Statement` answers in `gvGECLetters` (not used). All grids render the name as one uppercase `LAST FIRST [MIDDLE]` string (AT/affidavit grids also carry separate last/first anchors); the reports grid uses `lblOriginalDate` for every row. Zero-result searches render `lblRecordCount` 0 with no grid. House pool ~2 min at 1.5 s spacing (57 + 15 + ~3 pages).
- **Resolver rules** (fail closed): exact district for districted offices (statewide ignores the column); full-name evidence through the shared middle-name gate with one-sided roster->filing nickname expansion (`Steve`->`STEVEN`, `Chris`->`CHRISTOPHER`); every aligned spelling is one person unless two aligned spellings contradict each other on first name outside one nickname family (roster `Pat` aligning with both `PATRICK` and `PATRICIA`), middle names, or generational suffix (-> `ambiguous`, no link); a race whose cycle window has not opened yet reports `cycle_not_started` instead of searching; blank-district-only alignments -> `manual_confirm_required`; a bare surname never links. County rows (`county::District Attorney` = County Attorneys) and federal races are out of the office map.
- **Live result** (House only — statewide and Senate-special GENERAL rows are not seeded yet, only 8/4 primaries): 129 attempted, **128 linked** (123 exact, 5 nickname; 7 folded multiple spellings such as `PARKS RANDALL`/`PARKS RANDALL DUANE`), 0 ambiguous, 0 manual, 0 errors, 1 unmatched (`Pete Ferrell`, HD12 — no filing under that surname in the viewer for the office; manual follow-up). Links are LOCAL only.

### Phase 4 — direct side (first shippable release)

Aggregator + writer + sync: cover-sourced totals, occupation and size breakdowns with coverage metrics, per-period ledger. Per-candidate publish gate as above.

### Phase 5 — outside spending (second release)

Verified IE-statement transcription, 25-4148c last-minute report ingestion near the election, Schedule C inventory, three-path dedupe, coverage states; `outsideSupportTotal`/`outsideOpposeTotal` written only for `complete_for_explicit_rows`.

### Phase 6 — live run + validation

Full sync across scope; spot-audit 10 candidates cent-exact against covers; verify the Masterson oppose total equals $522,213.63 plus any later statements.

### Phase 7 — flags, labels, prod (per new-state checklist)

- Flags: code defaults false in `backend/src/config/featureFlags.ts`; document BOTH `KANSAS_CAMPAIGN_FINANCE_ENABLED` and `KANSAS_CAMPAIGN_FINANCE_SYNC_ENABLED` (`=false`) in the tracked `backend/.env.example` (alphabetical, next to the other state finance flags), set them `=true` in the operator's local untracked `backend/.env`, read flag to `render.yaml`.
- Source label: new source in `backend/src/pipeline/address/ballotLookupFinanceShared.ts` AND `KANSAS_SOS: "Kansas Secretary of State / KPDC"` in `FINANCE_SOURCE_LABELS` (`packages/api-client/src/format.ts`, keeping key style consistent with `MISSOURI_MEC`/`OHIO_SOS`) plus the `financeSourceLabel` test in `format.test.ts`.
- Loader registered in the ballot-lookup finance dispatch alongside the other states; typecheck + vitest green in backend and frontend/api-client.
- Prod: apply migration, promote data per finance sync runbook; Render deploys are manual (POST full SHA).

## Fail-closed rules

No snapshot publish for a candidate when: ambiguous link; any due period unaccounted; cover arithmetic off by one cent; amendment not verifiably a replacement; any money row unparsed or with broken row integrity (contributor/occupation/amount not provably same row); person/entity class ambiguous on an occupation-bound row; Schedule C direction ambiguous; multi-candidate spend unallocated; last-minute/regular dedupe unresolved; only source is the 2022 legacy database. Retain last-good snapshot on refresh failure.

## Risks

1. **Viewer fragility**: session-state postbacks, no durable IDs; markup drift breaks the walker. Mitigation: search-recipe persistence, artifact cache, whitespace-tolerant parsers, KPDC tree as degraded cover fallback.
2. **Paper-filer OCR**: real rate unknown until Phase 0.4; worst case a bounded manual queue.
3. **Scan lag / archive completeness**: viewer enumeration is the completeness reference; KPDC tree alone is insufficient near deadlines.
4. **Export cap** unverified above 4,285 rows (Phase 0.3).
5. **Election-week volume**: 25-4148c daily filings land Thu–Sun before the election; sync cadence must tighten that week.
6. **Legal**: 25-4154(d) review outcome could constrain staging or force design changes — which is why it starts in Phase 0.
