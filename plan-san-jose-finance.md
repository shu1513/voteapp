# San José Local Campaign Finance Plan

Status: Phases 1–2 built (shared `efileCalFinance/` client + parser, then the
San José eligibility + resolver adapter; both live-verified 2026-08-10);
Phases 3+ not started. Feasibility + export semantics verified against live
data 2026-08-10 (portal probed, 2025+2026 workbooks downloaded and audited).

## Goal and v1 scope

Per-candidate finance summaries for San José city offices, same card the other
city modules feed: total raised, total spent, top donor occupations/employers,
contribution size buckets, and outside spending for/against. v1 targets the
November 3, 2026 runoff (verified on the city 2026 election page: **Council
Districts 5, 7, 9 only** — mayor is presidential-cycle since 2024). Never
hardcode districts: sync is gated to OUR November-election roster candidates
(city finalizes its candidate list August 13).

Candidate qualification comes from OUR rosters (manual research), never
inferred from filings — the filing store also contains withdrawn, terminated,
prior-cycle, and non-city committees (verified: "David Cohen for California
Senate District 10 2026" files copies with the city).

## Proven source access (all verified live 2026-08-10)

- Official portal `https://efile.sanjoseca.gov` (vendor efile.systems), live
  since 2025-12-18. Old SouthTech portal retired **February 2026** — never
  build against it.
- Bulk export API (no auth):
  `GET /api/v1/public/campaign-bulk-export-url?year=<Y>&most_recent_only=<bool>`
  → returns a stable unsigned S3 URL, e.g.
  `https://efs-efile-campaign-exports.s3.amazonaws.com/csj/export/City_of_San_Jose_CAL_2026_most_recent.xlsx`.
  Years 2018–2026; recent 3 years refresh hourly. 2026 file ≈ 2.6 MB.
- Filing-metadata API: `GET /api/v1/public/campaign-search?...` → per-filing
  JSON: `state_id` (FPPC ID), committee `name`, `committee_type`, amendment
  chain (`amends_orig_id`, `amendment_number`), `e_filing_id`.
- Workbook (CAL 2.20 data, XLSX): **15 sheets, NO cover/CVR sheet** —
  `F460-Summary`, `F460-A-Contribs`, `F460-C-Contribs`, `F460-B1-Loans`,
  `F460-D-ContribIndepExpn`, `F460-E-Expenditures`, `S496`, `S497`, etc.
  Occupation/employer present (`Ctrib_Occ`/`Ctrib_Emp` on A and C sheets).
- Amendment semantics verified: the `most_recent_only=true` ("without amended
  records") variant keeps only current versions (superseded originals absent;
  e.g. Bien Doan filing 24690 replaced by amendment 24692).
- Backend already has the `xlsx` dependency; no new packages.

## Export semantics (audited — these are load-bearing)

- `F460-Summary` rows key on **(Form_Type, Line_Item)** — never a bare line
  number. `Form_Type=F460` line 2 = net loans; `Form_Type=A` line 2 =
  unitemized monetary < $100; `Form_Type=C` line 2 = unitemized nonmonetary.
- `Amount_A` = this report period; `Amount_B` = **calendar-year-to-date** (not
  cycle-to-date). The 2026 campaign contribution period began **November 4,
  2025**, so a 2026 cycle needs the **2025 + 2026** files summed.
- Form 460 line 5 includes loans (line 2). VoteApp keeps loans out of
  `total_raised` (`loans_received` is separate in
  `ballotLookupFinanceShared.ts`), so line 5 is NEVER mapped to total_raised.
- Filed arithmetic can be internally wrong: Bien Doan's current 2026 460
  reports YTD line 1 = 91,178.91, line 2 = 20,000.00, but line 3 =
  131,178.91 — a $20,000 overstatement baked into the filing. Invariant
  checks are mandatory, not nice-to-have.

### Cycle formulas (verified against live data)

Across canonical, non-overlapping, current-version F460 filings whose periods
fall inside the campaign period (2025-11-04 → runoff, spans calendar years):

- `total_raised` = Σ(F460 line 1 `Amount_A`) + Σ(F460 line 4 `Amount_A`)
  (monetary + nonmonetary, loans excluded)
- `loans_received` = loan-receipt lines of the B1 summary block (exact
  (Form_Type, Line_Item) pinned in Phase 1 fixtures), cross-checked against
  the `F460-B1-Loans` sheet
- `total_spent` = Σ(F460 line 11 `Amount_A`)
- `cash_on_hand` = latest filing's F460 line 16 `Amount_A`
- `debts_owed` = latest filing's F460 line 19 `Amount_A`
- unitemized bucket = Σ(Form_Type=A line 2) + Σ(Form_Type=C line 2)

Live check (Bien Doan D7, through 6/30/2026): raised 117,125.37 · loans
20,000 · spent 108,905.71 · cash 32,668.66 — matches the portal PDFs, not the
malformed line 3/5.

**Invariants** (per filing: line 3 = 1 + 2, line 5 = 3 + 4; per committee:
period continuity, no gaps/overlaps, cash chain line 16 consistency). On
violation: quarantine the committee, reconcile against the filing PDF before
anything is written — never publish silently from broken arithmetic.

### Outside spending

- Sources: `S496` sheet ∪ `F460-D-ContribIndepExpn` rows with
  `Expn_Code=IND`, deduped by **(Filer_ID, Tran_ID)** — verified live: all 41
  Schedule D IND rows matched an S496 row by that key (S496: 174 rows /
  $653,351 in the 2026 file at audit time).
- Schedule D `MON`/`IKD` rows are contributions TO committees, not
  independent spending — excluded from outside totals.
- S496 target identity is structured (`Cand_NamL/F`, `Office_Cd`,
  `Office_Dscr`, `Dist_No`, `Supp_Opp_Cd`).
- Form 496 only captures qualifying IEs in the 90-day window; the Schedule D
  union closes most of that gap. Anything still uncovered goes in
  `outside_coverage_note`.
- S497 (24-hour late contributions) is a provisional recency signal only —
  never added to totals.

## Committee resolution (no CVR sheet exists)

Dry-run against the live 2026 roster cohort (all 6 D5/7/9 runoff candidates)
2026-08-10 produced the constraints below. Every candidate does resolve, but
naive name matching is unsafe.

- Durable key = FPPC ID (`Filer_ID` in workbook = `state_id` in search API).
  **`Filer_ID` is not always numeric** — the export contains the literal
  string `Pending` (observed on a 2024 committee still ID-less today), so the
  key type is text and `Pending` is never a durable identity; such committees
  fall back to normalized-name keys and cannot auto-link. (Whether `Pending`
  later flips to a real ID is unverified — confirm during live validation.)
- **`Cmtte_Type` is the safety gate, not the name.** Observed across both
  files: `C` (candidate-controlled), `P` (primarily formed), `G` (general
  purpose) — labels are the CAL spec's committee-type codes and match every
  spot-checked committee (IBEW 332 fund, Silicon Valley Biz PAC, COMMON GOOD
  = all `G`). Only `C` may feed direct totals; any code outside the known set
  fails closed. Cross-check against the search API's human-readable
  `committee_type` ("Candidate or Officeholder"). This matters because an
  outside committee
  can carry the candidate's name: "South Bay Working Families Supporting
  Ortiz for City Council 2026" (`P`, FPPC 1487316) sits next to "Peter Ortiz
  for San Jose City Council District 5 2026" (`C`, FPPC 1480385). Matching on
  name alone would book independent-expenditure money as the candidate's own
  fundraising.
- **District is NOT reliably in the committee name** — 2 of 6 lack it: "Nora
  Campos for San Jose City Council 2026", "Van Le for City Council 2026" (the
  latter omits the city too). Formats seen: `District 5`, `D7`, none. So
  district is a *confirming* signal when present, never a match requirement.
- Name normalization is mandatory: accented/unaccented city ("San José" vs
  "San Jose") and trailing-whitespace duplicates of the same `Filer_ID`.
- **Match on word tokens, never substring** — a case-insensitive substring
  probe for "Le" hit IBEW Local 332, Silicon Valley Biz PAC, and COMMON GOOD
  SILICON VALLEY (the "le" inside "Electrical"/"Valley"). Tokenized matching
  makes short surnames like Le safe.
- `Elect_Date` is dirty (observed `20260630` — a period date — and `None`), so
  it is a soft signal only; cycle gating comes from filing periods.
- Match to roster candidates by name tokens + `Cmtte_Type=C` + city-office
  name parse, confirmed by district and election year when present, reusing
  `personFirstNameNicknames` / `personNameMiddleEvidence` + suffix veto.
  **Given name AND surname must both match** (given name nickname-aware) —
  surname alone collides in live data: "Nora Campos … 2026" and "Pamela
  Campos … District 2 2024" are different people.
- Resolution must be unique within the roster cohort; ambiguous, duplicate, or
  unparseable committees fail closed (no auto-link, queued for manual link).

## Privacy and transport hardening

- Never persist contributor street addresses (`Ctrib_Adr*` columns are
  parsed past, not stored).
- Download client: allowlist hosts (`efile.sanjoseca.gov`,
  `efs-efile-campaign-exports.s3.amazonaws.com`), cap workbook size, validate
  XLSX/ZIP magic bytes before parsing.

## Phases

Phase 0 (export proof) is **done** — endpoint, sheet inventory, amendment
semantics, formulas, dedup, and a full resolver dry-run against the live
roster cohort audited 2026-08-10 with the 2025 + 2026 `most_recent_only`
files. Roster prerequisite is also met: the three Nov-3-2026 runoff races
(D5/7/9) exist with 2 candidates each, and all 6 map to a `Cmtte_Type=C`
committee in the export. That 6-candidate mapping is **provisional** — the
city finalizes its candidate list August 13, 2026; re-run the resolver
dry-run against the final roster before enabling sync.

### Phase 1: shared vendor module + parser — DONE (2026-08-10)

- Built: `efileCalFinance/efileCalBulkClient.ts` (agency config = key +
  portal base URL + export-host allowlist; export-URL resolution → JSON
  `{success, data}`; size-capped magic-checked download; ETag/Last-Modified
  artifact cache modeled on `calAccessRawDataArtifactCache.ts`) and
  `efileCalWorkbookParser.ts` (typed rows for F460-Summary, A, C, B1, D,
  S496, S497; keyed (Form_Type, Line_Item); memo-row aware; exact
  text-decimal → cents; addresses never surfaced).
- Tests off synthetic workbooks (12): keying, money edge cases, flag cells,
  lenient `Elect_Date` vs strict period dates, `Pending` filer ID, missing
  sheet/column/identity fail-closed, allowlist, size cap, cache
  unchanged/changed/force.
- Live-verified: both 2025+2026 workbooks parse whole (no rejected rows);
  Bien Doan reconciles cent-exact (raised 117,125.37 / spent 108,905.71 /
  cash 32,668.66 / net loans 20,000); real portal refresh → `downloaded`
  then `unchanged` on the ETag.

### Phase 2: San José agency adapter — eligibility + resolution — DONE (2026-08-10)

- Built: `sanJoseFinance/sanJoseFinanceEligibleOffices.ts` (GEOID `0668000`,
  keys `place::Mayor` + `place::City Council Member`, seat parse covering the
  catalog's actual "Member, City Council, District N" title, D1–D10) and
  `sanJoseCandidateCommitteeResolver.ts` (committee grouping by `Filer_ID`
  with Pending rows keyed by name; type gate = lone `C`, unknown/conflicting
  codes fail closed; person match via the shared `personNameMiddleEvidence`
  gates + `personFirstNameNicknames` VoteApp-side expansion; district / year /
  cross-office / foreign-office name evidence as vetoes; FPPC-id tier off
  `state_filing_ids`, with the same vetoes applied — `state_filing_ids` is
  candidate-global across races, so a stored id contradicted by the
  committee's own name evidence falls through to the name tier instead of
  linking; ambiguity fails closed in both directions and a blocked
  name-match also blocks its linkable sibling).
- Tests (26): the full Phase 6 resolver list (Ortiz `P` committee, Campos /
  Van Le district-less names, "Le" substring safety, Nora-vs-Pamela, accent
  variants, Pending, unknown types, suffix veto, nickname, quoted call names,
  both ambiguity directions) plus seat-parse and eligibility gates.
- Live-verified: resolver run against the real 2025+2026 workbooks (21,913
  rows → 35 committees) resolves all 6 D5/7/9 runoff candidates to 6 unique
  `C` committees by name, matching the Phase 0 manual dry-run. Roster is
  still provisional until the city's final candidate list (Aug 13) — re-run
  before enabling sync.

### Phase 3: aggregation

- `sanJoseDirectFinanceAggregator.ts`: cycle formulas + invariants above;
  occupations/employers/size buckets from A + C sheets (`Entity_Cd=IND`,
  memo rows excluded); industry classification via
  `financeIndustryClassificationService`.
- `sanJoseOutsideSpendingAggregator.ts`: S496 ∪ Schedule D IND dedup;
  committee labels via `financeLabelClassifier`.
- Store `reported_through` (latest covered `Thru_Date`) alongside totals so
  the UI never implies sync time = data recency.

### Phase 4: schema, flags, writer

- Isolated tables + snapshot writer per SF/LA pattern; next free migration
  number at build time (identifiers ≤63 chars).
- Flags per launch checklist, **three** of them (repo-standard):
  `SAN_JOSE_CAMPAIGN_FINANCE_ENABLED`,
  `SAN_JOSE_CAMPAIGN_FINANCE_SYNC_ENABLED`,
  `SAN_JOSE_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED` (artifact refresh and
  DB sync stay separately gated). Defaults false in code; all added `=true`
  to `backend/.env`; read flag to `render.yaml`.
- Source enum `SAN_JOSE_CITY_CLERK` in `ballotLookupFinanceShared.ts`; label
  in `FINANCE_SOURCE_LABELS` + `financeSourceLabel` test.

### Phase 5: sync, loader, scripts

- Sync / auto-link / batch mirroring the SF module; ballot-lookup loader
  gated by eligible-office check; npm scripts wired like other city modules.

### Phase 6: tests and live validation

- Unit: malformed summary math (Bien Doan case), cross-calendar-year cycle
  assembly, memo rows, A/C unitemized keying, 496/Schedule-D dedup, MON/IKD
  exclusion, missing occupation handling, address-privacy assertion.
- Resolver unit tests, each from a real observed case: `P`-type committee
  carrying a candidate's name is never linked (Ortiz), district-less committee
  names still resolve (Campos, Van Le), short surnames do not substring-match
  ("Le" vs IBEW/Silicon Valley/COMMON GOOD), same-surname different-person
  committees never cross-link (Nora vs Pamela Campos), accent and
  trailing-whitespace normalization (Doan), `Filer_ID='Pending'` is not a
  durable key, dirty `Elect_Date` does not gate, unknown `Cmtte_Type` codes
  fail closed.
- Live: sync the D5/7/9 runoff candidates; reconcile every written total
  against portal 460 PDFs; verify at least one IE committee shows correct
  direction; verify quarantine fires on the known malformed filing if still
  uncorrected.

## Limitations and risk register

- Export covers e-filed statements only. San José now mandates e-filing, so
  this bites only migrated/historical or exception records — if a qualifying
  candidate has such a gap, disclose via `direct_coverage_note`
  (field exists in `ballotLookupFinanceShared.ts`; Georgia uses it).
- Filing lag: totals are as of `reported_through`, surfaced in UI.
- CARS (risk register, not v1 work): SOS's CAL-ACCESS replacement lands after
  the Nov 2026 election; no evidence it changes this vendor's local export.
  Keep parsing behind one interface; revisit when the vendor announces.

## Reuse beyond San José

- **San Diego (same vendor, near-drop-in)**: `efile.sandiego.gov` verified
  2026-08-10 with the identical Bulk Export (XLSX CAL 2.20, 2018–2026).
  A San Diego module = `efileCalFinance` agency config + its own eligible
  offices/GEOID + resolver quirks. Council districts 2/4/6/8 are on the Nov
  2026 ballot. Recommended fast-follow.
- **NetFile jurisdictions (future, different client)**: most CA local
  agencies (Oakland, Berkeley, Sacramento, Fresno, Santa Clara County, …)
  expose CAL-format public exports/APIs via NetFile. Same parser core,
  different transport. Build only when a covered city has roster candidates.
- **Other states: no reuse.** CAL is California-only.
