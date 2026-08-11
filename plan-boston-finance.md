# Boston (Massachusetts Municipal) Campaign Finance Plan

Written 2026-08-10 after live probes of `api.ocpf.us` and an audit of the existing `backend/src/pipeline/massachusettsFinance/` module. Verdict: **GO — this is an extension of the existing Massachusetts OCPF pipeline, not a new city module.** Boston mayor and city-council candidates file with the state OCPF system the pipeline already consumes; the module only gates them out at the eligible-office layer.

## What already exists (do not rebuild)

- `massachusettsOcpfClient.ts` — filer search, itemized contributions (with `occupation` + `employer`), IEPAC report summaries + details (per-candidate support/oppose expenditures).
- Resolver, direct/outside aggregators, writer (5 canonical tables), sync, batch sync, scheduler scripts, ballot-lookup loader.
- Flags `MASSACHUSETTS_CAMPAIGN_FINANCE_ENABLED` / `_SYNC_ENABLED` are already `true` in `backend/.env` and `render.yaml`. Source enum `MASSACHUSETTS_OCPF` and its display label already exist. **No new flags, no new migrations, no new source labels.**
- The writer already accepts `totalDisbursements` (`total_disbursements` column exists); the sync simply never populates it.
- In-repo precedent for municipal-via-state-source: `illinoisFinance` already serves `place::Mayor`, `place::City Council Member`, etc. from the state board. Follow that pattern.

## Verified live facts (2026-08-10 probes)

Municipal roster + totals endpoints (found in the OCPF SPA bundle, confirmed live):

- `GET /reports/mayoral/depository/{year}` — flat array, 128 filers across **45 cities** for 2025. Fields: `cpfId`, `filerName`, `officeSought` ("Mayoral, Boston"), `receiptsYtdNumeric`, `expendituresYtdNumeric`, `currentCashOnHandNumeric`, `startBalanceNumeric`, `isWinner`, `bankReportId`, `partyAffiliation`.
- `GET /reports/cc/ytd/{year}` — **wrapper object `{reports: [...], summary: {...}}`** (not a flat array — client's `arrayPayload` must learn the `reports` key). 546 rows for 2025, 76 with `officeSought = "City Councilor, Boston"`. Same YTD fields.
- Warning: `api.ocpf.us` returns `200 []` for many nonexistent paths — a 200 with an empty array is NOT proof an endpoint is real. Endpoints above returned real data.

Cross-checked totals, Boston mayor 2025 (matches OCPF site): Wu cpfId 15563 raised `$2,236,403.35` / spent `$1,833,012.33`; Kraft cpfId 18970 raised `$6,846,023.59` / spent `$6,569,520.85`.

Filer search (`/filers/listings/A?searchPhrase=…`) returns municipal candidate committees: `officeSought: "Mayoral, Boston"` / `"City Councilor, Boston"`, `accountTypeDescription: "Depository Candidate"`. The resolver's `isCandidateFilerUsable` already accepts DEPOSITORY account types.

Itemized receipts (`/search/items?cpfId=…`) work for depository filers: Wu 2025 returned 20,821 items, **99.1% with occupation + employer**. MA requires occupation/employer once a donor aggregates >$200/calendar year — missing occupation below that is normal, never infer it.

Outside spending: `/miscreports/iepacs/reports/2025` returned 111 IEPAC reports, 28 mentioning Boston candidates (Wu, Kraft, Louijeune, Flynn…), with per-candidate support/oppose in report details — the existing IE leg works for municipal races **unchanged**. `/miscreports/iepacs/candidates/{year}` (56 names for 2025) is a cheap prefilter if ever needed.

Non-IEPAC "ordinary" IE reports exist at `/miscreports/reports/{year}` (82 for 2025) but only 2 touched Boston, ≈$2k combined. Real gap, tiny money — v2.

**Totals semantics (Georgia lesson applies).** Wu's 2025 item sum is `$2,316,779.70` vs bank-report YTD `$2,236,403.35` (+3.6%; prior-year contributions, refund netting, timing). Therefore: **raised/spent/cash come from the depository YTD endpoints; itemized rows feed occupation/size breakdowns only.** Three YTD outcomes, handled distinctly (as built): a feed request failure throws — the candidate sync fails and the prior snapshot stays intact; a fetched feed with no row for the CPF falls back to the itemized sum for raised only (pre-YTD behavior) with spent/cash left null; a matched row with an invalid raised value writes null raised — never the itemized sum. Cash on hand is signed (overdrawn balances are real; migration 232). YTD totals are calendar-year, which for Boston's odd-year cycles (preliminary Sept + general Nov in the same year) equals the cycle for practical purposes — label them as calendar-year regardless.

**Amendments — no dedup needed (verified 2026-08-11, reversing the 2026-08-10 assumption).** IEPAC summaries carry `amendedByReportId` / `amendmentToReportId` / `isAmendment`; 26 of 111 2025 rows have `amendmentToReportId` set — they **are** the amendments, and OCPF already removes the superseded originals from the feed. Checked 2022–2025: zero rows with `amendedByReportId` set, zero superseded originals present, zero duplicate cpfId+reporting-period groups. Detail-level confirmation: original `/report/996492` has `isAmended: true, nextReportId: 996494` and is absent from the feed; amendment `996494` (`isAmendment: true`) is the row the feed serves. Since the sync fetches details only for feed reportIds, superseded originals are unreachable — **no double-counting exists**. Do not add a dedup filter; filtering on `amendmentToReportId` (the only populated field) would drop real money. Only if OCPF ever regresses and lists both: drop any row whose reportId appears as another row's `amendmentToReportId`.

**Council seat granularity.** OCPF filer rows say only "City Councilor, Boston" — no district vs at-large distinction (`districtCodeSought` encodes the city, 5035 = Boston). Seat identity comes from our roster; the resolver matches on name + office class + city. The cc list contains many inactive/former filers (76 Boston rows ≫ real ballot) — our roster stays the ballot authority, resolution is per-roster-candidate, so this only means name search must tolerate noise, same as today.

## V1 scope

Boston only (city allowlist, GEOID `2507000`), offices `place::Mayor` and Boston city-council canonical office(s) as they appear in the catalog when Boston 2027 (or backfilled 2025) elections are seeded. Deliverables per qualifying candidate: total raised, total spent (new), donor occupations, contribution-size buckets, IEPAC outside support/oppose with donor/industry breakdowns.

Expansion to the other 44 OCPF depository cities later = extend the allowlist; the endpoints already return them.

**Timing reality:** the DB currently has no Boston `place` elections; Boston has no 2026 municipal races (next council/mayor cycle 2027, odd years). Build value is (a) 2025 backfill for profiles, (b) ready lane for 2027, (c) the spent-total fix improves live state races immediately.

## Phases

### Phase 1 — municipal office mapping + resolver

`massachusettsFinanceEligibleOffices.ts`:
- Add `municipal` office scope handling: parse `officeSought` formats `Mayoral, {City}` and `City Councilor, {City}`.
- Add eligible keys `place::Mayor` + council canonical name; eligibility requires district `place` + GEOID in `MASSACHUSETTS_MUNICIPAL_FINANCE_CITIES` allowlist (`2507000` only), mirroring `isSanFranciscoFinanceEligibleElection`'s district gate and Illinois' place handling.
- Resolver: expected-office match compares office class + city name (from the allowlist entry), not district. Middle-name veto stays.

Loader/batch/due-list: widen the same gate; pass the city through to the resolver.

### Phase 2 — spent totals (and honest raised totals)

- Client: add `getMassachusettsOcpfMayoralDepositoryReports(year)` and `getMassachusettsOcpfCityCouncilYtdReports(year)` (handle the `{reports: […]}` wrapper); reuse `parseCandidateReport` (already reads `receiptsYtdNumeric` / `expendituresYtdNumeric`).
- Sync: for municipal candidates, after committee resolution, look up the candidate's YTD row by cpfId → `totalReceipts` + `totalDisbursements` (+ signed cash) from YTD numbers; itemized aggregation keeps feeding breakdowns and `directContributionTotal`. Outcome handling per the totals-semantics section above: feed failure → sync fails (prior snapshot kept); no row → itemized raised fallback, spent/cash null; matched-but-invalid raised → null.
- Decide in-PR whether statewide/legislative sync adopts the same YTD-sourced `totalDisbursements` (endpoints `/reports/statewide/ytd/{year}` + `/reports/legislative/…` already expose it and the client already parses it). Cheap, fixes "Spent" being blank for MA state races — do it unless review finds a blocker.

### Phase 3 — ~~IEPAC amendment dedup~~ CANCELLED (2026-08-11)

Investigated live before building: the feed already dedups (see the amendments paragraph above). The planned filter would be a no-op (`amendedByReportId` is `0` on every row, all years), and the "obvious" alternative — dropping rows with `amendmentToReportId` set — would delete the surviving amendment rows, i.e. real money. Nothing to build.

### Phase 4 (v2, optional) — ordinary IE reports

`/miscreports/reports/{year}` (non-IEPAC IE filers; same amendment fields, `candidateListing`). ~$2k impact for Boston 2025 — defer; until then the outside panel's coverage note should say IEPAC-only. **Costlier than first assumed (2026-08-11 probe):** summary rows carry no `committeeName` or totals, and their reportIds are a **different id namespace** — `/report/12576` returns an unrelated committee's bank report, not IE report 12576. Phase 4 needs fresh endpoint discovery (SPA bundle again) before any build.

Electioneering communications stay out entirely (never silently merged into IE totals).

### Tests

Unit: office mapping (both municipal formats, allowlist rejection of non-Boston cities), cc wrapper parsing, YTD-row totals selection. Live probe via `probeMassachusettsCandidateFinance.ts` against Wu 2025 asserting the two verified totals above. `npm run typecheck` + `npm test` in `backend/`.

Estimated: 1 PR (Phase 1+2 — shipped as #646; Phase 3 cancelled; Phase 4 deferred pending endpoint discovery).

## Reuse beyond Massachusetts

One MA build covers all 45 OCPF depository cities (Worcester, Springfield, Cambridge†, Lowell, Quincy, Lynn, Somerville…) — allowlist growth only. († councils are in `cc/ytd`; Cambridge has no elected mayor race.)

Other existing state pipelines whose sources are believed to centralize municipal filings — each needs a probe like this one before committing (ranked by city value × confidence):

| State pipeline | System | Cities unlocked | Confidence |
|---|---|---|---|
| illinoisFinance | ISBE | already does `place::` offices (Chicago-class) | shipped precedent |
| washingtonFinance | PDC | Seattle, Spokane, Tacoma | high — PDC covers all local candidates |
| hawaiiFinance | CSC | Honolulu mayor/council | high — CSC covers county candidates |
| oregonFinance | ORESTAR | Portland | high — ORESTAR is all-committee |
| louisianaFinance | Ethics Board | New Orleans, Baton Rouge | high — state centralizes local |
| marylandFinance | MDCRIS | Baltimore | high |
| alaskaFinance | APOC | Anchorage | medium-high |
| newJerseyFinance | ELEC | Newark, Jersey City | medium-high |
| kentuckyFinance | KREF | Louisville, Lexington | medium |
| nebraskaFinance | NADC | Omaha, Lincoln | medium |

Not reusable (municipal filings are local/city-run): FL, TX (Houston module exists), PA (Philadelphia has its own portal), MI, OH, AZ (Phoenix own), MN (Minneapolis→county), CO (Denver own system), UT (SLC own), CT, ME, IN, OK, WI. NYC/LA/SF/DC already have dedicated modules.

Rule of thumb going forward: before building any city-specific module, check whether the state pipeline's source already contains the city's filers — a 30-minute probe (filer search for a known mayor + one totals endpoint) settles it.

## Corrections to the 2026-08-10 external feasibility report

Kept because they change decisions:
- "cities over 65,000" is wrong — OCPF's mayoral depository feed includes 45 cities down to ~12k population (North Adams). Coverage is city-form, not population-gated.
- "enable the currently default-off Massachusetts feature flag" is wrong — both MA flags are already on in `.env` and `render.yaml`.
- Its four data-availability claims, the 2025 Wu/Kraft totals, the calendar-year YTD caveat, the missing `totalDisbursements`, and the IEPAC-only gap were all independently verified correct. Its amendment-dedup claim looked correct on 2026-08-10 but was **refuted** by the 2026-08-11 live check (see the amendments paragraph) — the feed already excludes superseded originals.
