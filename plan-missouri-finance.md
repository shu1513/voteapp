# Missouri Campaign Finance Plan

Written 2026-08-12 after a live probe of the Missouri Ethics Commission (MEC) portals (searches exercised in a real browser, committee pages walked, WAF behavior tested with plain HTTP) plus a codebase reuse audit. Revised the same day after an external review round; every adopted correction below was re-verified against the live portal or the code before adoption (WAF host behavior, `debts_owed`/`top_employers` loader behavior, factory link-upsert overwrite, the candidate-by-election search). Verdict: **GO — Phase 0 first.** Schema and any published numbers wait until the Phase 0 gates (acquisition proof, export schema, report-period semantics, totals mapping, report-summary reconciliation) pass.

MEC is the filing officer for **all** Missouri candidate offices since 2017 (RSMo § 130.026) — statewide, General Assembly, judicial, county, municipal, school board. One state module therefore covers state *and* local candidates, unlike the CA city modules.

## Verified sources (probed live 2026-08-12)

### Access reality: Imperva/Incapsula WAF — host-dependent (revised after review)

`mec.mo.gov` (bare host) served a 212-byte Incapsula JS challenge stub to plain curl; **`www.mec.mo.gov` served the complete ~60 KB ASP.NET page to plain curl on every attempt**, including with the descriptive VoteApp user agent (re-verified 5× on 2026-08-12). The original "plain HTTP is dead" conclusion came from probing the bare host only. Architecture:

1. **Primary: session-aware HTTPS client against `www.mec.mo.gov`** — cookie jar, `__VIEWSTATE`/`__EVENTVALIDATION` postbacks, the search→popup flow below, Georgia-style courteous throttling (single-flight, ~2 s spacing), and **challenge detection that fails closed**: any response matching the Incapsula stub (`_Incapsula_Resource`, tiny body) aborts the run and backs off — no challenge solving, ever. Phase 0 measures success/challenge rates across time before this is trusted.
2. **Contingency: Ohio-pattern CDP attach** (`ohioSosChromeClient.ts` — attaches to the user's running Chrome, no fingerprint spoofing) only if the plain client's live failure rate proves unacceptable. Acquisition runs locally either way (repo pattern: local acquisition scripts, snapshots promoted to prod later), but the plain client is strongly preferred — it also runs on Render if scheduled sync ever moves there.
3. **Preferred long-term: ask MEC for its existing system-wide extract** (see Prerequisites). § 130.057 directs open, electronically-readable public access — it supports the request but does not obligate MEC to build a custom recurring feed, so ask for what exists: the current full dump, field dictionary, report/version relationships, transaction primary keys, and which fields are redacted from public copies.

Either way, retrieval and parsing stay separate: every fetched body lands in an NC-style artifact cache (hash + manifest + pinned parser version, `northCarolinaNcsbeArtifactCache.ts` pattern) and the sync reads only the cache. Cache PII rules: artifacts contain contributor street addresses — restricted directory, never committed to git, sanitized fixtures only (addresses replaced), no raw addresses in logs or diagnostics.

### Primary: Contributions & Expenditures search (`CF12_ContrExpend.aspx`)

ASP.NET WebForms, `__VIEWSTATE` postbacks. Year floor **2002**. Results are **session-bound**: the search POST response emits `window.open('CF12_ContrExpendResults.aspx','_blank')` and the results page must be fetched in the same session. Verified live (Year=2026, last name "Smith"): 833 full-disclosure rows + 4 separate 48-hour (>$5,000) rows — counts are point-in-time observations, never asserted contracts. Columns:

```text
MECID | Committee Name | Report | Contributor Name/Address | Employer/Occupation | Contribution Date | Contribution Amount | Monetary/In-Kind | Committee (Y/N flag)
```

- `MECID` on every row — stable committee identity (e.g. `A222073`), the join key for everything.
- `Employer/Occupation` is **one combined display field**, formatting filer-dependent: `Retired/Retired`, `Self Employed/Farmer`, bare `Director`, `Unknown`, reversed `Real estate/Win real estate team LLC`. Store verbatim; never auto-split on `/`. (MEC's electronic-filing import spec has separate employer and occupation columns, so the Excel export may split them — Phase 0 decides; see hard fact 1.)
- `AMENDED <report name>` rows coexist with original-report rows in the same result set — naive summing double-counts.
- Sub-$100 rows appear (aggregate-triggered or voluntary itemization) — never filter by transaction size.
- **Export Results to Excel** button present (`ctl00$ContentPlaceHolder$btnExport`). Not yet downloaded; Phase 0 inspects the actual bytes (real XLSX vs CSV vs HTML-disguised-as-`.xls` — common with WebForms exports) before any parser is written.

### Primary: Committee Expenditures for Candidates search (`CF_SearchDirExp.aspx`)

The registered-committee outside-spending source. Year floor **2019** (correction to the external feasibility report, which implied deeper history). Results render inline, 25/page with paging postbacks; verified live for 2026: 1,718 rows, columns:

```text
Candidate Name and Address | Office Sought | Support/Oppose | Date | Amount | Reporting Committee | Report
```

- Explicit `Support`/`Oppose` stance per row (statutory basis § 130.041.1(7)); observed both stances across state senate, house, county, municipal, and school-board targets, including 24 Hour Expenditure Report rows (Show Me Promise, ABC PAC, Taxpayers Unlimited).
- The visible grid does not show the spender's MECID, but the grid's Reporting Committee sort control is internally named `lbtnMECID` (server sorts by it) and the committee-name link resolves the profile — the Excel export very likely carries MECID; Phase 0 confirms.
- **Export Results to Excel** button present.

### Committee Info page (committee profile — resolves everything)

Reached from any committee-name link. Verified live (`MECID A222073`): MECID, committee type, candidate name, treasurer, party marker, **Election History table (election date + political office, e.g. "4/2/2024 General Election → Alderperson Ward 3 City of Jackson")**, and a Reports tab with per-year lists of **stable numeric report IDs** + report name + filed date, AMENDED reports listed as separate rows. This page is the auto-link backbone: candidate-committee resolution and spender-MECID resolution both land here, no fuzzy name matching required.

### Secondary: Candidates by Election search (`CF12_SearchElection.aspx`) — discovery only

Verified live: election year → election date (11/03/2026 listed), filters for Office Sought / Political Subdivision / District Number / Committee Status. The Office Sought vocabulary is a closed dropdown (Assessor … State Representative, State Senator, Statewide Office, judicial and county offices) — Phase 0 captures the full list for `missouriFinanceEligibleOffices.ts` instead of guessing at "local offices". Two cautions, both from the review round: (a) results list **committees that registered**, with duplicates, terminated committees, and inconsistent district fields — this is a committee-discovery and reconciliation source, **never** a ballot roster (VoteApp rosters stay authoritative, built via `voteapp-manual-research`); (b) a full-office query (all Nov-2026 State Representative) hung the results render for 45+ s live — partition by district or fetch once per election as a cached artifact, never on any request path. The page reportedly offers Summary/Excel controls on results; Phase 0 inspects them — if the summary carries last-full-report totals per committee, it is a cheap reconciliation cross-check.

### Deferred: Non-Committee Expenditure Reports (`CF14_nonCommExp.aspx`)

§ 130.047 filers (entities spending their own funds; the filing does not disclose contributors). Year floor 2006; 119 documents for 2026, each a scanned PDF at a stable `https://mec.mo.gov/DMS/DOC/V/{docId}` URL. Real coverage (AFP, unions, Missouri Chamber, PROMO…) but document-based OCR work with hard precision requirements — **out of scope for v1**, and because of it every published outside total carries the explicit coverage note in hard fact 6.

## Hard facts that shape the design

1. **Employer/occupation must not be published under a false label.** Missouri law (§ 130.041) requires employer, *or* occupation if self-employed, *or* a retired notation, above a $100 aggregate — a universal "donor occupations" claim is dishonest for this state. Two verified code constraints: the shared loader hardcodes `top_employers: []` and routes only `occupation` + `contribution_size` ([standardStateFinanceBallotLookupLoader.ts:661](backend/src/pipeline/finance/standardStateFinanceBallotLookupLoader.ts)), and the UI hardcodes the heading "Top disclosed occupations of direct donors" ([FinanceSummaryCard.tsx:353](frontend/src/components/FinanceSummaryCard.tsx)). Decision rule, settled by the Phase 0 export inspection:
   - Export has **separate** employer and occupation columns → publish the occupation column only under the existing heading; keep employer text stored for later.
   - Export has **only the combined** display value → v1 **suppresses the occupation chart** for Missouri (empty `top_occupations`) rather than publishing employer strings as occupations. Relabeling the heading to "Reported employer or occupation" is a deliberate later change touching web + mobile + chatbot wording, not a v1 side effect.
   - Either way: store the reported text verbatim (`employer_occupation_display_reported`, plus split fields when the export provides them); `Retired`/`Self Employed`/`Not Employed` stay non-industry statuses; blank/`Unknown` → Unknown; no industry classification of ambiguous combined values, ever.
2. **Missouri needs a report inventory, not just amendment pairing.** Report types observed or statutory: Full Disclosure (quarterly + pre/post-election), **Limited Activity** (§ 130.046 — low-activity periods; the skipped detail is picked up by the next full report, so report periods deliberately overlap), Termination, 48-hour contributions >$5,000 (§ 130.044), 24-hour expenditure reports, plus AMENDED versions of each listed as separate rows. Build a small per-committee report inventory (NC `northCarolinaReportSelector.ts` / Georgia report-inventory precedent): report ID, type, coverage dates, filed date, amended-lineage. Canonical totals derive from **report-cover arithmetic over the canonical report set** (Georgia lesson), never from summing apparent report periods until Phase 0 proves the coverage semantics — including one fixture proving how Limited Activity → next Full Disclosure carry-forward behaves and one proving whether an amendment is a full replacement or a delta.
3. **Timely filings are one taxonomy, and dedup never guesses.** 48-hour contribution rows arrive on a separate results tab — excluded from canonical totals (they reappear in full disclosure). 24-hour expenditure rows sit *inside* the outside-spending results (much of the live 2026 Show Me Promise activity is on them), so they cannot be blanket-excluded; Phase 0 observes whether a timely row persists, disappears, or duplicates once the covering quarterly lands, and checks for any late-contribution/loan variant of the 24-hour channel while pinning the taxonomy. Dedup rule (revised after review): dedup **only** on source transaction/report identifiers proven in Phase 0, or on report lineage (timely report superseded by its covering full report). A composite like (spender, candidate, date, amount) can merge two legitimate same-day expenditures — it is a quarantine trigger, never a merge key. Ambiguous clusters are withheld with excluded-dollar diagnostics.
4. **Every displayed total gets an exact source mapping before publication (Phase 0 deliverable).** The shared contract: `total_raised` excludes loans (deliberate, `ballotLookupFinanceShared.ts`); Missouri report covers distinguish contributions, total receipts, loans, in-kind, expenditures, cash, and indebtedness, and candidate-committee contribution aggregation resets at the primary/general boundary (§ 130.041) — a calendar-year query is not automatically the November cycle. Phase 0 maps: raised (contributions vs total receipts), loans, in-kind (in raised? in spent?), refunds/negative adjustments, cash on hand, and the cycle window. **Debts: v1 publishes without them** — the shared loader nulls `debts_owed` for every non-Illinois state ([standardStateFinanceBallotLookupLoader.ts:659](backend/src/pipeline/finance/standardStateFinanceBallotLookupLoader.ts)); Missouri indebtedness is on the report cover, so record it in the report inventory, but surfacing it is a shared-loader change deferred until some cohort needs it.
5. **Results carry contributor street addresses.** Parse, use for nothing, never persist in product tables, keep raw artifacts restricted (see cache PII rules above).
6. **Outside totals are explicitly committee-reported only, and funders are cycle contributors.** Coverage note on every Missouri outside snapshot: "Registered-committee reported spending only; Missouri non-committee expenditure reports (§ 130.047) are not included." Prefer absent/unknown over `$0` where CF14 has simply not been ingested. Outside-group funder breakdowns come from CF12 keyed by the spender's MECID and are labeled committee-cycle contributors (the shared loader's `outsideSupportActionLabel` wording hook), never ad funders. Non-committee filers get no funder chart — the filing does not disclose contributors.
7. **Coverage boundaries are explicit.** Contribution search 2002+; committees that filed only with a county clerk (mostly pre-2017 local) are absent (stated on the search page); outside-spending search 2019+. All become coverage notes. v1 scope is the November-2026 cycle, so neither floor bites.

## Prerequisites

1. **Rosters.** Finance links land only on rostered candidates. Verify the November-2026 Missouri races we care about exist with candidate rows before Phase 2; fill gaps via the `voteapp-manual-research` skill first.
2. **MEC extract request (user action, parallel with Phase 0).** Email MEC campaign-finance staff (573-751-2013) with the § 130.057 ask scoped to what exists: the system-wide electronic dump, field dictionary, report/version relationships, transaction primary keys, and public-field redactions. Any yes shrinks the portal client.

## Architecture

New module `backend/src/pipeline/missouriFinance/`, tables prefixed `mo_candidate_finance_*` (no collision; longest name well under 63 chars). Standard five-table shape via `createStandardStateFinanceSnapshotWriter` — Missouri is **factory-canonical** on identity: link and outside identity are `committee_id` = MECID / `committee_name`, defaults untouched. Config: pairing validation (`outsideGroupValidation: "pairing"` — cascade-FK trap), `minElectionYear: 2024` (November-2026 scope; widen later if history is wanted), replace-merge, and `supersededLinkSource` set to Missouri's automatic link source so a committee change deactivates the stale automatic link inside the snapshot transaction (maine/maryland precedent) — without it, multiple automatic links can stay active and double-count.

**One deliberate factory extension (verified gap; revised after PR review):** the factory's link upsert overwrites `link_status`/`link_source` unconditionally ([standardStateFinanceSnapshotWriter.ts:391-392](backend/src/pipeline/finance/standardStateFinanceSnapshotWriter.ts)) and manual-link protection is factory-absent (capability matrix agrees). A Missouri wrapper **cannot** bolt the San José guard around the factory call: `replaceSnapshot` demands a Pool and opens its own transaction ([standardStateFinanceSnapshotWriter.ts:326](backend/src/pipeline/finance/standardStateFinanceSnapshotWriter.ts) — "must receive a Pool, not a PoolClient"), so an external precheck is non-atomic and the private link upsert still overwrites. Resolution: add an opt-in `manualLinkProtection` config to the factory itself — the capability matrix already earmarks this as "add only when a migrating cohort needs it", and Missouri is that cohort (six bespoke `M` states can migrate onto it later). Semantics inside the factory transaction, mirroring the shipped San José guard ([sanJoseFinanceWriter.ts:118-146](backend/src/pipeline/sanJoseFinance/sanJoseFinanceWriter.ts)): with an active `link_source='manual'` row for the (candidate, election) — an incoming automatic link with the **same** committee_id refreshes `last_verified_at` only, preserving status and source; a **different** committee_id skips the automatic write with a warning and a diagnostics count; stale-link supersession never deactivates manual rows. Default off — zero behavior change for every existing state. Ships as its own small PR with tests at the start of Phase 1. Automated discovery must never replace a curated manual link.

File layout mirrors `georgiaFinance/` (closest structural analog):

```text
missouriMecClient.ts             # session-aware www.mec.mo.gov fetch: cookie jar, postbacks, popup flow, throttle, challenge detection (fail closed)
missouriMecArtifactCache.ts      # NC pattern: hash, manifest, pinned parser version, restricted PII handling
missouriMecParsers.ts            # export + committee-page + report-inventory parsers, contract-tested, fail closed on drift
missouriReportInventory.ts       # hard fact 2: report id/type/coverage/amended-lineage, canonical-set selection
missouriCandidateCommitteeResolver.ts   # Committee Info election-history evidence first, name evidence last
missouriCandidateFinanceAutoLink.ts
missouriCandidateFinanceDueList.ts
missouriCandidateFinanceBatchSync.ts
missouriCandidateFinanceSync.ts
missouriDirectContributionAggregator.ts
missouriOutsideSpendingAggregator.ts
missouriOutsideGroupContributionAggregator.ts
missouriFinanceEligibleOffices.ts       # enumerated from the CF12_SearchElection office vocabulary, not guessed
missouriFinanceWriter.ts                # factory + SJ-style manual-link guard
missouriBallotLookupFinanceLoader.ts
index.ts
```

Direct breakdowns: `contribution_size` always; `occupation` per hard fact 1's decision rule. **Direct-donor** industry classification stays out of v1 (the shared loader can't route a direct `industry` category anyway). Outside-group funder classification is different — it ships in Phase 4, because without it the funder data is invisible (revised after PR review): the shared loader renders outside funders only through `category_type='industry'` rows plus donor rows as evidence beneath them ([standardStateFinanceBallotLookupLoader.ts:403](backend/src/pipeline/finance/standardStateFinanceBallotLookupLoader.ts), evidence join at :504-528) — donor breakdowns alone never display. The shipped Georgia pattern covers this with no new machinery ([georgiaOutsideGroupContributionAggregator.ts](backend/src/pipeline/georgiaFinance/georgiaOutsideGroupContributionAggregator.ts)): the aggregator emits uncapped organizational `donor` rows, and the sync rebuilds `industry` rows from the existing rule-based classifier plus cached manual verdicts (no AI calls). Group names and support/oppose totals render regardless via `top_supporting_groups`/`top_opposing_groups`.

### Phases (each lands as its own PR; probe before schema)

- **Phase 0 — probe (no schema, no publication).** `npm run missouri:finance:probe` script + acquisition spike. Hard gates:
  1. Acquisition proven end-to-end on `www.mec.mo.gov` with the plain client: search POST → session popup → both Excel exports downloaded and cached; success/challenge rate measured across separated runs with courteous throttling; challenge stub detection verified against the bare-host response. CDP fallback exercised only if the plain client fails this gate.
  2. Export bytes + schema pinned: sniff actual format (XLSX/CSV/HTML-as-`.xls`); column list for both exports; employer/occupation combined or separate (decides hard fact 1); MECID present on outside rows; export row counts match the visible grid for one large and one small query (silent-cap check — counts treated as observations).
  3. Report-inventory semantics: fixtures proving amendment replace-vs-delta, Limited Activity → next-Full-Disclosure carry-forward, and one committee's transaction rows reconciling to its report-cover totals to the cent.
  4. Timely-filing taxonomy pinned (fact 3), including whether a late-contribution/loan 24-hour variant exists, using Show Me Promise's 24-hour rows vs its July Quarterly.
  5. Totals mapping (fact 4) written down field-by-field against two real report covers (one statewide/legislative, one local).
  6. `CF12_SearchElection.aspx` Summary/Excel inspected; office-vocabulary list captured for `missouriFinanceEligibleOffices.ts`.
  7. Gold set resolves via Committee Info election history: ~8 committees spanning statewide/legislative/county/municipal/school-board and including an active, a terminated, an amended, a Limited Activity, a timely-filing, and (if findable) an out-of-state-registered case, plus one outside spender.
- **Phase 1 — schema + writer.** Migration (next free number — **never renumber**) for the five `mo_` tables; writer via the factory + manual-link guard; writer tests including the guard.
- **Phase 2 — resolver + auto-link.** Election-history evidence tier, then candidate-name + office/jurisdiction match against the roster, fail-closed ambiguity.
- **Phase 3 — direct finance.** Report inventory + contribution/expenditure aggregation with canonical-set selection and summary reconciliation, contribution-size buckets (+ occupation per fact 1), coverage notes, sync + due list + scheduler + flags + source label + ballot-lookup loader.
- **Phase 4 — outside spending + funders.** Yearly outside export (2019 floor), explicit-stance-only publication, spender MECID resolution, timely-report lineage dedup (fact 3), deterministic candidate matching (quarantine the rest with excluded-dollar diagnostics); then outside-group contributor breakdowns via CF12 keyed by spender MECID — organizational `donor` rows plus sync-time `industry` rows from the shared rule-based classifier (Georgia pattern; required for the funders to render at all) — committee-cycle wording, CF14 coverage note on every snapshot.
- **Phase 5 — live run + UI + prod checklist.** Full November-2026-cycle ingest locally, spot-reconcile against MEC report views, FinanceSummaryCard render check (including the no-occupation state if fact 1 lands on suppression), then the standard prod steps. Prod crons stay commented out in `render.yaml` pending Render billing (repo-wide state).

### Flags & labels (checklist items, do not skip)

- `MISSOURI_CAMPAIGN_FINANCE_ENABLED` + `MISSOURI_CAMPAIGN_FINANCE_SYNC_ENABLED` + `MISSOURI_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED` in `featureFlags.ts` (exact mirror of the `GEORGIA_*` trio — the trio already separates read-side from job-side): code defaults `false`, set `true` in `backend/.env` (alphabetical), documented in `backend/.env.example` (the file carries every state's flags), read flag added to `render.yaml`.
- Source enum `MISSOURI_MEC` in `ballotLookupFinanceShared.ts`; display label "Missouri Ethics Commission" in `FINANCE_SOURCE_LABELS` (`packages/api-client/src/format.ts`, alphabetical) + `format.test.ts` case.

## Out of scope (v1)

- **Non-committee expenditure PDFs** (`CF14` / DMS scans) — revisit as its own phase only after v1 ships, with strict precision gates (auto-publish requires unambiguous candidate + stance + amount; quarantine everything else). Until then the coverage note in hard fact 6 discloses the gap on every outside snapshot.
- `debts_owed` display (shared-loader gap, fact 4), pre-2024 elections, 48-hour contribution rows in canonical totals, earmarked/designated-contribution relationships, FEC-affiliate joins for out-of-state committees (empty funder breakdown instead of a guessed affiliate), direct-donor industry classification (later, shared classifier — outside-group funder classification is IN scope, Phase 4), republishing contributor street addresses (never).
