# Seattle Campaign Finance Plan (Washington module extension)

Written 2026-08-10; revised same day after a second verification round (summary-dataset IE fields, PAC-vs-candidate committee categories, municipal-court coverage, position matching, occupation threshold). Conclusion: Seattle is **not a new module**. All four requested metrics (total raised, total spent, direct-donor occupations, outside spending for/against) already exist in the three PDC datasets the Washington module consumes. The work is an extension of `backend/src/pipeline/washingtonFinance/` plus two statewide bug fixes.

## Verified facts (live queries, 2026-08-10)

- PDC covers Seattle city and municipal-court races in the same datasets already wired in `washingtonPdcClient.ts`: summary `3h9x-7bvm`, contributions `kv7h-kjye`, independent expenditures `67cp-h962`.
- **Summary rows for Candidate filers carry authoritative IE totals** (`independent_expenditures_for_amount` / `independent_expenditures_against_amount`). Wilson 2025: $273,026.25 for / $1,232,834.74 against; Harrell: $289,931.78 / $117,195.32. These equal the sum of all three C6 report types in the IE dataset (`Independent Expenditure`, `Independent Expenditure Ad`, `Electioneering Communication`) — the types are additive, no cross-type dedup is needed (reports 13218/13247 are distinct expenses, both included by PDC). PDC also handles amendment supersession itself.
- Committee-name traps: "Katie Wilson for an Affordable Seattle" (`SEATK--108`) and "Bruce Harrell for Seattle's Future" (`FUTUB--916`) are **Single Election Committee PACs**, not candidate committees. Wilson's candidate committee is `WILSK--949` (`committee_category = 'Candidate'`). The resolver already rejects non-Candidate categories (`washingtonCandidateCommitteeResolver.ts`), so PAC money cannot land as candidate money; PACs appear correctly as outside sponsors.
- Occupation disclosure: required only when an individual exceeds **$250 aggregate per cycle** (official column metadata on `kv7h-kjye`). Wilson candidate committee: 371 of 1,683 individual rows (22%) but $124,715.60 of $196,811.33 itemized individual dollars (**63%**) carry an occupation. Her remaining ~$817K arrived as batched `SMALL CONTRIBUTIONS` rows (code `Other` — democracy-voucher redemptions and other under-threshold cash), which are occupation-less by nature. UI wording stays "top disclosed occupations".
- 2026 Seattle races already in our DB and in PDC: Council District 5 (Kang $112,466.50 / $96,123.52; Jenks $111,998.36 / $105,623.51; James $49,298.56 / $27,017.87; Georgakopoulos $2,050.00 / $1,800.00) and Municipal Court Judge Position 5 (Newsom $82,994.85; Rothstein $35,603.29; Calkins $8,372.90). Council rows: `office='CITY COUNCIL MEMBER'`, `jurisdiction='CITY OF SEATTLE'`, `position='5'`. Judge rows: `office='MUNICIPAL COURT JUDGE'`, `jurisdiction='SEATTLE MUNICIPAL COURT'`, positions 1–7.
- Election-date caveat: PDC candidacy rows carry the **general** date (2026-11-03); our D5 primary election is 2026-08-04. Resolution must not require date equality — match on year + office + jurisdiction + position.
- Summary rows expose `reporting_option` (`full`/`mini`), `filing_type` (`Electronic`/paper), and `has_reports`. Mini-reporting campaigns (limit ~$7,000) file no itemized reports; paper filers can have empty calculated totals.
- Democracy vouchers: no dataset on data.seattle.gov (catalog verified empty) and none on data.wa.gov, but SEEC publishes current per-candidate voucher totals on its program-data page (2026 D5: Jenks $90,600, Kang $76,750, James $40,150). Voucher money already arrives inside PDC `contributions_amount` — any future voucher field is a *component of* raised money ("of total raised, voucher-funded"), never an addition. Historical cycles require a SEEC records request. Deferred (Phase 5).

## Pre-existing statewide bugs (fixed in PR #641 — Phases 1–2)

1. **Outside-spending undercount and wrong authority.** The C6 query filters `report_type = 'Independent Expenditure'` only and matches by candidate name (`washingtonPdcClient.ts`): Wilson opposition $717,855 captured vs $1,232,834.74 actual. Worse, `toSummary` (`washingtonCandidateFinanceSync.ts`) computes the headline outside totals by summing the fetched top-N groups and ignores the authoritative summary fields the client already parses (`independentExpendituresForAmount`/`AgainstAmount`).
2. **Occupation aggregation.** Server-side `$group=contributor_occupation` does no case normalization (`Retired`/`RETIRED`/`retired` split), and NULL occupation becomes a literal `"UNKNOWN"` category (`washingtonPdcClient.ts`).

## Phase 1: statewide outside-spending fix (SHIPPED — PR #641)

- Headline `outsideSupportTotal` / `outsideOpposeTotal` ← summary-row IE fields. Never derived from top-N group sums.
- Sponsor group breakdowns ← IE dataset across **all three C6 report types**; keep `portion_of_amount > 0` and `for_or_against in ('For','Against')`.
- Match C6 rows by stored hard IDs (`candidate_filer_id` / `candidate_committee_id`); name matching only as fallback for unlinked lookups.
- Reconciliation check: sum of fetched groups vs summary IE fields; log drift (timing skew between filings is expected — summary is the authority, groups are the explanation).
- Characterization fixtures: Wilson and Harrell 2025 values above.

## Phase 2: statewide occupation hygiene (SHIPPED — PR #641)

- Normalize server-side: `upper(trim(contributor_occupation))` in the `$select`/`$group`, and exclude NULL/blank rows in `$where` — no `"UNKNOWN"` bucket.
- Count semantics: `count(*)` counts contribution rows, not distinct donors. The UI deliberately does not render `contributor_count` today, so this is latent; keep the count but document it as row count (or drop it) — do not label it "contributors" if it ever renders.
- Disclosure note: occupations reflect donors above the $250 aggregate threshold — carry this in the direct coverage note (see Phase 3).

## Phase 3: Seattle (and WA-city) eligibility + resolution (SHIPPED)

Extend `washingtonFinanceEligibleOffices.ts` following the Illinois municipal precedent (`illinoisMunicipalityMatches` — generic name matching, no hardcoded city constants):

- Office keys: `place::Mayor`, `place::City Council Member`, `place::Municipal Attorney`, `place::Place Level Judge` (all exist in the offices catalog; SF precedent maps `Municipal Attorney` ↔ "City Attorney").
- PDC office mapping: `MAYOR`, `CITY COUNCIL MEMBER`, `CITY ATTORNEY`, `MUNICIPAL COURT JUDGE`; place-scope keys use `requiresJurisdiction` instead of legislative district.
- Jurisdiction normalizer handles both patterns to one city key: `CITY OF SEATTLE` → `SEATTLE`, `SEATTLE MUNICIPAL COURT` → `SEATTLE`; VoteApp side strips `city, Washington` from the district name. Generic — Bellevue, Spokane, Tacoma, Kirkland, Everett work the moment their rosters exist (verified present in PDC 2025 data). Rollout is naturally gated by roster coverage; no per-city allowlist.
- Position matching: PDC `position` is authoritative for council and court seats; our side parses the position from the ballot title (`Council District No. 5`, `Judge Position No. 5`). Require exact position agreement when both sides have one; never require election-date equality (primary vs general dates differ).
- `listWashingtonCandidateElectionsMissingFinanceLinks` (`washingtonCandidateFinanceAutoLink.ts`) currently extracts only the legislative district from the GEOID; it must also return district name, GEOID, and ballot title so place-scope resolution can build jurisdiction + position inputs.
- Client model: surface `position`, `jurisdiction`, `reporting_option`, `filing_type` on `WashingtonPdcCandidateSummary` (jurisdiction/has_reports already parsed; add the rest).
- Persist `filer_id` / `committee_id` / `candidacy_id` on links — columns already exist.
- Coverage state, minimal version: derive a per-candidate note from `reporting_option` / `filing_type` / `has_reports` / link status (mini-reporting, paper filing, no reports yet, unlinked) and emit it through the existing loader coverage-note fields (`directCoverageNote` / `outsideCoverageNote` — the Georgia pattern, `GEORGIA_DIRECT_COVERAGE_NOTE` precedent). No new UI contract; "unlinked" and "mini" must not render as $0.
- Shipped scope note: the coverage note landed as a static per-state `directCoverageNote` (the $250 occupation-disclosure threshold + batched small contributions), because the loader's note fields are per-state config and the summaries table has no per-candidate note column. "Unlinked" and "mini" already cannot render as $0 (no link row → no summary; missing totals stay null). `reporting_option`/`filing_type` are already parsed on `WashingtonPdcCandidateSummary`; only the per-candidate reporting-state note remains deferred (needs a summaries-table column plus writer/loader plumbing).

## Phase 4: rollout

- **No new module, feature flags, or source label** — Seattle rides `WASHINGTON_CAMPAIGN_FINANCE_ENABLED` / `..._SYNC_ENABLED` and the existing `washington_pdc` source enum (new-state checklist consulted; flag/label steps don't apply to an extension).
- "Raised" semantics contract test: PDC `contributions_amount` includes cash, in-kind, candidate's own money, and small-contribution batches; loans sit in a separate `loans_amount` field. Pin `totalReceipts = directContributionTotal = contributions_amount` behavior with a fixture and document the meaning ("PDC-reported contributions") in the loader note if wording matters.
- Legal release gate: PDC data terms restrict commercial use of lists of individuals (RCW 42.56.070). We display aggregates only — occupations, size buckets, group totals — and never republish donor names/addresses; that is policy, not proof of compliance. Before enabling Seattle (or any WA expansion) in production, get counsel/PDC written guidance on aggregate use under a revenue-bearing product. Same posture as other modules, now recorded as an explicit gate.
- BatchSync: Washington's bespoke due-list query gains place-scope office keys and passes jurisdiction/position through the search input.
- Tests: eligible-offices mapping (place keys, jurisdiction normalizer, position parse), resolver (category filter, position agreement, no date equality), Phase-1 IE fixtures, coverage-note derivation.
- End-to-end acceptance before merge: sync 2026 Seattle Council D5 (4 candidates) and Municipal Court Position 5 (3 candidates) locally; totals must match the verified figures listed above (refresh live values at run time — filings continue).

## Phase 5 (deferred, separate decision): democracy vouchers

- SEEC program-data page publishes current per-candidate voucher totals (HTML, no API; historical cycles via records request).
- If added: display as a component of raised money ("$X of total raised was voucher-funded"), sourced from SEEC, never summed on top of PDC totals. Needs a public-funds-style field (SF `sanFranciscoPublicFundsMatcher` precedent).
- Defer until Seattle base coverage ships and voucher visibility is actually wanted.

## Non-goals

- No donor-name or address republication; aggregates only.
- "Qualifying candidates" = candidates on our rosters for Seattle elections (ballot-qualified). PDC registration neither adds nor implies ballot status; voucher qualification is a Phase 5 concept kept separate.

## Appendix: where else the existing structure reaches (investigated 2026-08-10)

**Tier A — verified live, same already-integrated datasets, extension-only work:**

| Target | Module | Evidence |
|---|---|---|
| Seattle + Bellevue, Spokane, Tacoma, Kirkland, Everett (+ King/Snohomish counties, ports, municipal courts) | washingtonFinance | PDC summary dataset lists 2025–2026 local candidates with totals per jurisdiction |
| Honolulu (Mayor, Council, Prosecuting Attorney) + Maui/Kauai/Hawaii councils | hawaiiFinance | CSC contributions dataset office values: `Mayor` 17,004 rows, `Honolulu Council` 10,097, `Prosecuting Attorney` 2,536 |
| Chicago + all IL municipalities | illinoisFinance | **Already shipped** — `place::*` keys + `illinoisMunicipalityMatches` exist; Chicago elections in DB |

**Tier B — state portal is known to cover local filers; not live-verified this round:** Oregon/Portland (ORESTAR), Alaska/Anchorage (APOC), Maryland/Baltimore (CRIS), Massachusetts/Boston (OCPF), Kentucky/Louisville (KREF), Louisiana/New Orleans (state ethics), New York/Buffalo-Rochester (state BOE; NYC already separate), New Jersey/Newark-Jersey City (ELEC), Vermont/Burlington, Nebraska/Omaha-Lincoln (NADC — module is direct-only, no outside-spending tables).

**Tier C — no path through existing sources (local filing regimes; each would be a new city module like SF/LA/Houston):** other California cities (per-city NetFile), Texas cities beyond Houston, Philadelphia, Denver, Phoenix, and cities in FL/MI/MN/OH/WI/TN.

Priority note: the only near-term ballot value is Washington (Nov 2026 city generals, Seattle D5 + court positions already rostered). Hawaii county offices next (2026 cycle data live). Chicago's next municipal cycle is Feb 2027; most Tier B cities vote Nov 2027.
