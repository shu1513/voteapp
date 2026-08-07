# San Francisco Local Campaign Finance Plan

Written 2026-08-06; revised the same day after a second investigation round. Probed the San Francisco Ethics Commission (SFEC) datasets on DataSF, the official SFEC dashboard repository on GitHub, and audited the current campaign-finance architecture. The implementation must remain isolated, flag-gated, conservative about identity, and reuse the shared finance infrastructure (contract types, classification cache, standard-table loader pattern) instead of adding another large loader inside `ballotLookup.ts`.

The Los Angeles City adapter (`backend/src/pipeline/losAngelesCityFinance/`, migration `173`) is the closest structural blueprint. San Francisco's key difference: DataSF serves only raw filing data, but **SFEC publishes its reconciled dashboard data as machine-readable files in an official GitHub repository**, which this plan uses as the primary identity-and-totals source, with raw DataSF reconstruction as validator and fallback.

## Verified sources (probed live 2026-08-06)

### SFEC dashboard repository (primary for identity and headline totals)

`https://github.com/sfethics/dashboards-2025` is the source behind `https://campaign.sfethics.org` (the site is served directly from the repo via GitHub Pages; verified `server: GitHub.com`). It is updated daily by SFEC staff via automated "markdowns update" commits.

Per-contest files at `elections/<election-date>/contests/<contest>.md` contain YAML frontmatter with:

- `candidates[]`: `filer_nid`, `filer_id` (FPPC), `committee_name`, `candidate_name`, reconciled `funds`, `expenses` — exact dashboard numbers (verified: Lurie 2024 Mayor `10917642.52` / `10816112.85`; Wong June 2026 D4 `412371.0` / `410727.79`).
- `ie_candidates[]`: per candidate, each outside committee with `position` (`SUPPORT`/`OPPOSE`), `filer_id`, `committee_name`, and amounts (verified: Wong support committees sum to `744401.01` and oppose `24753.92`, matching the rendered dashboard to the cent).
- `contributors[]`: top contest contributors (informational; not needed for v1).

November 2026 contest files already exist: `asr` (Assessor-Recorder), `bos02/04/06/08/10`, `ccb` (Community College Board), `pdr` (Public Defender), `usd` (Board of Education).

Direction is per candidate-committee relation, not per committee: in `bos04.md` the GrowSF committee *supports* Wong while *opposing* Gee and Chow. Schema must model that.

Caveats (mitigations built into the plan): the repo is a publishing artifact, not a contracted API — no license/SLA, frontmatter schema can change, and the repo name is period-scoped (`dashboards-2025`) and will likely roll over for future cycles. Repo/branch must be configuration, parsing must be defensive and versioned, the rendered `campaign.sfethics.org` pages are the fetch fallback, and the raw-data path below stays alive as regression oracle. Elections before 2024-11-05 are not in this repo; deep historical backfill uses the raw path.

### DataSF datasets (breakdowns, balances, public funds, validation)

All on `https://data.sfgov.org` (Socrata/SODA), refreshed nightly; DataSF warns overnight processing can leave datasets temporarily incomplete, so syncs run after ~08:00 Pacific.

| Dataset | ID | Used for |
|---|---|---|
| Campaign Finance - Transactions | `pitq-e56w` | ~971k rows. Occupations, employers, size buckets, refunds: `transaction_occupation`, `transaction_employer`, `calculated_amount`, `entity_code`, `form_type`, `transaction_code`, `memo_code`, `cross_reference_match`/`_schedule`, `is_itemized`, `support_oppose_code`, candidate/office/election fields |
| Campaign Finance - Summary Totals | `9ggq-m8hp` | Form 460 summary lines per filing: cash, debt, loans (`line_*_col_a/b`, schedule totals), `sync_flag`, `data_as_of` |
| Campaign Finance - SF Campaign Filers | `4c8t-ngau` | Filer registry: `fppc_id`, `filer_nid`, `filer_type` (`Candidate or Officeholder` / `Primarily Formed Candidate` / `General Purpose`), `candidate_name`, `status` |
| Campaign Finance - Filings Received by SFEC | `qizs-bwft` | Filing index; freshness cross-checks |
| Campaign Finance - Public Funds Approved | `dbak-p2fq` | Public financing. **No committee id**: only `election_date`, `district`, `candidate`, `date_of_submission`, `date_certified_approved`, `pending_completed`, `funds_approved`. Mayor and Supervisor candidates only |

Source guarantees verified and relied upon:

- **Amendment supersession is upstream.** The published datasets contain only the current version of each filing (verified: max one row per `filing_nid` in summary totals across Lurie's nine filings, amendments included). Do not re-implement version selection; instead assert the guarantee (duplicate-`filing_nid` detection fails the sync loudly) and keep `filing_version` in diagnostics.
- **`calculated_amount` is the canonical amount column** on transactions ("most appropriate" amount per heterogeneous form type). Aggregators use it; `transaction_amount_1` is fetched for diagnostics only. All arithmetic in integer cents.
- **SFEC's documented contributor methodology** (from the dashboard pages): itemized contributions on Form 460 Schedules A and C, plus contributions reported on Form 496 (≥ $100) and Form 497 Part 1 (≥ $1,000) during the late reporting period. Returned contributions appear as negative Schedule A rows (same period) or Schedule E `RFD` rows (later period).

### Validated cross-checks already performed

- Summing Lurie's 460 `line_5_col_a` across filings reproduces the manifest `funds` figure exactly (`$10,917,642.52`); the naive `line_11_col_a` expense sum does *not* match the manifest expenses — reinforcing that headline totals come from the manifest, not raw sums.
- Candidate-tagged `F496` rows alone miss most outside money in small races (Wong: `$1,680` tagged vs `$744k` actual via primarily-formed and general-purpose committees) — reinforcing that outside-group relations come from the manifest.
- Schedule `D` rows duplicate `F496` activity (Lurie oppose: D `$2.89M` ≈ F496 `$2.99M`) — raw-path reconciliation must deduplicate; the manifest path avoids the problem entirely.

## Hard prerequisites (before the adapter can display anything)

1. **Candidate rosters.** Every November 2026 San Francisco election in the local database has zero linked candidates. Roster and profile work goes through the `voteapp-manual-research` skill. Can proceed in parallel with adapter phases.
2. **Office-scope modeling (Phase −1, blocking).** San Francisco is a consolidated city-county and the catalog splits its offices across scopes. Verified constraints:
   - `backend/src/pipeline/validators/electionsValidator.ts` rejects mayor-titled races under `county` scope (`cityLike` check) and DA/Sheriff-titled races under `place` scope (`countyLike` check).
   - Current local rows: Supervisor, Assessor-Recorder, Public Defender as `county::06075`; Board of Education as `school_unified::0634410`; ballot measures as `place::0667000`.
   - Expected catalog placement for offices not yet ingested: `place::Mayor`, `place::Municipal Attorney` (City Attorney), `place::City Treasurer`; `county::District Attorney`, `county::Sheriff`.
   Phase −1 confirms each SF office's canonical office + scope against the catalog and validator before eligibility code is written.
3. **Community College Board decision (Phase −1).** `ElectionDistrictType` has no community-college type (`backend/src/types/election.ts` — only elementary/secondary/unified school types) and no `Community College Board Member` canonical office exists. SFEC data is ready (`ccb.md` in the manifest), but VoteApp cannot model the contest. Decide explicitly: (a) defer CCB out of v1, or (b) add an election-model migration (new district type or a deliberate county/place-scoped modeling of this SF-only contest). Roster research alone cannot solve this; default recommendation is (a) defer, ship the other nine offices, and file (b) as follow-up.

## Goal and v1 scope

San Francisco municipal candidate races: Mayor, Board of Supervisors, City Attorney, District Attorney, Sheriff, Treasurer, Assessor-Recorder, Public Defender, Board of Education (Community College Board pending the Phase −1 decision).

Per candidate and election cycle:

- Total raised and spent — taken from the official manifest (reconciled), so they match `campaign.sfethics.org` exactly.
- Cash on hand, outstanding debt, loans received — from DataSF summary totals (latest filing's balance lines).
- Public funds received — from the public-funds dataset (Mayor/Supervisor candidates only).
- Top direct-donor occupations and employers (itemized, individuals only) and standard contribution-size buckets — from DataSF transactions using SFEC's documented contributor formula.
- Outside support/oppose totals and named groups — from the manifest's per-candidate `ie_candidates` relations.
- Industries via deterministic classification + previously cached manual classifications only. **No AI calls in finance sync** (matches current policy: due-sync scripts inject no classifier; unresolved high-value labels feed the existing manual industry-label due queue).

Out of scope for v1: ballot-measure committees, behested payments, DCCC races, Individual Expenditure Ceiling data, unitemized-donor analysis (no donor identity exists by law), and employer *display* (see Phase 8).

## Phase 0: dual-path validation gate

A standalone probe (`probeSanFranciscoCandidateFinance.ts`, kept afterward as a smoke test) proves both paths before any schema or runtime code:

**0A — manifest parser.** Typed, defensive parser for the contest frontmatter (unknown keys tolerated, missing/renamed known keys fail loudly, schema hash recorded). Fetch from configured repo raw URLs with the rendered-site fallback. Validate against Lurie (2024 Mayor) and the June 2026 D4 contest: candidate totals, committee identities, per-relation outside amounts must match the rendered dashboards to the cent.

**0B — raw reconstruction as oracle.** From DataSF only, reproduce for the same two races: contribution totals per the documented contributor formula (Sch A + C itemized + 496 ≥$100 + 497P1 ≥$1,000 late, refunds negative), and outside totals (F496 + primarily-formed committee spending, Schedule D/F496 dedupe via `cross_reference_match`/`cross_reference_schedule`/transaction IDs, memo-row handling proven — not blanket-excluded until the data shows the rule). Compare raw vs manifest vs rendered site; document every residual difference and its cause.

**Decision point.** Confirm hybrid (manifest primary, raw oracle) or fall back to raw-only if the manifest proves unstable. Only the proven composition rules ship. Record the methodology version with every snapshot.

## Phase 1: clients

`backend/src/pipeline/sanFranciscoFinance/`:

- `sanFranciscoDashboardManifestClient.ts` — fetches contest files from the configured SFEC repo (raw.githubusercontent.com, branch + repo name in config, rendered-site fallback), parses frontmatter, returns typed contest data. Bounded timeouts, retry on 429/5xx.
- `sanFranciscoOpenDataClient.ts` — SODA client modeled on `losAngelesOpenDataClient.ts`: base `https://data.sfgov.org/resource/<id>.json`, optional `SAN_FRANCISCO_OPEN_DATA_APP_TOKEN` (`X-App-Token`), 30s timeout, retry on 429/5xx, stable `$order` including `:id`, bounded paging. Typed fetchers: filers by name/fppc_id; summary rows by `fppc_id`; itemized transactions by committee and by form types; public-funds rows by election date + district. Fetch `calculated_amount` and `transaction_amount_1`; aggregate on `calculated_amount`; integer cents throughout. Server-side `$select`/`$where`/`$group` to keep transfers small. Defensive row mapping; per-row rejects never throw.

## Phase 2: eligibility and office mapping

`sanFranciscoFinanceEligibleOffices.ts`, written **after** Phase −1 confirms scopes:

- State `CA`; district/GEOID pairs: `county::06075`, `place::0667000`, `school_unified::0634410`.
- Office → (scope, contest-code) map per Phase −1 results, e.g. `County Supervisor` → `county` + `bosNN` (seat from ballot title "District N", N ∈ 1–11, NFKD-normalized parsing like LA's), `County Assessor-Recorder` → `county` + `asr`, `Public Defender` → `county` + `pdr`, `Mayor` → `place` + `myr`, `School Board Member` → `school_unified` + `usd`, etc.
- Eligibility is exact; contest-code mapping doubles as the manifest file locator.

## Phase 3: identity, links, and relations

`sanFranciscoCandidateCommitteeResolver.ts` + `sanFranciscoCandidateFinanceAutoLink.ts`:

- Candidate → controlled committee comes **from the manifest**: the contest file names each candidate with `filer_nid` + `filer_id`. Match manifest `candidate_name` to VoteApp candidates by normalized name within the already-eligibility-matched contest — no committee-name heuristics needed. Cross-check `filer_type = 'Candidate or Officeholder'` in the filer registry. Ambiguity (two VoteApp candidates normalize alike, or manifest candidate matches nobody) fails closed into `needs_review` / unresolved; no name-only auto-links outside the contest context.
- Outside relations come from the manifest's `ie_candidates`: store one row per (candidate, election, spender committee, direction). When a manifest entry lacks a usable committee id, retain it under a stable synthetic id derived from source + election + normalized committee name — never silently drop money.
- Links become trusted identities on later syncs; manifest disappearance of a linked committee flags the link instead of deleting it.

## Phase 4: aggregation

- `sanFranciscoHeadlineTotals` (from manifest): `total_raised` = `funds`, `total_spent` = `expenses` per candidate. Outside `support_total`/`oppose_total` = sums of the candidate's manifest relations by direction; groups list = the relations themselves with source URLs to the contest dashboard.
- `sanFranciscoBalanceAggregator` (from summary totals): cash on hand and outstanding debt from the latest filing's ending-balance lines; loans from Schedule B lines (`loans_received`, excluded from `total_raised` per the shared contract). Assert the no-duplicate-`filing_nid` guarantee.
- `sanFranciscoPublicFundsMatcher` (from `dbak-p2fq`): match by election date + district + normalized candidate name; sum rows whose status (`pending_completed` / certification date) marks them approved; ambiguous name matches fail closed (no public-funds figure rather than a wrong one). Mayor/Supervisor only.
- `sanFranciscoDirectContributionAggregator` (from transactions): the documented contributor formula (Sch A + C itemized + qualifying 496/497P1 late-period rows, deduplicated against later 460 reporting per Phase 0 rules). Occupation/employer analysis restricted to individual contributors via `entity_code`. Refunds reduce totals and are excluded from size buckets (no absolute-value bucketing). Occupations stay as disclosed — `classifyFinanceLabel` classifies industries and does not merge occupation synonyms ("ATTORNEY" and "LAWYER" remain separate rows); any alias layer is a separately tested follow-up, not v1. Reconcile the itemized sum against the manifest `funds` figure and store the difference (unitemized + timing) as a diagnostic.

## Phase 5: schema, flags, writer

Next free migration number at implementation time; `sfc_` prefix; identifiers ≤ 63 chars:

- `sfc_candidate_finance_links` — candidate/election identity, manifest contest code, `fppc_id`, `filer_nid`, committee name, link status (`active`/`needs_review`/`inactive`), link source, source URL, `last_verified_at`. Modeled on migration 173's link table.
- `sfc_candidate_finance_outside_committee_links` — normalized relation table (replaces any jsonb idea): `candidate_id`, `election_id`, `election_year`, `spender_fppc_id` (or synthetic id), `spender_filer_nid`, `spender_name`, `support_oppose`, `relation_source`, `source_url`, `last_verified_at`; unique per (candidate, election, spender, direction). A committee may legitimately appear supporting one candidate and opposing another.
- `sfc_candidate_finance_summaries` — LA columns plus `debts_owed`, `loans_received`, `public_funds_received`, `methodology_version`; no membership columns (SF does not disclose member communications separately).
- `sfc_candidate_finance_direct_breakdowns` (`occupation` / `employer` / `industry` / `contribution_size`).
- `sfc_candidate_finance_outside_groups` (per-cycle amounts per relation row).

Flags, default off, LA-pattern pair:

- `SAN_FRANCISCO_CAMPAIGN_FINANCE_ENABLED`
- `SAN_FRANCISCO_CAMPAIGN_FINANCE_SYNC_ENABLED` (requires the first; `force` bypasses only the flag gate — see Phase 7 for backfill targeting)

`sanFranciscoFinanceWriter.ts`: **all-or-nothing candidate snapshots.** Stage every component (headline, balances, public funds, direct breakdowns, outside groups); write transactionally only when every required component passed source-health checks. No mixed-as-of summaries (partially fresh direct totals alongside stale outside totals must be impossible). Source unavailable → preserve prior snapshot untouched; source affirmatively returns no qualifying data → write zero totals and clear stale details.

## Phase 6: sync, batch, and source health

- `sanFranciscoCandidateFinanceSync.ts` — one candidate/election: manifest fetch, DataSF fetches, aggregate, deterministic + cached-manual industry classification only (no classifier injected; unresolved labels ≥ the existing threshold are enqueued to the manual industry-label due queue), stage, health-check, write, return diagnostics (raw-vs-manifest reconciliation difference included).
- Source-health checks before any write: dataset `data_as_of`/`data_loaded_at` recency vs the filings index; summary `sync_flag` validation; cross-dataset freshness coherence; previous-vs-new total anomaly bounds (an order-of-magnitude drop on an unchanged filing set aborts the write and reports).
- `sanFranciscoCandidateFinanceBatchSync.ts` — LA due-query pattern with Phase 2 geography predicates: auto-link missing links first (warn-and-continue), sync due links stalest-first; defaults 25 candidates, 1-day staleness, 45-day lookback, 730-day lookahead; skip `withdrawn`/`lost`. Manifest contest files cached per batch run.

## Phase 7: scheduling, scripts, backfill

Scripts mirroring the LA set: `syncDueSanFranciscoCandidateFinance.ts`, `triggerSanFranciscoCandidateFinanceSync.ts`, `probeSanFranciscoCandidateFinance.ts`, plus scheduler upsert/worker scripts if the resident-worker route is chosen.

**Production scheduling is explicit, not implied.** render.yaml currently enables read flags only and deliberately omits all `*_SYNC_ENABLED` flags and finance cron jobs, so creating scheduler scripts alone runs nothing. v1 recommendation: a one-shot Render cron invoking the due-sync command daily at **16:30 UTC** (08:30 PDT / 09:30 PST — after DataSF's nightly refresh year-round), with the sync flag set on the cron service only. Alternative: deploy a resident scheduler worker. Either way the deploy checklist names the choice.

**Historical backfill** (2024 Mayor et al.): `--force` bypasses only the feature-flag gate, not the due-query date window. Backfill needs historical candidate/election links to exist plus explicit targeting — `--election-id` / `--lookback-days` options on the due-sync script — and a test proving the ordinary due query cannot silently pull unbounded history. Pre-2024 elections are absent from the manifest repo; anything older runs raw-path-only and is a separate, later decision.

## Phase 8: ballot-lookup and UI integration

- Add source value `SAN_FRANCISCO_ETHICS` to the union in `backend/src/pipeline/address/ballotLookupFinanceShared.ts`.
- Add `sanFranciscoBallotLookupFinanceLoader.ts` (flag-gated, exact-eligibility) via the shared standard-table loader helper; register after the CA state and LA city entries in the `ballotLookup.ts` registry — GEOID eligibility keeps the domains disjoint.
- API-client label (web + mobile cards): `SAN_FRANCISCO_ETHICS`: `San Francisco Ethics Commission`.
- UI reality, stated precisely: **no UI changes are required** for raised/spent, occupations, size buckets, cash/debt/loans/public funds, or outside groups — the existing profile card renders those. Employer and direct-industry breakdowns are **stored but deliberately not displayed** (existing product decision documented in `FinanceSummaryCard.tsx`); showing employers would be a separate product decision, not part of this adapter.
- Set `outside_coverage_note` only if Phase 0 identifies a systematic gap in the manifest's outside coverage; otherwise leave unset.

## Phase 9: tests and validation

Fixtures are compact captured rows/frontmatter, not dataset dumps.

- Manifest parser: schema drift (unknown keys pass, missing known keys fail), synthetic-id fallback for id-less committees, dual-direction committee relations, fetch fallback.
- SODA client: paging, retry, `$where` construction, malformed-row rejection, cents conversion.
- Source guarantees: duplicate-`filing_nid` assertion fires; `sync_flag`/freshness gates blockwrites; anomaly bounds.
- Contributor formula: Sch A + C + 496/497P1 composition, late-period dedupe vs later 460s, refund sign handling and bucket exclusion, `entity_code` individual filter, memo-row rule as proven in Phase 0.
- Resolver: manifest-driven linking, ambiguous-name fail-closed, disappearance flagging, prior-office committee rejection.
- Public funds: name+district match, approved-only summation, ambiguity fail-closed.
- Eligibility: per-office scope matrix from Phase −1, seat parsing, non-SF CA rejection, LA/CA-state adapters untouched.
- Writer: all-or-nothing staging, preserve-vs-clear semantics, methodology version stamping.
- No-AI guarantee: sync path performs zero classifier calls; unresolved labels enqueue to the manual due queue.
- Loader registration and shared finance output shape; flag gating; scheduler-choice behavior.

Validation gates: backend typecheck; focused SF tests; full backend suite; empty-database migration run; api-client tests; live Lurie + June-2026-D4 probe matching the rendered dashboards to the cent on both paths; then a live 2026-cycle run against real linked candidates once rosters exist, and database-backed candidate-profile API verification.

## Sequencing

1. **Phase −1**: office-scope confirmation + Community College Board modeling decision. Roster work (all nine/ten races) proceeds in parallel via the manual-research skill.
2. **Phase 0** dual-path validation. Gate + architecture decision.
3. Phases 1–6 (clients, eligibility, identity, aggregation, schema, sync).
4. Phase 7 scheduling decision wired into deploy config; Phase 8 loader + label; Phase 9 validation.
5. Targeted historical backfill (2024 Mayor et al.) via explicit election targeting; then enable flags per the deploy checklist.
