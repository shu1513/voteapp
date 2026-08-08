# San Francisco Local Campaign Finance Plan

Written 2026-08-06; revised the same day after a second investigation round. Probed the San Francisco Ethics Commission (SFEC) datasets on DataSF, the official SFEC dashboard repository on GitHub, and audited the current campaign-finance architecture. The implementation must remain isolated, flag-gated, conservative about identity, and reuse the shared finance infrastructure (contract types, classification cache, standard-table loader pattern) instead of adding another large loader inside `ballotLookup.ts`.

The Los Angeles City adapter (`backend/src/pipeline/losAngelesCityFinance/`, migration `173`) is the closest structural blueprint. San Francisco's key difference: DataSF serves only raw filing data, but **SFEC publishes its reconciled dashboard data as machine-readable files in an official GitHub repository**, which this plan uses as the primary identity-and-totals source, with raw DataSF reconstruction as validator and fallback.

## Phase 0 status — COMPLETE (2026-08-06, branch `claude/sf-finance-phase-0`)

Implemented: `sanFranciscoDashboardManifestClient.ts` (0A), a lean `sanFranciscoOpenDataClient.ts` (0B oracle), and `probeSanFranciscoCandidateFinance.ts` (npm `san-francisco-candidates:finance:probe`), with parser/client unit tests. Live results across both canonical contests (10 mayoral + 5 D4 candidates):

- **Funds formula proven**: dashboard `funds` = Form 460 line-5 contributions (period prefix up to a cutoff filing) **+ public funds approved** (`dbak-p2fq`). Exact to the cent for 14 of 15 candidates (Breed's `$1,185,000`, Peskin's `$1,200,006`, Wong's `$252,000` public funds all reconcile exactly). Sole residual: Hirsch-Shell off by `$29.38` (immaterial; likely a returned-contribution edge).
- **Expenses are not raw-reproducible**: naive line-11 sums differ per candidate (`$0` for some, `+$51,338` Lurie, `+$102,896` Chow) — manifest stays authoritative for expenses.
- **Outside spending requires the manifest**: candidate-tagged F496 sums diverge from dashboard relations in both directions (Wong `$0` tagged vs `$744k` manifest; Breed `$3.19M` tagged vs `$2.69M` manifest), and Schedule D overlaps F496.
- **New source fact**: F496 transaction rows carry no `election_date` — contest bounding must use transaction-date windows.
- All four reference checks (Lurie funds/expenses, Wong funds/expenses/outside S/O) pass live.

**Decision: hybrid confirmed.** Manifest primary for identity, headline totals, and outside relations; DataSF for occupations/employers/buckets/balances/public funds; raw path retained as oracle via the probe.

**Explicitly NOT proven in Phase 0** (deferred to the Phase 4 entry gate, which must prove them before the direct-contribution aggregator ships): the itemized contributor formula (Sch A + C + 496 ≥$100 + 497P1 ≥$1,000), occupation/employer extraction, `entity_code` individual filtering, refund handling, F496/F497-vs-460 deduplication, memo/cross-reference rules, and outside-spender funding backtrace. Phase 0 validated headline totals and outside relations only. Also deferred to Phase 1: the rendered-site fetch fallback and manifest schema-version recording (the parser ships fail-loudly validation but no hash).

## Verified sources (probed live 2026-08-06)

### SFEC dashboard repository (primary for identity and headline totals)

`https://github.com/sfethics/dashboards-2025` is the source behind `https://campaign.sfethics.org` (the site is served directly from the repo via GitHub Pages; verified `server: GitHub.com`). It is updated daily by SFEC staff via automated "markdowns update" commits.

Per-contest files at `elections/<election-date>/contests/<contest>.md` contain YAML frontmatter with:

- `candidates[]`: `filer_nid`, `filer_id` (FPPC), `committee_name`, `candidate_name`, reconciled `funds`, `expenses` — exact dashboard numbers (verified: Lurie 2024 Mayor `10917642.52` / `10816112.85`; Wong June 2026 D4 `412371.0` / `410727.79`).
- `ie_candidates[]`: per candidate, each outside committee with `position` (`SUPPORT`/`OPPOSE`), `filer_id`, `committee_name`, and amounts (verified: Wong support committees sum to `744401.01` and oppose `24753.92`, matching the rendered dashboard to the cent).
- `contributors[]`: top contest contributors (informational; not needed for v1).

November 2026 contest files already exist: `asr` (Assessor-Recorder), `bos02/04/06/08/10`, `ccb` (Community College Board), `pdr` (Public Defender), `usd` (Board of Education).

Direction is per candidate-committee relation, not per committee: in `bos04.md` the GrowSF committee *supports* Wong while *opposing* Gee and Chow. Schema must model that.

Caveats (mitigations built into the plan): the repo is a publishing artifact, not a contracted API — no license/SLA, frontmatter schema can change, and the repo name is period-scoped (`dashboards-2025`) and will likely roll over for future cycles. Repo/branch must be configuration, parsing must be defensive and versioned, the GitHub contents API is the fetch fallback (Phase 1 finding: the rendered `campaign.sfethics.org` pages are Jekyll-built HTML and 404 the `.md` paths, so they can never feed the parser), and the raw-data path below stays alive as regression oracle. Elections before 2024-11-05 are not in this repo; deep historical backfill uses the raw path.

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
2. **Office-scope modeling (Phase −1) — DONE (2026-08-07).** Confirmed against the office catalog (`seedOffices.ts` + local `offices` table), `electionsValidator.ts`, the local district rows, and the live SFEC repo contest listings (2024-11-05: `bos01/03/05/07/09/11, cat, ccb, dat, myr, shf, ttx, usd`; 2026-11-03: `asr, bos02/04/06/08/10, ccb, pdr, usd`):

   | SFEC contest | Ballot office | Scope::canonical office | Geoid |
   |---|---|---|---|
   | `myr` | Mayor | `place::Mayor` | 0667000 |
   | `cat` | City Attorney | `place::Municipal Attorney` (catalog alias "City Attorney") | 0667000 |
   | `ttx` | Treasurer | `place::City Treasurer` | 0667000 |
   | `dat` | District Attorney | `county::District Attorney` | 06075 |
   | `shf` | Sheriff | `county::Sheriff` | 06075 |
   | `bosNN` | Supervisor District N (1–11) | `county::County Supervisor` | 06075 |
   | `asr` | Assessor-Recorder | `county::County Assessor-Recorder` | 06075 |
   | `pdr` | Public Defender | `county::Public Defender` | 06075 |
   | `usd` | Board of Education (at-large) | `school_unified::School Board Member` | 0634410 |

   The validator's `cityLike` check forces mayor/city titles into `place` and its `countyLike` check forces sheriff/DA titles into `county`, so these pairs are the only ones it accepts. Local ballot titles read "Member, Board of Supervisors, District N"; the local Nov 2026 rows are exactly the manifest contests minus `ccb`, all with zero candidates.
3. **Community College Board decision (Phase −1) — DECIDED (2026-08-07): deferred out of v1.** `ElectionDistrictType` has no community-college type and no `Community College Board Member` canonical office exists; the local database correspondingly has no CCB election row. SFEC data stays ready (`ccb.md` in both cycles), so the follow-up remains: an election-model migration (new district type or deliberate scoped modeling) if CCB is ever brought in.

## Goal and v1 scope

San Francisco municipal candidate races: Mayor, Board of Supervisors, City Attorney, District Attorney, Sheriff, Treasurer, Assessor-Recorder, Public Defender, Board of Education. Community College Board is excluded from v1 (Phase −1 decision — no district type or canonical office; follow-up migration filed above).

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

**0A — manifest parser** (done). Typed, defensive parser for the contest frontmatter: unknown keys tolerated, missing/renamed known keys fail loudly. Validated against Lurie (2024 Mayor) and the June 2026 D4 contest: candidate totals, committee identities, per-relation outside amounts match the rendered dashboards to the cent. Rendered-site fetch fallback and schema-version recording were deferred to the Phase 1 client.

**0B — raw reconstruction as headline oracle** (done, deliberately narrower than first drafted). From DataSF, reproduce the dashboard funds figure (Form 460 line-5 prefix + public funds) and quantify how far candidate-tagged F496/Schedule D rows diverge from the manifest's outside relations. The itemized contributor formula (Sch A + C + 496 ≥$100 + 497P1 ≥$1,000, refunds, `entity_code`, memo/cross-reference rules, spender backtrace) was NOT exercised here — it belongs to the Phase 4 entry gate, where the direct-contribution aggregator needs it.

**Decision point** (done). Hybrid confirmed: manifest primary, raw oracle for headline reconciliation. Only composition rules proven by a gate ship. Record the methodology version with every snapshot.

## Phase 1: clients — COMPLETE (2026-08-06, branch `claude/sf-finance-phase-1`)

`backend/src/pipeline/sanFranciscoFinance/`:

- `sanFranciscoDashboardManifestClient.ts` — fetches contest files from the configured SFEC repo (raw.githubusercontent.com, branch + repo name in config), parses frontmatter, returns typed contest data. Bounded timeouts, retry on 429/5xx. Phase 1 added: every parse records a `schemaFingerprint` (sorted union of frontmatter keys per level) so upstream drift is visible in diagnostics before it breaks parsing, and the fetch falls back to the GitHub contents API (`api.github.com/repos/<repo>/contents/…`, verified byte-identical) when the raw host fails. **Deviation from the original plan**: the rendered-site fallback is impossible — `campaign.sfethics.org` is Jekyll-built HTML and 404s the `.md` paths (verified live), so the contents API is the fallback instead. Parse failures are never retried through the fallback (same bytes).
- `sanFranciscoOpenDataClient.ts` — SODA client modeled on `losAngelesOpenDataClient.ts`: base `https://data.sfgov.org/resource/<id>.json`, optional `SAN_FRANCISCO_OPEN_DATA_APP_TOKEN` (`X-App-Token`), 30s timeout, retry on 429/5xx, stable `$order` including `:id`, bounded paging. Typed fetchers: filers by fppc_id/candidate-name fragment (`4c8t-ngau`, "pending" ids map to null); summary rows by `fppc_id`; itemized transactions by committee + explicit form types + transaction-date bounds; public-funds rows by election date + optional district. Fetches `calculated_amount` (canonical) and `transaction_amount_1` (diagnostic); integer cents throughout; server-side `$select`/`$where`/`$group`; per-row rejects drop the row, never throw. Live-verified: Wong filer lookup, 525 Schedule A rows ($157,005.00), 27 mayoral 2024 public-funds rows.
- **Phase 1 discovery for the Phase 4 gate**: `cross_reference_match` and `cross_reference_schedule` are 100% null across all 971k transaction rows (verified 2026-08-06) — the late-filing dedupe cannot rely on them and must be proven with transaction ids/amount-and-date matching instead. The columns stay in the row type so upstream repopulation becomes visible.

## Phase 2: eligibility and office mapping — COMPLETE (2026-08-07, branch `claude/sf-finance-phase-2`)

`sanFranciscoFinanceEligibleOffices.ts`:

- State `CA`; district/GEOID pairs: `county::06075`, `place::0667000`, `school_unified::0634410`; office scope must equal the district type it sits in.
- Office → contest-code map per the Phase −1 table above; `County Supervisor` → `bosNN` (zero-padded, N ∈ 1–11 parsed NFKD-normalized from "Member, Board of Supervisors, District N"); CCB absent by decision.
- Eligibility is exact; the contest code doubles as the manifest file locator. Live-verified against the local database: all eight Nov 2026 races map to their exact SFEC contest codes, all five ballot measures reject.

## Phase 3: identity, links, and relations

`sanFranciscoCandidateCommitteeResolver.ts` + `sanFranciscoCandidateFinanceAutoLink.ts`:

- Candidate → controlled committee comes **from the manifest**: the contest file names each candidate with `filer_nid` + `filer_id`. Match manifest `candidate_name` to VoteApp candidates by normalized name within the already-eligibility-matched contest — no committee-name heuristics needed. Cross-check `filer_type = 'Candidate or Officeholder'` in the filer registry. Ambiguity (two VoteApp candidates normalize alike, or manifest candidate matches nobody) fails closed into `needs_review` / unresolved; no name-only auto-links outside the contest context.
- Outside relations come from the manifest's `ie_candidates`: store one row per (candidate, election, spender committee, direction). When a manifest entry lacks a usable committee id, retain it under a stable synthetic id derived from source + election + normalized committee name — never silently drop money.
- Links become trusted identities on later syncs; manifest disappearance of a linked committee flags the link instead of deleting it.

## Phase 4: aggregation

- `sanFranciscoHeadlineTotals` (from manifest): `total_raised` = `funds`, `total_spent` = `expenses` per candidate. Outside `support_total`/`oppose_total` = sums of the candidate's manifest relations by direction; groups list = the relations themselves with source URLs to the contest dashboard.
- `sanFranciscoBalanceAggregator` (from summary totals): cash on hand and outstanding debt from the latest filing's ending-balance lines; loans from Schedule B lines (`loans_received`, excluded from `total_raised` per the shared contract). Assert the no-duplicate-`filing_nid` guarantee.
- `sanFranciscoPublicFundsMatcher` (from `dbak-p2fq`): match by election date + district + normalized candidate name; sum `funds_approved` rows directly — every published row is an approval. `pending_completed` is no longer populated by the source (verified live: 152 valid approved rows carry it blank) and must never gate approval; `date_certified_approved` may optionally be required as a stricter signal. Ambiguous name matches fail closed (no public-funds figure rather than a wrong one). Mayor/Supervisor only.
- `sanFranciscoDirectContributionAggregator` (from transactions): **entry gate first** — before this aggregator ships, extend the probe to prove the itemized contributor formula against the two canonical races (Sch A + C itemized + qualifying 496/497P1 late-period rows, dedupe against later 460 reporting via transaction IDs and amount/date matching — `cross_reference_match`/`cross_reference_schedule` are 100% null upstream per the Phase 1 discovery — memo-row handling shown by the data rather than blanket-excluded, refunds negative). Phase 0 did not exercise these rules. Then implement the proven formula. Occupation/employer analysis restricted to individual contributors via `entity_code`. Refunds reduce totals and are excluded from size buckets (no absolute-value bucketing). Occupations stay as disclosed — `classifyFinanceLabel` classifies industries and does not merge occupation synonyms ("ATTORNEY" and "LAWYER" remain separate rows); any alias layer is a separately tested follow-up, not v1. Reconcile the itemized sum against the manifest `funds` figure and store the difference (unitemized + timing) as a diagnostic.

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

**Production scheduling is explicit, not implied.** render.yaml currently enables read flags only and deliberately omits all `*_SYNC_ENABLED` flags and finance cron jobs, so creating scheduler scripts alone runs nothing. v1 recommendation: a one-shot Render cron invoking the due-sync command daily at **16:30 UTC** (09:30 PDT / 08:30 PST — after DataSF's nightly refresh year-round), with the sync flag set on the cron service only. Alternative: deploy a resident scheduler worker. Either way the deploy checklist names the choice.

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
- Contributor formula: Sch A + C + 496/497P1 composition, late-period dedupe vs later 460s, refund sign handling and bucket exclusion, `entity_code` individual filter, memo-row rule as proven by the Phase 4 entry gate.
- Resolver: manifest-driven linking, ambiguous-name fail-closed, disappearance flagging, prior-office committee rejection.
- Public funds: name+district match, approved-only summation, ambiguity fail-closed.
- Eligibility: per-office scope matrix from Phase −1, seat parsing, non-SF CA rejection, LA/CA-state adapters untouched.
- Writer: all-or-nothing staging, preserve-vs-clear semantics, methodology version stamping.
- No-AI guarantee: sync path performs zero classifier calls; unresolved labels enqueue to the manual due queue.
- Loader registration and shared finance output shape; flag gating; scheduler-choice behavior.

Validation gates: backend typecheck; focused SF tests; full backend suite; empty-database migration run; api-client tests; live Lurie + June-2026-D4 probe with manifest headline and outside values matching the rendered dashboards to the cent, while the raw path validates the proven contribution formula and records residuals (raw expenses and outside tagging are documented as non-reproducible); then a live 2026-cycle run against real linked candidates once rosters exist, and database-backed candidate-profile API verification.

## Sequencing

1. **Phase −1** (done): office-scope confirmation + Community College Board deferral. Roster work (the eight Nov 2026 races in the local database) proceeds in parallel via the manual-research skill.
2. **Phase 0** dual-path validation. Gate + architecture decision.
3. Phases 1–6 (clients, eligibility, identity, aggregation, schema, sync).
4. Phase 7 scheduling decision wired into deploy config; Phase 8 loader + label; Phase 9 validation.
5. Targeted historical backfill (2024 Mayor et al.) via explicit election targeting; then enable flags per the deploy checklist.
