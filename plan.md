# Finance module consolidation plan (v2)

Date: 2026-08-01. Scope: `backend/src/pipeline/*Finance` (34 modules, ~140k lines) plus the shared `backend/src/pipeline/finance/` module. v2 incorporates a second review pass; every claim below was verified against the code in this worktree.

## Background

The July 12–18 refactor wave extracted four shared pieces:

- `createStandardStateFinanceSnapshotWriter` (factory, 610 lines) — adopted by **Texas and Houston only**. Config surface today is only `{label, tables}`.
- `standardStateFinanceBallotLookupLoader` (parameterized loader, 596 lines) — adopted by **Texas, Illinois, Houston only**.
- SQL label-normalization function (replaced 22 inline blobs) — fully propagated.
- Shared outside-industry explanation builder — adopted by ~23 modules. Fully propagated.

After the refactor, feature work continued in the copy-per-state pattern. Fixes land in single states and do not propagate to the 30+ sibling copies.

## Evidence of duplication

Pairwise similarity after normalizing state names and whitespace (best-match sibling, % identical lines):

| Role file | Files | Lines | Avg similarity | Verdict |
|---|---|---|---|---|
| `*FinanceWriter` | 34 | 17.5k | 89% (32 ≥85%) | migrate to factory, exact-shape cohort first |
| `*BallotLookupFinanceLoader` | 31 modules | 12.4k | 86% | migrate after descriptor redesign + characterization tests |
| `*CandidateFinanceAutoLink` | 28 | 7.2k | 86% | extract two primitives, not one factory |
| `*CandidateFinanceBatchSync` | 34 | 17k | 84% | due-list builder first; orchestration later |
| `*CandidateFinanceSync` | 33 | 13.9k | 78% | partial extraction only, if at all |
| `*FinanceEligibleOffices` | 34 | 4k | 82% | shared types only; data is real config |
| `*CandidateCommitteeResolver` | 31 | 10.4k | 70% | leave — genuinely state-specific |
| `*DirectContributionAggregator` | 30 | 8.4k | 74% | leave |
| `*OutsideSpendingAggregator` | 20 | 5.7k | 62% | leave |

City outliers: `newYorkCityFinance` and `losAngelesCityFinance` writers are ~35%/31% similar to any sibling. Excluded from writer/loader migration.

**Line similarity hides schema differences.** Verified examples the percentages don't show:

- Colorado's writer is direct-contributions-only — zero outside-spending code; the factory's five mandatory tables cannot represent it.
- California adds `needs_review`, `debtsOwed`, extra category types.
- Illinois adds SBE fields and a signed cash balance; Kentucky (`normalizeNullableSignedAmount`) and Louisiana (`normalizeNullableBalance`) also allow negative cash. The factory rejects negative amounts everywhere.
- Hawaii keys links on `election_period`; Wisconsin on `entity_id` + `assigned_committee_id`; Vermont uses a numeric entity id.

### Semantic drift found (why this is worth doing)

| Feature | Where it exists | Shared factory has it? |
|---|---|---|
| "must receive a Pool" transaction guard | 21 of 35 writers | yes |
| Outside-group-breakdown ↔ group validation | pairing in 12, presence-only in 10 (see matrix) | **configurable since PR #488** (`outsideGroupValidation`, default none) |
| `normalizeCommitteeId` (whitespace collapse; Maine also uppercases) | 2 of 35 (ME, MD) | **configurable since second-wave prep PR** (`normalizeCommitteeId` fn) |
| Same-source link supersession in replaceSnapshot | 2 of 35 (ME `cfis_bulk`, MD `cfs_public_export`) | **configurable since second-wave prep PR** (`supersededLinkSource`) |
| Summary upsert policy | States split across replace-all, COALESCE-all, and mixed (see matrix) | **configurable since PR #488** (`summaryUpdatePolicy`, default = old COALESCE-all) |
| Election-year floor | NJ 1980, FL 1996, UT 1998, 20 states 2000, CA/CO 2001, AZ 2002, CT 2008, OK 2014, NM 2020, NE 2021 | **required config since PR #488** (`minElectionYear`; was hardcoded 2014) |
| Outside-group identity columns | 9 states factory-canonical `committee_id`; 4 `committee_key`; 3 `sponsor_id`; 8 one-offs (see matrix) | **configurable since PR #509** (`outsideGroupIdentityColumns`, default `committee_id`/`committee_name`) |

A bug fixed in one state's writer today fixes 1 of 32 copies.

## Safety net (verified counts)

- 34 writers, 34 writer tests (Minnesota's was missing; added in PR #488). The factory has its own config-option test file since PR #488.
- 34 batchSync files, 34 tests. 28 auto-link files, 25 tests. **Only 10 loader tests for 31 loaders** — loader migration is NOT low-risk until characterization tests exist.
- Proven recipe: Texas writer migration (`6b2664b2`) kept every exported name/type as a thin wrapper; tests passed unchanged.
- Gates per PR: `npm run typecheck` + `npm test` in `backend/`.

## Phases

### Phase 0 — capability matrix + test gaps — **DONE (PR #488)**

One checked-in markdown matrix (`docs/finance-module-capability-matrix.md`), one row per module: link/outside-group identity columns; optional tables/features; extra link+summary fields; category unions; election-year floor; per-field null-merge policy (replace vs preserve); signed-value fields; transaction ownership (Pool-only vs caller-transaction); validation extras; test files present. Populated mechanically (greps + reading upsert statements), reviewed by hand. This matrix decides every cohort below.

Also: add the missing Minnesota writer test (copy the sibling pattern).

Deliberately NOT doing: auto-generated characterization suites for every module upfront. Characterization tests are written per cohort, right before that cohort migrates.

### Phase 1 — writers, exact-canonical-shape cohort first

Factory prep — items 1–3 **shipped in PR #488**, item 4 **shipped in the second-wave prep PR**:
1. ~~`minElectionYear` becomes a required config field~~ — done; Texas/Houston pass 2014 explicitly.
2. ~~Per-field summary update policy (`replace` | `preserveWhenNull`)~~ — done (default = old COALESCE-all). The signed-allowed field set was **deliberately deferred** to Phase 5 — no Phase-1 state needs it (signed cash = IL/KY/LA, all in deferred cohorts).
3. ~~Outside-group validation as opt-in config~~ — done (`none` | `presence` | `pairing`, default none).
4. ~~`normalizeCommitteeId` config fn + `supersededLinkSource` same-source supersession~~ — shipped in the second-wave prep PR (Maine/Maryland prereqs; supersession was a first-pass matrix miss, now recorded there).
5. Still open — transaction rule from the matrix: Pool-guard states keep the guard; caller-transaction (`self`-tx) states get a `transactionMode` option if verification shows they rely on it. (Code check 2026-08-01: maine/maryland writers are Pool-guard — they reject bare queryables; the wrapper reproduces that with the Arizona-style stub, no factory option needed for them.)
6. ~~Configurable outside-group identity columns~~ — done (`outsideGroupIdentityColumns: {id?, name?}`, defaults `committee_id`/`committee_name`, identifier-validated because interpolated into SQL; input fields stay `committeeId`/`committeeName`, wrappers map state field names). Code check 2026-08-02: this alone unblocks only **oregon** — DC and alaska also rename the *link* identity column (`committee_key`; `candidate_filer_id`+`candidate_filer_name`), and massachusetts additionally has an extra link column (`filer_name`, 13-param link upsert).
7. ~~Configurable link identity columns~~ — done (`linkIdentityColumns: {id?, name?}`, defaults `committee_id`/`committee_name`, identifier-validated; the id column joins the link upsert's conflict target; supersession UPDATE unaffected — it keys on candidate/election/source). Unblocks **districtOfColumbia** (id → `committee_key`) and **alaska** (id+name → `candidate_filer_id`/`candidate_filer_name`); massachusetts stays blocked by its extra `filer_name` column.

Migration: pilot = **arizona** — done (PR #492). Second wave: **maine, maryland** — done (PRs #499, #508). Third wave: **oregon** — done (PR #510: sponsor identity via `outsideGroupIdentityColumns`; wrapper maps `sponsorId`/`sponsorName` input fields, pre-validates the link for the pinned "Oregon ORESTAR committee ID" noun, keeps `presence` validation — the cascade-FK stale-group window predates the migration, same standing decision as Texas/Houston; per-state tests fully untouched, zero relaxations). Fourth wave: **districtOfColumbia** — done (PR #512: label "D.C.", `committee_key` via both identity params, COALESCE-default policy, pairing; the bespoke writer normalized outside keys only, so the wrapper normalizes in its field mapping rather than via `normalizeCommitteeId`, and it preserves the legacy quirk where an empty-but-present groups list with breakdowns throws the pairing message; per-state tests untouched). **alaska DROPPED** (source audit 2026-08-02, Michigan pattern): its writer deletes the summary row when absent and stale-deletes omitted sections unconditionally — one-state full-replace semantics (grep-verified across all writers) that stay out of the factory per the working rules; alaska keeps its bespoke writer. **Phase 1 is complete**: every state migratable under the working rules is migrated (arizona, maine, maryland, oregon, districtOfColumbia + the original texas/houston). Next phase: Phase 2. **Michigan dropped from the wave**: its `candidate_loan_total` summary column is one-state (grep-verified), and one-state capabilities stay out of the factory per the working rules — deferred to Phase 5 alongside its manual-link-protection need. Wrapper keeps all exported names/types; per-state tests untouched except one documented delta: the ME/MD tests pin the bespoke writers' exact multi-line COALESCE whitespace, which cannot coexist with the factory's single-line clauses (pinned by the Texas/Houston/factory tests) — those whitespace-only assertions get relaxed to whitespace-insensitive checks with the semantic assertion kept.

Explicitly deferred to Phase 5: michigan (see above), alaska (one-state delete-when-absent snapshot semantics — likely stays bespoke permanently), colorado (direct-only), california, illinois, hawaii, vermont, wisconsin, and any other matrix row with extra columns/tables.

### Phase 2 — shared batchSync due-list query builder

The most uniform, highest-fix-traffic slice of batchSync: the due-list query (link table join, staleness ordering, paging, count). Extract a query builder parameterized by a link-identity descriptor (column list + row mapper). States keep their own orchestration loops for now — the loop is where office filters, historical-year gates, embedded auto-link (Vermont), artifact loading, caches, `force`, and dry-run flags live, and those are not uniform.

**Builder shipped** (`createStandardStateFinanceDueListQuery` in `finance/standardStateFinanceDueListQuery.ts`, own PR per the working rules): config `{state, tables: {links, summaries}, eligibleOfficeKeys, linkColumns?, mapRow?}`. Defaults produce the canonical query (`committee_id`/`committee_name`, mapped to `StandardStateFinanceDueRow`) — verified byte-identical to the bespoke Texas SQL. `linkColumns` (identifier-validated because interpolated into SQL) swaps/extends the link columns selected between `district` and `source_url` and requires a `mapRow`; `state` must match `^[A-Z]{2}$`. Cohort data lives in the matrix's "BatchSync due-list queries" section (full normalized diff of all 33 templates, 2026-08-02): **8 states byte-identical to canonical** (indiana, maine, maryland, minnesota, nebraska, newMexico, oklahoma, texas), **12 more column-list-only** (alaska, districtOfColumbia, hawaii, illinois, louisiana, massachusetts, newYork, tennessee, utah, vermont, virginia, wisconsin), the rest carry real query deltas (arizona, michigan, oregon, kentucky, washington, pennsylvania, florida, california, colorado, connecticut, newJersey) and migrate only if their delta lands behind config in its own PR. Migration order: canonical cohort first (pilot = texas, already a factory state), then the column-list cohort; per-state batchSync tests stay untouched.

**Canonical cohort migrated** (all 8 in one PR — the transformation is a uniform mechanical swap, verified per state): each `listDue<State>CandidateFinanceSyncRows` is now the builder under canonical config, the `<State>CandidateFinanceDueRow` types alias `StandardStateFinanceDueRow`, and the private due-query row types + `parseTotalDueRows` + `mapDueRow` helpers are deleted (−155 lines per state). Parity proof per state: the new function's emitted SQL is **byte-identical** to the pre-migration template extracted from main, parameters and row mapping equal, on recording mocks. Per-state batchSync tests unmodified and green. Remaining: the 12-state column-list cohort (needs `linkColumns` + per-state `mapRow`), then real-delta states only if their deltas land behind config.

### Phase 3 — loaders, after characterization tests

Descriptor redesign first: the current single committee-column option is insufficient — verified Washington uses `link.committee_id` but `outside_group.sponsor_id`/`sponsor_name` in the same file. Per-relation descriptor:
- link identity column; outside-group identity + name columns; breakdown identity column (`committee_id`/`committee_key`/`sponsor_id`/`filer_id` variants all exist);
- optional feature flags for which query families a state has (direct breakdowns, outside groups, industry rollups).

Keep it a descriptor, not a query DSL. If a state needs genuinely different SQL (New York's classified-industry handling), it stays unmigrated rather than growing the descriptor.

Cohorts: ~26 exact-name non-city loaders remain (31 modules − 3 migrated − 2 cities). Each cohort PR: characterization test capturing current loader output on fixtures **before** the swap (existing coverage is 10 of 31). Differently-named loaders (alaska, arizona, pennsylvania) and Florida (has no lookup loader file) are a final, shape-verified cohort — include or formally drop after inspection.

### Phase 4 — auto-link primitives

Not one `createStandardCandidateFinanceAutoLink`. Verified non-uniformity: Vermont's auto-link lives inside its batchSync with statewide-only safety logic; Michigan's auto-link file only lists candidates (no resolve/write). Extract two composable pieces:
1. Missing-link query builder (shares the identity descriptor from Phase 2).
2. Failure-isolated loop runner (attempted/linked/failed counters, per-item error capture).

Candidate mapping, resolver invocation, link writing, and caps stay per-state hooks. Wrapper-equivalent states (the 24 ≥85% group) collapse to config + resolver; Vermont/Michigan-style modules just reuse the primitives where they fit.

### Phase 5 — remaining writer schema families

Extend the factory capability-by-capability, driven by the matrix: optional-tables support (Colorado direct-only), extra summary/link columns (California, Illinois, Hawaii, Wisconsin, Vermont). Each capability lands in its own factory PR, then its cohort migrates. If a capability would serve exactly one state, the state keeps its bespoke writer — a fork with one user is cheaper than a meta-framework.

### Phase 6 — full batch orchestration runner (conditional)

Only after Phases 2+4 prove out, and only for states whose loops are by-then-thin wrappers around the shared primitives. If the loop bodies stay heterogeneous, stop here — due-list + primitives already capture the shared fix surface.

### Phase 7 (optional / defer)

- `*CandidateFinanceSync` (78%): extract only the common tail if earlier phases make it obvious.
- `*FinanceEligibleOffices`: shared type scaffolding only.
- Scripts layer (~183 finance files, 40 per-state sync CLIs): separate effort.

### Non-goals

- Resolvers, aggregators, portal clients/parsers/data sources — real state-specific logic.
- NYC and LA City writers/loaders.
- Silent behavior harmonization. Every behavior change (validation adoption, merge-policy change) is its own reviewed, per-state decision.

## Order and risk

| Phase | Scope | Risk |
|---|---|---|
| 0 | matrix + MN test | none |
| 1 | factory prep + exact-shape writers (pilot → cohorts) | low for exact-shape cohort; medium overall |
| 2 | due-list query builder | medium |
| 3 | loader descriptor + cohorts w/ characterization tests | medium |
| 4 | auto-link primitives | medium-high |
| 5 | remaining writer families | medium |
| 6 | full batch runner | high — do only if justified |

Line-deletion estimate (~40k) is provisional and NOT the success criterion. Success = semantic parity proven per cohort + shared fix surface for the four chokepoints.

## Working rules

- Every migration PR: wrapper keeps exported names/types; per-state tests unmodified (plus new characterization tests where coverage was missing); `npm run typecheck` + `npm test` green; the state's matrix row cited in the PR body with any deltas listed.
- Factory/descriptor changes land in their own PR before any state migrates onto them.
- A state's extra feature is either promoted into the factory behind config (default = current shared behavior) or the state stays unmigrated. No silent losses, no one-state meta-features.
