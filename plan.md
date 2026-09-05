# Finance module consolidation plan (v3)

Date: 2026-08-01 (v2), amended 2026-09-04 (v3 addendum at the end). Scope: `backend/src/pipeline/*Finance` (34 modules / ~140k lines at v2; 57 dirs / 227k lines at v3) plus the shared `backend/src/pipeline/finance/` module, and — since v3 — the finance schedulers, scripts and flags. **Where the v3 addendum conflicts with an earlier section, v3 wins**; the earlier sections are kept as history and carry inline pointers where superseded. Every claim was verified against the code in this worktree at the time it was written.

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

**Column-list cohort pilot: districtOfColumbia migrated** (smallest delta — `committee_key` for `committee_id`; `linkColumns: ["committee_key", "committee_name"]` + a mapRow producing the state's `committeeKey` field; due-row type kept literal since its field names differ from the canonical row). Same parity proof as the canonical cohort: emitted SQL byte-identical to the pre-migration template from main, params + mapping equal; per-state tests untouched. Remaining 11 column-list states follow in ~2 wave PRs: alaska, hawaii, illinois, louisiana, massachusetts, newYork, tennessee, utah, vermont, virginia, wisconsin.

**Column-list wave 1 migrated** (hawaii, virginia, wisconsin, utah, louisiana, newYork — the extras-only and simple-rename states): each is builder config + a state `mapRow`; per-state quirks preserved verbatim in the mapper — hawaii runs `normalizeHawaiiCscDistrict` on the district, virginia runs its trim-to-null `normalizeDistrict` (kept as a private fn) and types `linkSource` as `VirginiaFinanceLinkSource`; louisiana's office-keys constant is a Set (the only cohort state), spread once in the config. Same per-state parity proof (SQL byte-identical to main, params + mapping equal, fixtures exercising both district normalizers); per-state tests untouched. Remaining wave 2: alaska, illinois, massachusetts, tennessee, vermont (multi-column swaps).

**Column-list wave 2 migrated — the cohort is closed.** alaska (`candidate_filer_id`/`candidate_filer_name` + typed `link_source`), illinois (four nullable SBE columns + `committee_key`), massachusetts (`candidate_cpf_id`/`filer_name`; mapper keeps `normalizeMassachusettsOcpfDistrict`), tennessee (`camp_candidate_id`/`owner_name` + nullable `committee_name` + typed `link_source` + `report_list_url`; mapper keeps its trim-to-null `normalizeDistrict`), vermont (`filer_registration_guid` + numeric `entity_id` + `filer_name`). One documented non-byte delta, the only one in the whole Phase-2 migration: tennessee's bespoke query selected `report_list_url` after `source_url`; the builder emits it in the link-column slot before it — a pure column-order move, rows are read by name. Parity proof: 4 of 5 byte-identical to main; tennessee byte-identical after relocating exactly those two lines in the old SQL. **Phase 2 status: all 20 builder-eligible states migrated** (8 canonical + 12 column-list). The 11 real-delta states keep bespoke queries unless a delta lands behind config (own PR); orchestration extraction is Phase 6's question.

### Phase 3 — loaders, after characterization tests

Descriptor redesign first: the current single committee-column option is insufficient — verified Washington uses `link.committee_id` but `outside_group.sponsor_id`/`sponsor_name` in the same file. Per-relation descriptor:
- link identity column; outside-group identity + name columns; breakdown identity column (`committee_id`/`committee_key`/`sponsor_id`/`filer_id` variants all exist);
- optional feature flags for which query families a state has (direct breakdowns, outside groups, industry rollups).

Keep it a descriptor, not a query DSL. If a state needs genuinely different SQL (New York's classified-industry handling), it stays unmigrated rather than growing the descriptor.

Cohorts: ~22 non-city loaders remain after cohort 0 (31 modules − 7 migrated − 2 cities). Each cohort PR: characterization test capturing current loader output on fixtures **before** the swap (pre-campaign coverage was 10 of 31). Differently-named loaders (alaska, pennsylvania — arizona migrated in cohort 0) and Florida (has no lookup loader file) are a final, shape-verified cohort — include or formally drop after inspection.

**Cohort 0 migrated (arizona, hawaii, maine, maryland)** — one deliberate ordering deviation: these four needed **zero** factory changes (normalized-SQL cluster, 2026-08-02, full readout in the matrix's "Query-shape families"), so they migrated *before* the descriptor redesign — a smaller first step that also validated the characterization recipe. Their only query-shape delta from the shared loader is the donor-only evidence variant, which existing `evidenceLabelTypes: ["donor"]` covers (textual SQL difference, equal output via the mapper's `?? "donor"` default; illinois set the precedent); maine/maryland's office filters ride the existing `isEligibleElection` option. The pin-then-swap recipe is validated on this cohort and is the template for every later one: shared harness `tests/helpers/stateFinanceLoaderCharacterization.ts` pins output map + query order/tables + request params + migration-column cross-check (no SQL text assertions) against the bespoke loader first, then the file becomes a texas-style wrapper and the pinned tests must stay green unchanged. Later cohorts still need their own descriptor/query-family work first. ~−1,860 lines.

**Descriptor shipped** (own PR per the working rules, no state migrates in it): `linkIdentityColumn?: string` (summary query only, incl. all six illinoisD2 aggregate guards) and `outsideGroupIdentityColumns?: {id?, name?}` (all three outside queries — group, industry, donor evidence — including the COALESCE name fallback, the pairing join, and the evidence ORDER BY tiebreak). One pair covers both outside relations because no state names groups and group-breakdowns differently (cluster-verified). The ID defaults chain through `committeeColumn` (the name default is always `committee_name`), so existing configs are untouched — refactor verified byte-identical emitted SQL under both live config shapes (canonical texas and illinois committee_key+D2+donor). New columns are identifier-validated (interpolated into SQL); emitted rows keep the canonical `committee_id`/`committee_name` aliases, so row mapping never changes (matches how the bespoke sponsor states alias too). Naming mirrors the writer factory's params.

**Descriptor cohort pilot: washington migrated** (506→42-line wrapper; first consumer of `outsideGroupIdentityColumns` — `{id: "sponsor_id", name: "sponsor_name"}`, everything else shared defaults: both evidence label types, no office filter, canonical link identity). Same pin-then-swap recipe: characterization spec green against the bespoke loader first, unchanged after the swap; its bespoke sponsor SQL lines match the descriptor's emissions site-for-site (the descriptor tests were written from washington's shape).

**Descriptor wave 2 migrated: wisconsin, alaska, massachusetts** (~510/643/509 → ~45-line wrappers each). wisconsin = `sponsor_*` + donor-only (its bespoke summary SQL comment dropped — text-only); alaska = `linkIdentityColumn: "candidate_filer_id"` + `outside_group_id`/`outside_group_name` + donor-only, first consumer of `linkIdentityColumn`; massachusetts = `candidate_cpf_id` + `iepac_cpf_id`/`iepac_name` + donor-only. All three keep office filters via `isEligibleElection`. One accepted no-op delta, documented in the alaska wrapper: alaska's local helper copies treated `""` as null (shared: 0) and truncated counts (shared: rounds) — unobservable because amounts are `numeric(16,2) NOT NULL` and `contributor_count` is `integer`, so Postgres never yields `""` or fractional count strings. Same pin-then-swap recipe, specs green before and unchanged after.

**Direct-breakdown filter option shipped** (own PR, no state migrates): `directBreakdownCategoryTypes?` narrows the direct-breakdown query's `category_type IN (…)` list, default `occupation`+`contribution_size` (byte-identical default SQL, stash-verified under both live config shapes). Whitelisted to the two types the mapper can *route* — accepting e.g. `industry` would silently land those rows in `top_occupations`, so unroutable types are rejected; newYork (and the CA/CO/CT/NE/NM family) needs an industry→`direct_campaign.top_industries` routing capability first and stays deferred. Unblocks **louisiana + vermont** (`= 'contribution_size'` states, verified: their mappers fill only the buckets and emit literal `[]` for occupations, byte-matching the standard mapper's empty-map output; bucket source_url fallback identical).

**Louisiana DROPPED from the loader cohort** (source audit at pin time): its loader wraps the five reads in a `BEGIN READ ONLY` transaction when handed a Pool (`withConsistentLouisianaFinanceRead`, pinned by its own test) — grep-verified one-state, and one-state capabilities stay out of the shared loader per the working rules. Louisiana keeps its bespoke loader; if a consistent-read default for the *shared* loader is ever wanted, that is a separate all-states factory decision, not a migration blocker to paper over.

**Support-action wording option shipped + vermont pinned** (same PR, no state migrates): the characterization pin caught that louisiana AND vermont pass `"PAC contributions supporting this candidate"` as `buildOutsideIndustrySupportExplanation`'s action clause — two states, so it earned `outsideSupportActionLabel?` (default = the shared "independent spending…" wording; JS-only, no SQL change). The harness gained matching knobs (`directCategoryTypes` — fixtures must mimic what the state's SQL returns, since the mock bypasses SQL; `outsideSupportActionLabel`), and vermont's characterization spec is pinned green against its bespoke loader (`contribution_size`-only fixtures + PAC wording). Vermont's helper copies share alaska's `Math.trunc` count edge — same unobservable-delta reasoning (`contributor_count integer`).

**Vermont migrated** (644 → ~59-line wrapper): first state to combine both descriptor halves — `linkIdentityColumn: "filer_registration_guid"` AND `outsideGroupIdentityColumns: {id: "filer_registration_guid", name: "filer_name"}` (CFD keys every relation by filer registration GUID) — plus donor-only evidence, `directBreakdownCategoryTypes: ["contribution_size"]`, `outsideSupportActionLabel` (PAC wording), and `isEligibleElection`. Exported `VermontBallotLookup*Row` types kept as aliases of the shared request rows (arizona precedent). Pin unchanged and green after the swap; equal-output deltas absorbed by config: bespoke hardcoded `organization_type: "donor"` ↔ shared reads `category_type` (donor-only rows), bespoke literal `[]` occupations ↔ shared's empty map under the narrowed filter, `total_raised: direct_contribution_total ?? total_receipts` = the totals-variant mapper. One test-text delta: `ballotLookup.test.ts`'s Vermont detail spec asserted the bespoke SQL fragment `category_type = 'contribution_size'`; the shared loader emits the equivalent `IN ('contribution_size')`, so that one expected string changed (the harness contract deliberately lets SQL reword).

**DistrictOfColumbia migrated** (506 → ~49-line wrapper): the cheapest swap of the campaign — exactly one non-default option, `committeeColumn: "committee_key"` (verified at migration as planned: OCF keys the link and both outside relations by `committee_key`, so the one-column option covers everything; illinois precedent). No office filter, both evidence label types, default wording, and DC already imported the shared helpers, so there were no local copies to diff. Coverage gap filled pin-first: DC had **no** ballot-lookup loader test at all; the characterization spec + flag-gate test were written and run green against the bespoke loader before the swap, then kept unchanged. **One accepted no-op delta** (alaska precedent, documented in the wrapper): the bespoke mapper hardcoded `organization_type: "donor"` while the shared mapper reads the row's `category_type`. Unreachable — DC outside breakdowns are constrained to `('donor', 'industry')` by `dc_cff_outside_breakdowns_type_check` (migration 120) and the shared snapshot writer's outside category type is likewise `"donor" | "industry"`, so with the evidence filter admitting only donor/employer, every evidence row is `donor`. `evidenceLabelTypes` deliberately stays at the default: the bespoke SQL also selected `IN ('donor', 'employer')`, and narrowing it would change emitted SQL for no output change while splitting DC from texas/houston/washington, which carry the identical constraint and also run the default. **Review lesson: check the schema CHECK before calling a mapper difference a bug fix** — the first draft of this migration claimed employer evidence had been mislabeled, which the constraint makes impossible.

Remaining loader cohorts: newYork (industry routing), tennessee (extra `expenditure_count` + trimmed summary), oregon/pennsylvania (sum-vs-max outside totals), michigan (`candidate_loan_total`), newJersey (`::text` casts), then the family-subset states (minnesota / 4-query / 2-query / utah) if they earn their config; kentucky (dedup CTE), cities, florida (no file), and now louisiana (read transaction) stay out or get formally dropped.

### ⏸ Pause point — add new states (and cities) here, after Phase 3

**The best time to add new states is after Phase 3 lands** (writer factory + due-list builder + shared loader all exist). At that point a new state is mostly config: canonical schema, a thin factory writer wrapper, a due-list builder config, a loader descriptor, plus the genuinely bespoke parts (resolver, portal client/parser, eligible offices). Do not wait for Phases 4–6: auto-link is cheap to copy from a sibling, and Phases 5–6 only concern old states' odd schemas and orchestration — they never block a canonically-built new state.

Rules for any new state, whenever it's added:

- **Canonical schema first** — 5 standard tables, `committee_id`/`committee_name` identity, standard summary columns, no extra link columns unless the source data truly cannot fit. Schema drift is what created the 34-way mess this plan is unwinding.
- Writer = factory wrapper from day one (arizona/oregon pattern); due-list = builder config from day one. Zero migration debt.
- If added **before** Phase 3: copy the loader and auto-link from a migrated sibling (texas/illinois pattern) and write the loader characterization test up front, so the state sweeps into Phase 3/4 cohorts cheaply instead of joining the untested backlog.
- **Reuse gate (v3, 2026-09-04)**: reuse an existing shared component when its contract fits; prefer a small adapter for local differences; extend the shared component when a concrete cohort benefits without complicating existing callers; otherwise keep the bespoke implementation and say why in the file header — naming the delta AND why the shared piece was not extended. Two consumers are evidence, not a license to generalize; one local difference is not a reason to keep a whole copied file.
- A product deadline may pull a state in earlier than this pause point — that's fine under the rules above. Never add a state as a bespoke 4–5k-line copy.

Ohio (the first pause-point state, `ohio_plan.md`, PRs 1–8 + live run, 2026-08) hardened these into rules — each one bought with a real failure or a real discovery:

- **Plan doc before code.** Write `<state>_plan.md` (ohio_plan.md is the model): feasibility findings, numbered settled design decisions, and the PR ladder — then keep revising it as PRs land. Ohio's expensive calls (two-stage 31-U, no occupations, fail-closed matching) were all plan-phase decisions, not code-review saves.
- **Acquisition spike is its own gated PR, before any parser.** User-authorized portal access; its job is to discover the transport that actually works (Ohio: plain fetch AND automated Chrome were both Cloudflare-403 — only the user's real attended Chrome passed), pull a full real cycle, capture fixtures, and confirm or kill every data-model assumption. Two of Ohio's thirteen design decisions were revised and one feature (occupations) was killed by what the spike found.
- **Probe real bytes; pin what they show; fail closed on the rest.** Form-type vocabularies, headers, and reconciliation rules come from the downloaded files, never from portal docs. Anything outside the pinned set lands in a diagnostic counter — not in a bucket, not in a guess. Never trust the portal's own listings either: Ohio's file-transfer page publishes product labels, not file names, and its size column can be stale.
- **Cache with manifests; sync reads cache only.** Every artifact installs through validate → SHA-256 → atomic replace, with a manifest (byte size, row count, date range). Portal download IDs are discovered by exact label match each run, never hardcoded — and label matching must be exact and list-scoped (Ohio's NEW tab carries per-committee files whose labels differ from the statewide annual only by a committee name; a prefix match would install one candidate's file as the whole state's).
- **Wire the read flag end-to-end in the schema PR**: `backend/.env` AND `render.yaml`. Ohio shipped with neither, and all 189 synced summaries rendered nothing until it was noticed by hand.
- **Disclose known coverage gaps with the totals.** If some of the state's spenders disclose through a channel the pipeline doesn't read (Ohio: nonregistered spenders' PDF filings), set the loader's `outsideCoverageNote` so the card says so next to the dollar figures — and remove it when the gap closes.
- **The final PR is a live run through the committed acquisition script** — fresh portal pull, full sync, money-reconciliation + match-rate report. Not a hand-assembled cache: Ohio's label-discovery bug survived four merged PRs because the spike had downloaded files through a different path, and the script's own discovery step had never run end-to-end.

**Cities**: add whenever the product needs them; NYC/LA showed cities don't fit the standard shape (own tables, own summary fields), so a new city stays mostly custom regardless of refactor progress. Timing is product-driven, not refactor-driven.

### Phase 4 — auto-link primitives

Not one `createStandardCandidateFinanceAutoLink`. Verified non-uniformity: Vermont's auto-link lives inside its batchSync with statewide-only safety logic; Michigan's auto-link file only lists candidates (no resolve/write). Extract two composable pieces:
1. Missing-link query builder (shares the identity descriptor from Phase 2). *(v3: it needs no link-identity projection and lives in its own module; see the addendum.)*
2. Failure-isolated loop runner (attempted/linked/failed counters, per-item error capture). *(v3: conditional — extracted only if several wrappers become materially simpler.)*

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

- Every migration PR: wrapper keeps exported names/types; per-state tests unmodified (plus new characterization tests where coverage was missing); `npm run typecheck` + `npm test` green; the state's matrix row cited in the PR body with any deltas listed. *(v3 supersedes the test clause: behavioral assertions preserved, implementation-text changes listed and justified, DB-backed comparison for any changed SQL — see "Verification rule" in the addendum.)*
- Factory/descriptor changes land in their own PR before any state migrates onto them. *(v3: a small backward-compatible option may ship with its one pilot when the option has its own tests; substantial shared-behavior changes still get their own PR.)*
- A state's extra feature is either promoted into the factory behind config (default = current shared behavior) or the state stays unmigrated. No silent losses, no one-state meta-features.

---

## v3 addendum — 2026-09-04 re-audit after the pause point

Hands-on re-count of this worktree at `fc2deb359` (Read/Grep/Bash only), then a second review pass whose claims were re-verified line by line. Counts below name their file sets so they can be re-run.

### Inventory (reproducible)

- `backend/src/pipeline/*Finance/` = **57 dirs, 227k lines** — **47 state/DC modules, 9 city modules** (austin, denver, houston, losAngelesCity, newYorkCity, phoenix, sanDiegoCity, sanFrancisco, sanJose), and **`efileCalFinance`, a shared vendor client/parser used by sanJose + sanDiegoCity (5 imports each) — not a source**. Eight glue roles (Writer/Loader/DueList/BatchSync/AutoLink/Sync/EligibleOffices/index) = 96k lines; clients/parsers/resolvers/aggregators = 131k.
- `src/scripts/` matching `finance` = 271 files, 41k lines. `src/scheduler/` matching `finance` = 45 files = **41 `*CandidateFinanceSyncScheduler.ts` + 3 raw-data-refresh schedulers (CA/IN/PA) + the federal `candidateFinanceSyncScheduler.ts`**; none of the 2026-08/09 states except ohio/northCarolina/georgia/missouri/montana/southCarolina have a scheduler.
- Tests: 832 finance test files, 190k lines (62% of all backend test lines; 1,202 files / 305k total). 63 scheduler tests.
- `ballotLookup.ts` loader registry: **55 entries, 47 distinct state codes** (AZ/CA/CO/TX carry city entries too). `featureFlags.ts`: 879 lines; 56 `is<X>CampaignFinanceEnabled`, 53 `…SyncEnabled(force)`, 28 raw-refresh gates whose env keys use source prefixes (`OHIO_SOS_`, `MARYLAND_CFS_`, `COLORADO_TRACER_`…), so they are pairs plus assorted thirds, not 54 uniform trios.
- Sixteen states landed after the Ohio pause point (ohio, northCarolina, georgia, rhodeIsland, missouri, newHampshire, delaware, nevada, montana, arkansas, westVirginia, idaho, northDakota, kansas, alabama, southCarolina). Rows: matrix "Post-pause-point modules".

### Corrections to v2 text (the factories moved on; the prose did not)

- Writer factory now has `allowNegativeCashOnHand`, custom `directCategoryTypes`, same-identity `M` built in and `manualLinkProtection` (`M+`), `supersededLinkSource`, `normalizeCommitteeId`, both identity-column descriptors. v2's "the factory rejects negative amounts everywhere" and the matrix's "still missing: signed-amount fields" are stale — only caller-transaction mode and optional tables remain unbuilt.
- Identity examples in v2 "Evidence" are wrong on three states (verified at the writers' `ON CONFLICT`): hawaii's link key is `(candidate_id, election_id, committee_id)` with `election_period` an extra column; wisconsin's key is `(candidate_id, election_id, entity_id)` (`assigned_committee_id` is extra); vermont's key is `(candidate_id, election_id, filer_registration_guid)` (numeric `entity_id` is extra). Conflict keys, not extra columns, decide factory eligibility.
- Shared loader already selects and routes `industry` direct breakdowns (`SUPPORTED_DIRECT_CATEGORY_TYPES`; westVirginia/newHampshire/nevada use it), plus funding columns and coverage notes — v2/Phase 3's "industry routing must land first" is stale.
- Similarity percentages in v2 are **historical (2026-08-01)**: they prove duplication existed; they do not decide migration eligibility today.

### What held

- Every post-pause state is a writer-factory wrapper and (except kansas — no loader by design until statewide rosters exist) a shared-loader wrapper. Zero writer/loader debt for new states. Houston runs the factory; austin/denver reuse the shared loader + due-list builder — "cities stay custom" means "preserve different contracts", not a blanket exclusion.
- Frontend is source-agnostic. Adding a state touches 8 backend files outside its dir plus `.env`/`render.yaml`.

### What broke (verified file sets)

1. **Schedulers — never in the plan.** The 41 sync schedulers copy `assertPositiveInteger`, `readSchedulerRuntimeConfig`, `getQueueConnection`, `defaultJobOptions` (41× each). After replacing the state name and cron default, **washington, hawaii, wisconsin, virginia, massachusetts are identical (0 diff lines, 275 lines each)**; ohio differs in 123 lines, vermont in 164, georgia adds `maxPasses`. Recurring-job registration is gated on the **master** flag everywhere (WA/HI/WI/VA/MA/OH/GA/VT) — that is shared factory behavior; **southCarolina is the exception** (gates registration on `isSouthCarolinaCampaignFinanceSyncEnabled(force)`). Disabled-result shapes, reserved-job-id checks and payload fields also vary.
2. **Scripts — plan said "separate effort"; never started.** `readValueFlag` copied 42×, `parsePositiveInteger` 164×. Behavior is NOT uniform: `syncDueTexasCandidateFinance` accepts `9007199254740993` and silently rounds (regex only), `syncDueWestVirginiaCandidateFinance` rejects it (`Number.isSafeInteger`); Texas due rejects a duplicated flag, Texas trigger takes the first value. `utils/cliFlags.ts` (`readPositiveIntegerFlag`, 18 users), `financeCliFlagGuard.ts` and `missouriCandidateFinanceCli.ts` already exist as partial shared pieces.
3. **Auto-link (Phase 4) never started.** 50 files, **all** contain a missing-links query (the 6 city files + losAngeles write it in compact form my first clause regex missed). Normalized WHERE/JOIN/ORDER clauses cluster into two big families (23–24 and 11 files, depending on which clauses are compared) plus singletons. Membership and the normalization script get published in the Phase 4 builder PR, not assumed here.
4. **BatchSync.** 54 files; 53 share the outer `for (const row of due.rows)` loop, but the loop *bodies* differ materially: montana runs per-year outside sweeps with partial progress, idaho shares whole-dataset reads and stores a run artifact, newHampshire memoizes API calls, re-resolves candidate spellings and treats "nothing written" as failure. Identical `for` loops are weak evidence for identical orchestration.
5. **Due list — the reuse rule was skipped 7×.** missouri, newHampshire, delaware, montana, alabama, idaho, southCarolina each carry a bespoke due list whose normalized WHERE/JOIN/ORDER clauses are **identical** (0 differing lines vs montana). Deltas vs the builder: `AND election.election_stage = 'general'` (all 7); `election.election_date::text AS election_date` (6 — alabama instead selects `election.official_ballot_title AS ballot_title`); state link columns via the existing `linkColumns` mechanism; per-state mappers (alabama's parses the stored committee id via `parseStoredInternalCommitteeId`; newHampshire's emits a `candidateNames[]` list of spellings that `chooseSyncCandidateName` tries in order and verifies against the linked filer — required, test-covered).
6. **featureFlags.ts** — pairs are uniform (`enabled` + `syncEnabled(force)` with `force` never bypassing the master flag); the third gate is not.
7. `type Queryable` copied 281×; `ConnectableQueryable` 98× in **three non-equivalent shapes** (`{connect: () => Promise<PoolClient>}`, `Pick<Pool, "connect">`, optional `connect?`).
8. Sync bodies (53 files) are per-source; only one operation recurs verbatim: `enrichOutsideGroupIndustryBreakdowns` (17 copies — classify all donors → rebuild industries → cap display rows). The capped-donor bug (PRs #548/#550) lived in exactly that copy set. Texas and Maryland differ in duplicate normalization, `minIndustryAmount`, and null `contributorCount` merging — pin before extracting.

### Phases (v3) — recommended order: 2b → 8 pilot → 4 builder → 9 → 10 (independent, small)

Each substantial shared-behavior change is its own PR. A small backward-compatible option and its one pilot may ship together when the option has its own tests.

#### Phase 2b — general-stage due-list cohort (6 states now; newHampshire stays bespoke)

Builder additions, defaults unchanged (existing configs emit byte-identical SQL): `electionStage?: "general"` (validated literal interpolated like `state`, matching the cluster's SQL; no new bind parameter), and an explicit, closed projection set — `selectElectionDate?: boolean`, `selectBallotTitle?: boolean`. No open-ended "any election column" API. Cohort: **montana — DONE** (pilot: `electionStage` + `selectElectionDate` landed with it; `selectBallotTitle` deferred to alabama's PR; DB parity on 9 inputs incl. the 240-row full set and the lookahead boundary; two implementation-text pins in its test reworded) → **missouri, delaware — DONE** (same shape; MO DB parity on 9 inputs over the 161-row live set; DE has no local links, so its parity rests on its legacy SQL being MO's with tables/state substituted plus the empty-set run) → **idaho, southCarolina, alabama — DONE** (2026-09-05; `selectBallotTitle` added to the builder with alabama, placed between `office_name` and `district` like its legacy projection; DB parity on 9 inputs each over the 238/145/213-row live sets, including the stale-7 → 0 and stale-1 → all cases). **Phase 2b complete.** newHampshire keeps its file: its extra candidate-name projection is one-state and load-bearing. Parity = run old and new query functions against the local Postgres on the live link rows and compare row sets + `totalDueRows` (the real-data recipe from Phase 3), plus params/mapping on recording mocks; whitespace-normalized text alone is not accepted (optional `AS`, aliases, implicit `ASC`, `SELECT *` all differ).

#### Phase 8 — scheduler factory (pilot cohort = the five identical files)

`createStateFinanceSyncScheduler(config)` reproducing exactly washington/hawaii/wisconsin/virginia/massachusetts (each has 6 behavioral tests; ohio has 1 that only checks unsafe integers). Config: names, env prefix, cron default, flag functions, `syncDue`; state exports keep their names/types; queue names, scheduler ids, payloads, gating and `queue.close()` cleanup preserved explicitly. Exceptional behavior (southCarolina sync-gated registration, georgia `maxPasses`, vermont) stays in state wrappers/callbacks rather than factory options; those states migrate only if a wrapper stays thin. Line savings are an estimate until the pilot lands. **Pilot cohort — DONE** (2026-09-05): `createStateCandidateFinanceSyncScheduler` in `scheduler/stateCandidateFinanceSyncScheduler.ts`, generic over the batch item type; config = `stateLabel`, `jobName`, `dailySchedulerId`, `defaultQueueName`, `linkedElectionJobIdPrefix`, `envPrefix`, `defaultDailyCron`, `isEnabled`, `isSyncEnabled`, `syncDue`. All five states migrated; each keeps its 8 exports (2 constants, 6 functions) and 3 type aliases, their 6-test files unchanged. Parity = the 30 state tests + an export-surface comparison against origin/main (names, kinds, constant values, job ids). 275 → 66 lines per state.

#### Phase 4 — missing-links query builder; loop runner conditional

Builder for the missing-links query only (candidate elections + `NOT EXISTS` against the links table — it needs no link-identity projection, and it deliberately uses different name/office/district/date expressions from the due list, so it does **not** share a module with the due-list builder; share only demonstrated predicates such as the stage filter and office-key match). Wave 1 = the largest normalized family; the PR publishes the family membership and the normalization script. Extract a failure-isolated loop runner only if several wrappers become materially simpler; preserve per-state statuses, injected dependencies, registry caching, caps and error reporting. **Builder + wave 1 — DONE** (2026-09-05): `createStandardStateFinanceMissingLinksQuery({ state, linksTable, eligibleOfficeKeys })` in `finance/standardStateFinanceMissingLinksQuery.ts`; input `maxCandidates?` binds NULL (LIMIT ALL) when omitted, which pennsylvania relies on and which is runtime-identical for the rest (pg sends undefined as NULL). Family membership, found by normalizing each file's `NOT EXISTS` template literal (collapse whitespace, drop `AS`, mask the links table and state code) and hashing: **16 files** — alaska, florida, georgia, hawaii, indiana, louisiana, maine, maryland, minnesota, newMexico, newYork, oklahoma, pennsylvania, texas, virginia, wisconsin — all migrated; each keeps its function name and row-type name (now an alias). Remaining clusters are pairs (california/colorado, northCarolina/ohio, northDakota/westVirginia) and singletons; migrate only if a later diff shows a one-option delta. Loop runner NOT extracted: the 16 wrappers did not get materially simpler in their loop bodies (resolver inputs, link writers, caps and statuses all differ), so the conditional was not met. Parity = DB run of old vs new list functions on 8 inputs per state (full set, LIMIT 5, no cap, lookahead 58/59, lookback 0/1 on Nov 4, far-past window) plus a byte-for-byte pin of the Texas text.

#### Phase 9 — shared CLI primitives (by behavior cohort, not a blind sweep)

Extend `utils/cliFlags.ts` / add `scripts/financeCliArgs.ts`: strict value reading (duplicates rejected), safe-integer parsing (unsafe rejected). Adopting them where a script today silently rounds or takes the first duplicate is an explicit behavior fix, listed per script in the PR — no compatibility switches to preserve the unsafe paths. Option assembly stays local per script. **Primitives + wave 1 — DONE** (2026-09-05): `readStrictFlagValue(argv, flag): string | null` and `readStrictPositiveIntegerFlag(argv, flag): number | undefined` in `utils/cliFlags.ts` (same signatures and error strings as the copied helpers — `Missing X value`, `Provide X at most once`, `Invalid X value: raw` — so call sites swap by name). Cohorts, found by hashing each script's whitespace-normalized helper bodies: the strict `parseFlagValue` (84 files, plus an equivalent `!value` spelling in 4) pairs with either the regex-only `parsePositiveIntegerFlag` (72 files — silently rounds above 2^53) or the regex + `isSafeInteger` one (13). Wave 1 = the 27 `syncDue*` scripts in that pairing whose only other helper is their own `parse…ScriptArgs`; 22 gained the safe-integer rejection (behavior fix, each existing test pins `9007199254740993`), 5 (maine, maryland, newYorkCity, northCarolina, ohio) were already safe. **Wave 2 — DONE** (2026-09-05): the 40 pure `trigger*`/`upsert*` siblings (same pairing, same "only helper is its own `parse…Args`" rule; includes the indiana raw-data-refresh trigger/upsert pair, whose integer flags are `--year`/`--timeout-ms`); 34 gained the safe-integer rejection (29 pinned in existing tests; louisiana ×2, newYork ×2 and the oregon trigger have no script test), 6 (maine, northCarolina, ohio trigger/upsert) were already safe. **Wave 3 — DONE** (2026-09-05): the first-duplicate-wins `parseFlagValue` (28 files — the colorado, connecticut, houston, maryland, michigan, newMexico, pennsylvania, texas trigger/upsert pairs, syncDueConnecticut, and the raw-refresh trigger/upsert scripts for california, colorado, connecticut, newMexico, pennsylvania, texas). Two behavior fixes per script: a repeated value flag now throws `Provide X at most once` (it used to take the first inline form, else the first space form), and digit-only values above 2^53 are rejected (all 28 were regex-only; the `trim`/empty-check variants in 13 of them were unreachable because the reader already trims and throws on empty). Both pinned in the 26 scripts with a test file; the houston pair has none. Per-script enum/artifact-kind helpers (connecticut eCRIS, newMexico CFIS) stay local and call the shared reader. Remaining: the probe family (fallback-argument signatures) and the raw-refresh `readValueFlags` family need their own primitive shapes and are not promised.

#### Phase 10 — flag helpers (small)

`defineStateFinanceFlagPair(prefix)` for the uniform enabled/sync pair (reads env at call time, `force` never bypasses the master flag, short-circuit + invalid-value errors kept); third gates take their existing explicit env key. No unused raw-refresh functions are generated.

#### Deferred / not promised

- Phase 6 full batch runner: stays deferred; Phase 4 alone is not justification (see finding 4).
- Alias sweep (`Queryable`): opportunistic when touching a file; no standalone PR.
- Writer Phase 5: reassess small cohorts against today's factory before proposing options; bespoke writers whose persistence semantics differ (alaska delete-when-absent, michigan loan column) stay. No general extra-column framework.
- Phase 3 loaders: reopen with current capabilities. newYork is NOT yet a safe swap: its direct query caps every category at 5 rows including `contribution_size`; the shared loader exempts size buckets (`rn <= 5 OR category_type = 'contribution_size'`), so newYork/louisiana can drop the 6th bucket today. louisiana's `BEGIN READ ONLY` wrapper can own the transaction and hand the client to the shared loader (Read Committed gives no cross-query snapshot anyway — preserve, don't reinterpret).
- Narrow extraction of `enrichOutsideGroupIndustryBreakdowns` after pinning the Texas/Maryland differences.

### Known-defect follow-ups (so "preserve behavior" does not mean "keep bugs")

- Texas/Houston writers run `outsideGroupValidation` "none" with the cascade-FK stale-group window (Phase 1 note).
- newYork/louisiana loaders cap `contribution_size` at 5 (6-bucket scheme) — decide fix or accept, per state.

### Verification rule (replaces "per-state tests untouched")

Behavioral assertions are preserved; implementation-text changes (SQL wording, error nouns) are listed and justified in the PR. The characterization harness is mock-based (it pins mapped fixtures, query order, params, referenced columns — it never executes SQL), so any changed SQL shape also gets a database-backed old-vs-new comparison on local Postgres. Per cohort add focused cases for: missing/null/zero/empty collections; multiple committees; all six size buckets; window boundaries, stage filter, ordering, limit; manual-link protection and rollback; scheduler gates, payloads, cleanup. No combinatorial suite over every config.

### New-state rules — edits

"Sync reads cache only" applies to artifact-based sources (newHampshire and idaho have intentional live-API paths). "Five standard tables" = preferred schema, never a reason to fabricate or misrepresent missing coverage. Do not delay a new state for unfinished consolidation phases when existing components already fit. Kansas (no loader) and rhodeIsland/nevada (import-style, no batchSync/due list) are deliberate.
