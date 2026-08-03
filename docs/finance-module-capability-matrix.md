# Finance module capability matrix

Phase 0 deliverable of the finance consolidation plan (see `plan.md`). Extracted mechanically from the writers, loaders, and test tree on 2026-08-01; spot-verified by hand. This matrix decides migration cohorts — cite the relevant row in every migration PR.

## Legend

- **Tables**: which of the five canonical tables (`links`, `summaries`, `direct_breakdowns`, `outside_groups`, `outside_group_breakdowns`) the writer touches. `5` = all five.
- **Link extras**: link-table columns beyond the canonical set (`candidate_id`, `election_id`, `election_year`, `candidate_name_normalized`, `office_name`, `district`, `committee_id`, `committee_name`, `link_status`, `link_source`, `source_url`, `last_verified_at`). `→` marks a rename of `committee_id`/`committee_name`.
- **Merge policy** (summaries upsert): `R` = replace all fields (`= EXCLUDED.x`); `C` = COALESCE all (preserve when incoming NULL); `R+oC` = replace, but COALESCE the two outside totals; `C+oR` = COALESCE, but replace outside totals. Field-set deviations noted.
- **Floor**: minimum accepted election year.
- **Tx**: `pool` = throws unless given a Pool ("must receive a Pool"); `self` = opens own transaction on a Pool, runs **unwrapped** on a client (pre-refactor Texas semantics); `factory` = delegates to the shared writer.
- **Val+**: `G` = outside-group-breakdown ↔ group **pairing** validation (each breakdown must match a group in the snapshot; two message variants exist: "require matching…" and "must reference…"); `g` = presence-only check (breakdowns need ≥1 group, no pairing); `N` = `normalizeCommitteeId`; `M` = manual-link protection (link upsert preserves `link_status`/`link_source` when the existing row's `link_source = 'manual'`).
- **Signed**: summary fields allowed to be negative.
- **Direct cats**: `std` = `occupation` + `contribution_size`.
- **Tests** (per-module files): W = writer, L = ballot-lookup loader, A = auto-link, B = batchSync.

The shared factory (`createStandardStateFinanceSnapshotWriter`) after PR #488 + the second-wave prep PR + the outside-identity PR #509: canonical link columns, all 5 tables mandatory, outside-group identity configurable (defaults `committee_id`/`committee_name`), tx `pool`, config = `{label, tables, minElectionYear (required), summaryUpdatePolicy? (per-column replace|preserveWhenNull, default preserve), outsideGroupValidation? (none|presence|pairing, default none), normalizeCommitteeId? (fn applied at every committee-id write/compare site, default identity), supersededLinkSource? (an active incoming link with this source deactivates other active same-source links for the candidate+election inside the snapshot tx, default off), outsideGroupIdentityColumns? ({id?, name?} column names in the outside tables, default committee_id/committee_name, identifier-validated; input fields stay committeeId/committeeName — wrappers map)}`. linkIdentityColumns? ({id?, name?} column names in the links table, default committee_id/committee_name, identifier-validated; id joins the link conflict target) is also configurable since the link-identity PR. Still missing (add only when a migrating cohort needs it): signed-amount fields, caller-transaction mode, manual-link protection, optional tables.

## Writers

| Module | Tables | Link extras | Merge | Floor | Tx | Val+ | Signed | Direct cats |
|---|---|---|---|---|---|---|---|---|
| alaska | 5 | →`candidate_filer_id`/`candidate_filer_name` | R | 2000 | pool | G | — | std |
| arizona | 5 | none | R | 2002 | pool | g | — | std |
| california | 5 | no `district`; →`controlled_committee_id`/`_name` | C; +`debts_owed`; no `direct_contribution_total` | 2001 | self | — | — | std+`contributor_source_type`+`industry` |
| colorado | 3 (no outside) | +`tracer_candidate_id` | C; only receipts+source | 2001 | self | — | — | std |
| connecticut | 3 (no outside) | none | C; no direct/cash | 2008 | self | — | — | std |
| districtOfColumbia | 5 | →`committee_key` | C | 2000 | pool | G | — | std |
| florida | **6** (+`outside_group_links`, 2nd insert) | none | R+oC | 1996 | pool | — | — | std |
| hawaii | 5 | +`election_period` | C+oR | 2000 | pool | g | — | std |
| houston | factory wrapper | — | factory | factory | factory | — | — | — |
| illinois | 5 | +`sbe_candidate_id`,`sbe_district_type`,`sbe_office`,`is_at_large`; →`committee_key` | R+oC; +`debts_owed` | 2000 | pool | G,M | `cash_on_hand` | std |
| indiana | 3 (no outside) | none | R; only receipts+direct | 2000 | self | M | — | std+`pac_backed_industry` |
| kentucky | 5 | +`candidate_key`; →`committee_key` | R | 2000 | pool | G | `cash_on_hand` | std |
| losAngelesCity | 4 (no `outside_group_breakdowns`); `lacity_` prefix | different shape (city outlier) | — | none | self | — | — | — |
| louisiana | 5 | →`filer_number`/`filer_name` | R | 2000 | pool | G | `cash_on_hand` | std+`contributor_type`+`donor` |
| maine | 5 | supersedes same-source active links (`cfis_bulk`) | R+oC | 2000 | pool | G,N | — | std |
| maryland | 5 | supersedes same-source active links (`cfs_public_export`) | R+oC | 2000 | pool | G,N | — | std |
| massachusetts | 5 | →`candidate_cpf_id`/`filer_name`; keeps `committee_name` | R | 2000 | pool | g | — | std |
| michigan | 5 | none | R+oC; +`candidate_loan_total` | 2000 | pool | G,M | — | std |
| minnesota | **4 (no `direct_breakdowns`)** | none | C | 2000 | pool | G,M | — | n/a (no direct table) |
| nebraska | 3 (no outside) | none | C; only receipts+direct | 2021 | self | — | — | std |
| newJersey | 5 | →`candidate_entity_s`/`entity_name`; +`election_type_code` | R | 1980 | pool | G | — | std |
| newMexico | 5 | none | C; no `cash_on_hand` | 2020 | self | — | — | std |
| newYorkCity | 5 (`nyc_` prefix) | city outlier | different summary fields entirely (`private_contributions`, `net_expenditures`, `outstanding_bills`, `public_funds`) | none | self | M | — | — |
| newYork | 5 | →`filer_id`/`filer_name` | C+oR | 2000 | pool | g | — | std+`contributor_type`+`donor`+`industry` |
| oklahoma | 3 (no outside) | none | C; only receipts+direct | 2014 | self | — | — | std |
| oregon | 5 | none | R | 2000 | pool | g | — | std |
| pennsylvania | 5 | →`filer_id`/`filer_name` | R+oC | 2000 | pool | G,M | — | std |
| tennessee | 5 | +`camp_candidate_id`,`owner_name`,`report_list_url` | R; no disb/cash | 2000 | self | G | — | std |
| texas | factory wrapper | — | factory | factory | factory | — | — | — |
| utah | 3 + **alt outside tables** (`supporting_committees`, `supporting_committee_industries`) | →`folder_id` | R; no outside | 1998 | self | — | — | `contribution_size` only |
| vermont | 5 | +`filer_registration_guid`,`entity_id`; →`filer_name` | R | 2000 | pool | g | — | std+`contributor_source_type`+`donor` |
| virginia | 3 (no outside) | +`committee_code` | C; only receipts+direct | 2000 | self | — | — | std |
| washington | 5 | +`filer_id`,`candidacy_id` | C+oR | 2000 | pool | g | — | std+`employer`+`industry` |
| wisconsin | 5 | +`entity_id`,`assigned_committee_id` | C | 2000 | pool | g | — | std+`employer`+`industry` |

### Writer outside-group identity columns

The factory's outside-groups and outside-group-breakdowns identity columns default to `committee_id`/`committee_name` and are configurable via `outsideGroupIdentityColumns` since PR #509; the `committeeId`/`committeeName` input fields stay fixed (wrappers map state-specific field names onto them). Writers diverge (this was missed in the first pass and disqualified Oregon as a pilot):

- **`committee_id`/`committee_name` (factory-canonical)**: arizona, california, florida, hawaii, maine, maryland, michigan, minnesota, newMexico
- **`committee_key`**: districtOfColumbia, illinois, kentucky, tennessee
- **`sponsor_id`/`sponsor_name`** (and `sponsorId` input fields): oregon, washington, wisconsin
- **One-offs**: alaska `outside_group_id`; louisiana `filer_number`; massachusetts `iepac_cpf_id`; newJersey `outside_entity_s`; newYork `filer_id`; newYorkCity `spender_id`; pennsylvania `group_id`; utah `committee_name` only (alt tables); vermont `filer_registration_guid`
- **No outside tables**: colorado, connecticut, indiana, nebraska, oklahoma, virginia (+ texas/houston wrappers, factory-owned)

Any state outside the factory-canonical group needs a configurable outside-identity column before its writer can migrate — shipped as `outsideGroupIdentityColumns` (own PR, 2026-08-02). Caution: outside identity alone unblocks only **oregon** (its link side is canonical). dc/alaska also rename the link identity column — covered by `linkIdentityColumns` since the link-identity PR, making both migratable. massachusetts has an extra link column (`filer_name` — 13-param link upsert, verified 2026-08-02) and still needs factory work.

**Cascade-FK trap (affects migration validation choice):** the `*_outside_group_breakdowns` tables carry a `FOREIGN KEY ... REFERENCES *_outside_groups ON DELETE CASCADE` (verified for arizona, texas, houston — same DDL template). The factory upserts breakdowns before deleting stale groups, so under validation `none` or `presence` a breakdown referencing a stale group inserts, is cascade-deleted by the stale-group cleanup, and is still counted as written. Any state with this FK must migrate with `outsideGroupValidation: "pairing"` (arizona does, per PR #492 review) — or document why not. Texas/Houston run `none` today; that window predates the factory (it was extracted byte-identical from Texas) and fixing them is a separate behavior-change decision.

Val+ distribution: pairing validation `G` in 12 (two message variants — "require matching": dc, kentucky, maryland, newJersey, tennessee, maine; "must reference": alaska, illinois, louisiana, michigan, minnesota, pennsylvania); presence-only `g` in 10; manual-link protection `M` in 6 (illinois, indiana, michigan, minnesota, newYorkCity, pennsylvania). Factory coverage: pairing/presence configurable since PR #488 (with the factory's "must reference" wording — "require matching" states reproduce their wording in wrapper pre-validation), `normalizeCommitteeId` and same-source link supersession (maine `cfis_bulk`, maryland `cfs_public_export` — a first-pass matrix miss, recorded here) configurable since the second-wave prep PR; manual-link protection `M` still factory-absent.

## Ballot-lookup loaders (identity columns per relation)

`link.id`, `link.candidate_id`, `link.election_id`, `*.link_id` are universal and omitted. Shared loader today supports `committee_id`/`committee_key` as one column across all relations.

| Module | File | Link identity | Outside-group / breakdown identity |
|---|---|---|---|
| alaska | `alaskaCandidateFinanceBallotLookup.ts` (nonstandard name) | `candidate_filer_id` | `outside_group_id` |
| arizona | `arizonaFinanceBallotLookup.ts` (nonstandard name) | `committee_id` | `committee_id` |
| california | standard name | `controlled_committee_id` | `committee_id` |
| colorado | standard | `committee_id` | direct-only |
| connecticut | standard | `committee_id` | direct-only |
| districtOfColumbia | standard | `committee_key` | `committee_key` |
| florida | **no loader file** | — | — |
| hawaii | standard | `committee_id` | `committee_id` |
| houston / illinois / texas | wrappers over shared loader | — | — |
| indiana | standard | `committee_id` | direct-only |
| kentucky | standard | `committee_key` | `committee_key` |
| losAngelesCity | `losAngelesBallotLookupFinanceLoader.ts` | `fppc_committee_id` | city outlier |
| louisiana | standard | `filer_number` | `filer_number` |
| maine | standard | `committee_id` | `committee_id` |
| maryland | standard | `committee_id` | `committee_id` |
| massachusetts | standard | `candidate_cpf_id` | `iepac_cpf_id` |
| michigan | standard | `committee_id` | `committee_id` |
| minnesota | standard | `committee_id` | `committee_id` |
| nebraska | standard | `committee_id` | direct-only |
| newJersey | standard | entity columns | entity columns |
| newMexico | standard | `committee_id` | `committee_id` |
| newYorkCity | standard | `cfb_candidate_id` | `spender_id` |
| newYork | standard | `filer_id` | `filer_id` |
| oklahoma | standard | `committee_id` | direct-only |
| oregon | standard | `committee_id` | **`sponsor_id`** (mixed) |
| pennsylvania | `pennsylvaniaBallotLookupFinance.ts` (nonstandard name) | `filer_id` | **`group_id`** (mixed) |
| tennessee | standard | `camp_candidate_id` | `committee_key` |
| utah | standard | `folder_id` | alt tables |
| vermont | standard | `filer_registration_guid` | `filer_registration_guid` |
| virginia | standard | `committee_id` | direct-only |
| washington | standard | `committee_id` | **`sponsor_id`** (mixed) |
| wisconsin | standard | `committee_id` | **`sponsor_id`** (mixed) |

Mixed-identity states (link column ≠ outside column) are why the loader descriptor must be per-relation, not a single option.

## Auto-link shape

- **Standalone file with resolve + write** (wrapper-equivalent candidates): 26 modules.
- **File without link-writing**: illinois (resolve-only), michigan (list-only).
- **Embedded in batchSync, no file**: arizona, maryland, vermont (vermont adds statewide-only safety logic).
- **No auto-link at all**: newJersey, newYorkCity, utah.

## Transaction-ownership note

`self` writers (california, colorado, connecticut, indiana, losAngelesCity, nebraska, newMexico, newYorkCity, oklahoma, tennessee, utah, virginia) open their own transaction only when handed a Pool; on a client they run **without** a wrapping transaction (assumed caller-owned). The factory instead throws on non-Pool input. Migrating a `self` writer to the factory changes this contract — check each one's callers (the `cycleArtifactRows` batch cohort passes clients: colorado, indiana, maine*, maryland*, nebraska, newMexico, oklahoma — *maine/maryland are `pool` writers, so their batch paths must already cope; verify when they migrate).

## Test coverage (per module)

| Role | Have tests | Missing |
|---|---|---|
| Writer | 34 of 34 (minnesota added in PR #488) | — |
| Ballot-lookup loader | 10 of 31 with loader files | 21 modules — characterization tests required before each loader cohort migrates |
| Auto-link | 25 of 28 files | illinois, michigan + 1 |
| BatchSync | 34 of 34 | — |

The factory itself has a dedicated config-option test file (`tests/pipeline/finance/standardStateFinanceSnapshotWriter.test.ts`, PR #488) alongside the Texas/Houston wrapper tests.

## Cohort readout (feeds plan phases)

- **Phase 1 pilot (exact canonical shape)**: **arizona** — the only state with canonical link columns, all 5 tables, factory-canonical outside identity, merge `R`, std categories, `pool` tx. The factory prereqs (per-field merge policy, required floor, presence check `g`) shipped in PR #488. Oregon was originally listed as a co-pilot but is disqualified: its outside tables use `sponsor_id`/`sponsor_name` and its exported types use `sponsorId` fields.
- **Phase 1 second wave (factory-canonical outside identity only)**: maine, maryland (`G`+`N` + `R+oC` policy + same-source supersession — factory prereqs `normalizeCommitteeId` + `supersededLinkSource` shipped in the second-wave prep PR). **michigan is deferred to Phase 5**: its `candidate_loan_total` summary column is a one-state capability (grep-verified 2026-08-01 — no other writer has it), and the plan's working rule keeps one-state features out of the factory; it also needs manual-link protection `M`. The outside-identity renames (`sponsor_id`, `group_id`, `iepac_cpf_id`, `outside_group_id`, `filer_number`) are covered by `outsideGroupIdentityColumns` since PR #509 — **oregon migrated onto the factory** (third wave: sponsor identity columns + `sponsorId`/`sponsorName` field mapping in the wrapper, `presence` validation kept — its cascade-FK stale-group window predates the migration, same standing decision as Texas/Houston). Link-identity renames are covered by `linkIdentityColumns` since the link-identity PR — **districtOfColumbia and alaska are now migratable** (fourth wave). The rest still need factory work: washington/wisconsin (extra link columns), pennsylvania (manual-link protection), massachusetts (extra `filer_name` column), louisiana (signed `cash_on_hand` + extra direct category types), florida (extra table).
- **Deferred writer families (Phase 5)**: michigan (`candidate_loan_total` + manual-link protection), california, illinois, hawaii, kentucky, vermont, wisconsin, washington, newJersey, tennessee (extra columns/fields); colorado, connecticut, indiana, nebraska, oklahoma, virginia (3-table direct-only + reduced summary sets); minnesota (no direct table); utah (alt outside tables); city outliers (nyc, losAngelesCity) stay bespoke.
- **Loader cohorts**: uniform-identity `committee_id` states first (hawaii, maine, maryland, michigan, minnesota, newMexico + direct-only colorado, connecticut, indiana, nebraska, oklahoma, virginia); then `committee_key` (dc, kentucky); mixed-identity and renamed states after the per-relation descriptor exists; alaska/arizona/pennsylvania (nonstandard filenames) and florida (no loader) resolved case-by-case.
