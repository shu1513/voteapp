# Manual Research Adapter

This document captures the phase-1 plan for populating local Postgres with Codex-researched election data while reusing the
backend's existing AI prompt briefs, validators, and writers wherever they are already cleanly separated.

The goal is not to add a second data model. A manual research run should produce the same contract-shaped payloads that the
AI provider calls return today, then pass those payloads through the same conservative backend checks before writing canonical
rows.

## Scope

Unit of work:

1. Pick one row from `public.districts`.
2. Research future elections for exactly that district scope.
3. For office elections, research candidate rosters, candidate profiles, candidate-election links, and candidate records.
4. For ballot-measure elections, research ballot-measure detail and research-area tags.
5. Verify inserted/updated rows in canonical tables.

The first pilot should use a small district scope with real upcoming contests. `statewide` is a good first target because it
exercises office matching and ballot measures without requiring address-level district union logic.

## Existing Pieces To Reuse

### Elections

Prompt brief:

- `src/ai/providers/electionsPrompt.ts`

Payload contract and shape validation:

- `parseCanonicalElectionPayload` in `src/contracts/electionPayloadContract.ts`

Existing validator/writer path:

- `runElectionsValidator` in `src/pipeline/validators/electionsValidator.ts`
- `runElectionsWriter` in `src/pipeline/writers/electionsWriter.ts`

Why this path is the best first adapter target:

- It already accepts an enriched payload from `staging_items`.
- The validator handles schema, scope mismatches, date sanity, and soft-fail/retry feedback.
- The writer already links `elections.office_id` through `OfficeMatcher`, writes `election_senate_metadata`, remembers
  `election_seed_urls`, and emits downstream candidate-roster or ballot-measure draft messages.

Manual injection shape:

1. Build an `ElectionEnrichedPayload`.
2. Upsert `staging_items` with:
   - `item_type = 'election'`
   - `status = 'pending'`
   - `schema_version = ELECTION_ENRICHMENT_SCHEMA_VERSION`
   - `payload = <ElectionEnrichedPayload>`
   - `model = 'manual-research:codex'`
3. Add the ingest key to `staging:elections:pending`.
4. Run `npm run elections:validate -- --once`.
5. Run `npm run elections:write -- --once`.

### Candidate Roster

Prompt brief:

- `src/ai/providers/candidateRosterPrompt.ts`

Payload contract:

- `parseCandidateRosterPayload` in `src/contracts/candidateRosterPayloadContract.ts`

Existing flow to reuse:

- `runCandidateRosterEnricher` in `src/pipeline/enrichers/candidateRosterEnricher.ts`

Important behavior:

- If the `candidate_roster:<election_id>` staging row is already `validated` or `written`, the enricher reads the stored
  candidates and skips the AI roster call.
- It then fans out candidate profile draft messages with candidate-level seed URLs.

Manual injection shape:

1. Build `{ election_id, candidates }`, where each candidate row satisfies `CandidateRosterEntry`.
2. Mark or insert the `candidate_roster:<election_id>` staging row as `validated`, with `run_id`, `model =
   'manual-research:codex'`, `schema_version = NULL`, and `ai_raw_debug.manual_research = true`.
3. Emit `staging:candidates:roster:draft` for that election.
4. Run `npm run candidates:roster:enrich -- --once`.

Manual fanout:

- Prefer `manual:candidate-roster:fanout` after roster injection. It reads the validated staging row for one election and
  emits profile drafts without relying on a broad Redis stream worker to claim the intended message.

### Candidate Profile

Prompt brief:

- `src/ai/providers/candidateProfilePrompt.ts`

Payload contract:

- `parseCandidateProfilePayload` in `src/contracts/candidateProfilePayloadContract.ts`

Reusable write/link helpers:

- `findOrCreateCandidateFromProfile` in `src/pipeline/candidates/candidateProfileIdentity.ts`
- `upsertCandidateElection` in `src/pipeline/candidates/candidateProfileLinks.ts`

Important behavior to preserve:

- Existing candidates are matched only by same name plus a hard identifier such as date of birth, social/profile URL,
  official website URL, FEC ID, or state filing ID.
- Duplicate-name rows without hard identifiers should not be force-linked.
- Nonpartisan contests store `party = 'Nonpartisan'`.

Manual path:

- `validateCandidateProfileAiPayload` is exported from `src/ai/enrichCandidateProfile.ts` for no-AI manual payload
  validation, including candidate profile source URL reachability checks.
- `manual:candidate-profile:write` validates the researched profile payload with that shared validator, requires a hard
  identifier by default, calls `findOrCreateCandidateFromProfile`, and links the candidate to the target office election with
  `upsertCandidateElection` in one transaction.
- Add `--repair-report-file file` to write a `manual_research_repair_report.v1` JSON report when validation or quality gates
  find gaps. Add `--strict-quality-gate` when the run should stop on missing profile fields such as summary, official website,
  or party in partisan contests. After a focused field-only repair pass finds no reliable value, rerun with
  `--confirmed-gap <gap-id>` using the exact repair-report gap id, such as `candidate_profile.summary` or
  `candidate_profile.official_website_url`, to document the confirmed null instead of blocking import.
- Add `--is-incumbent true|false` when the researched roster/profile pass has source-backed incumbency information. If omitted,
  the writer preserves an existing candidate-election incumbency value when one exists.
- Add `--emit-record-draft` only when the operator wants the profile write to enqueue the next candidate-record draft artifact
  after the candidate/election link commits.
- Add `--emit-finance-sync` only when the operator deliberately wants the optional production profile side effect. It reuses
  the same linked-election campaign-finance sync fanout as the normal candidate-profile enricher after the candidate/election
  link commits.

### Candidate Records

Prompt briefs:

- `src/ai/providers/candidateRecordDiscoveryPrompt.ts`
- `src/ai/providers/candidateRecordAreaLabelPrompt.ts`

Payload contracts:

- `parseCandidateRecordDiscoveryPayloadPartial` in `src/contracts/candidateRecordDiscoveryPayloadContract.ts`
- `parseCandidateRecordAreaLabelPayload` in `src/contracts/candidateRecordAreaLabelPayloadContract.ts`

Reusable write/tag helpers:

- `upsertCandidateRecords` in `src/pipeline/candidates/candidateRecordStore.ts`
- `loadCandidateElectionOfficeContext` in `src/pipeline/candidates/candidateRecordOfficeContext.ts`
- `loadAllowedResearchAreasForOfficeId`, `validateCandidateRecordAreaLabels`, and `upsertCandidateRecordAreaTags` in
  `src/pipeline/candidates/candidateRecordAreaTagging.ts`

Important behavior to preserve:

- Candidate records dedupe by candidate plus v3 identity key: normalized source URL, event date, and description.
- Similar same-date/same-source descriptions update existing rows when similarity is high enough.
- Area labels must be limited to areas allowed for the matched `office_id`, with universal `general` and
  `integrity_and_ethics` included by policy.

Manual path:

- `validateCandidateRecordDiscoveryPayload` is exported from `src/ai/enrichCandidateRecords.ts` for no-AI manual record
  validation. It reuses the production partial parser and source URL verifier, returning verified records and rows that need
  source/schema repair.
- `manual:candidate-records:write` validates the researched record payload with that shared validator, fails fast if any row
  is dropped for source/schema repair, validates the separate area-label payload, upserts records, maps persisted record IDs
  to labels, validates labels, prunes stale labels for touched records, and upserts tags.
- Add `--repair-report-file file` to write a `manual_research_repair_report.v1` JSON report for dropped records, bad label
  payloads, label-validation failures, or quality gaps. Add `--strict-quality-gate` when the run should stop on zero verified
  records or all-neutral/general labels until a focused record or label repair pass is completed. After a focused pass confirms
  no source-backed replacement or no stance-bearing records exist, rerun with `--confirmed-gap <gap-id>` to preserve that
  outcome in dry-run output and avoid blocking live import.

### Ballot Measures

Prompt brief:

- `src/ai/providers/ballotMeasuresPrompt.ts`

Existing flow:

- `runBallotMeasuresEnricher` in `src/pipeline/enrichers/ballotMeasuresEnricher.ts`

Reusable tag helpers:

- `loadAllowedBallotMeasureResearchAreas` and `upsertBallotMeasureResearchAreaTags` in
  `src/pipeline/ballotMeasures/ballotMeasureResearchAreaTags.ts`

Manual path:

- `validateBallotMeasureAiPayload` is exported from `src/ai/enrichBallotMeasure.ts` for no-AI manual payload validation,
  including `official_measure_url` and `sources` reachability checks.
- `manual:ballot-measure:write` validates the researched payload with the same full validator, verifies the target election is
  a ballot-measure race, upserts `ballot_measures`, and upserts ballot-measure research-area tags.

## Pilot Sequence

1. Confirm DB baseline counts for the selected district:
   - matching `districts` row
   - existing `elections`
   - linked `candidate_elections`
   - existing `ballot_measures`
2. Generate and save the exact prompt text used as the research brief.
3. Research official sources first, then trusted secondary sources only as backup.
4. Produce JSON matching the relevant backend contract.
5. Run the contract parser before any write.
6. Use existing writer/helper functions rather than raw SQL where helper functions exist.
7. Verify row counts and sample reads after each stage.
8. Record unresolved ambiguity instead of guessing.

## Manual Wrapper Commands

Committed local/operator wrappers now cover the no-AI district import path:

1. `manual:elections:inject` - accepts researched election JSON, validates it, writes staging, and emits the validator stream
   message.
2. `manual:candidate-roster:inject` - accepts an election ID and researched roster JSON, validates it, marks the roster
   staging row as validated, and emits the roster draft stream message.
3. `manual:candidate-roster:fanout` - reads the validated `candidate_roster:<election_id>` staging row and emits profile
   drafts for only that election, avoiding broad Redis worker consumption.
4. `manual:candidate-profile:write` - accepts an election ID and profile JSON, validates payload shape and profile source URL
   reachability, finds/creates candidate, links candidate to election, and can emit a focused repair report for validation or
   quality gaps. With `--emit-finance-sync`, it also requests the same optional linked-election campaign-finance sync fanout
   used by the production profile worker.
5. `manual:candidate-records:write` - accepts candidate/election IDs plus record and label JSON, validates payload shape and
   record source URL reachability, fails fast on rows needing source/schema repair, upserts records, upserts area tags, and can
   emit a focused repair report for source, schema, label, or quality gaps.
6. `manual:ballot-measure:write` - accepts an election ID and researched ballot-measure JSON, validates payload shape and URL
   reachability, upserts measure, and upserts tags.

These wrappers should be local/operator tooling. They should not change the production read path.

## Phase 2 Pilot Notes: Vermont Statewide

Pilot district:

- `district_id = 9360fb02-6976-4c28-a130-5ce08e125436`
- `district_name = Vermont`
- `district_type = statewide`
- `state = VT`

Manual election payload:

- Staged as `manual:elections:vt-statewide:2026-phase2`.
- Parsed successfully with `parseCanonicalElectionPayload`.
- Validated successfully through `npm run elections:validate -- --once`.
- Written successfully through `npm run elections:write -- --once`.

Rows written or updated by the existing election writer:

- Six 2026-08-11 statewide office primary rows:
  - Governor
  - Lieutenant Governor
  - Secretary of State
  - Treasurer
  - Auditor of Accounts
  - Attorney General
- Six 2026-11-03 statewide office general rows for the same offices.
- One 2026-11-03 ballot-measure election row:
  - `PR.3 - Declaration of Rights; right to collectively bargain`

Office linking result:

- Initial writer pass linked 10 of 12 office rows.
- `Auditor of Accounts` did not match the canonical statewide `State Auditor` office.
- Added one data alias:
  - `scope = statewide`
  - `alias_text = Auditor of Accounts`
  - `normalized_alias = auditor of accounts`
  - `office = State Auditor`
- Replayed the same validated payload through the writer.
- Final writer pass linked all 12 office rows by exact alias.

Seed URLs written:

- `non_judicial_office`
  - `https://sos.vermont.gov/elections/election-info-resources/candidates`
  - `https://www.ncsl.org/elections-and-campaigns/2026-state-primary-election-dates`
- `ballot_measure`
  - `https://legislature.vermont.gov/bill/status/2026/PR.3`
  - `https://www.ncsl.org/elections-and-campaigns/2026-state-primary-election-dates`

Downstream fanout:

- The election writer created 12 pending `candidate_roster:<election_id>` staging rows.
- It also emitted candidate roster draft messages for those 12 office elections.
- Candidate roster was not run during this pilot because the official Vermont SOS pages were blocked by CloudFront from this
  environment, and using non-official candidate lists would risk writing unverified ballot rosters.

Known stale rows still present after the pilot:

- `Proposal 4 (PR.4) — Equal Protection of Law Amendment`
- duplicate `State Treasurer` general row
- `United States Senator` 2026 row
- `Writer Probe Test Election`

Reason:

- `electionsWriter` upserts by `(district_id, official_ballot_title_key, election_date)`.
- It does not delete, supersede, or mark stale rows that are outside the incoming payload's conflict keys.
- A future manual adapter needs a separate, explicit stale-row review step rather than hidden deletion behavior.

## Phase 3 Skill

Created a local Codex skill for repeat manual research runs:

- Skill path: `~/.codex/skills/voteapp-manual-research`
- Main instructions: `~/.codex/skills/voteapp-manual-research/SKILL.md`
- Pipeline reference: `~/.codex/skills/voteapp-manual-research/references/pipeline-map.md`

Use it by asking Codex to use `$voteapp-manual-research`.

Purpose:

- Reuse VoteApp prompt files as research briefs.
- Prefer official sources and browse for current facts.
- Produce contract-shaped payloads.
- Import through existing validators, writers, and helper functions where possible.
- Avoid VoteApp AI provider calls unless explicitly requested.
- Report stale rows and ambiguity instead of silently deleting or guessing.

## Phase 4 Initial Wrapper Scripts

Created an isolated branch for local/operator wrapper work:

- `codex/manual-research-import`

Operational prerequisites:

- Back up the target database or use a staging/local database before running these scripts against important data.
- Set `DATABASE_URL` explicitly for the intended target database before any command that reads or writes DB context.
- Set `REDIS_URL` explicitly before live commands that publish stream messages.
- The operator environment must have write access to Postgres and publish access to Redis.
- Run `npm run manual:research:preflight` before importing. It verifies required manual-import columns, idempotency indexes, and
  unallowed migration-number collisions.
- Run one district or election at a time and verify canonical rows after each stage before continuing.

Manual commands:

- `npm run manual:research:preflight`
- `npm run manual:elections:inject -- --file payload.json [--ingest-key key] [--run-id id] [--dry-run]`
- `npm run manual:candidate-roster:inject -- --election-id uuid --file roster.json [--run-id id] [--dry-run]`
- `npm run manual:candidate-roster:fanout -- --election-id uuid [--run-id id] [--dry-run]`
- `npm run manual:candidate-profile:write -- --election-id uuid --file profile.json [--run-id id] [--is-incumbent true|false] [--emit-record-draft] [--emit-finance-sync] [--allow-no-hard-identifier] [--strict-quality-gate] [--confirmed-gap <gap-id>] [--repair-report-file file] [--dry-run]`
- `npm run manual:candidate-records:write -- --candidate-id uuid --election-id uuid --records-file records.json --labels-file labels.json [--strict-quality-gate] [--confirmed-gap <gap-id>] [--repair-report-file file] [--dry-run]`
- `npm run manual:ballot-measure:write -- --election-id uuid --file payload.json [--dry-run]`
- `npm run manual:candidates:rename -- --candidate-id uuid --display-name "New Name" --source-url https://... --reason text [--first-name text] [--last-name text] [--dry-run]`
  - Guarded correction of a stored candidate name to the official ballot name (the profile writer fills blanks only and
    `--replace-profile-fields` deliberately excludes identity fields, so this wrapper is the supported path). It updates
    `display_name` (plus `first_name`/`last_name` only when those flags are passed), appends the official HTTPS source to
    `profile_sources`, and writes an audit row to `candidate_rename_audit` (migration 253) in the same transaction. It
    refuses merged or soft-deleted rows and refuses when another live candidate in any shared election already carries the
    new name (roster matching treats a same-election `display_name` match as the same person). Records and
    `candidate_elections` links are left untouched; re-running the same command is idempotent and only converges the
    stored source. Local-database guard and `--dry-run` behave like the other cleanup wrappers
    (`manual:candidates:merge`, `manual:candidate-elections:unlink`).

Safety properties:

- No API route, frontend, scheduler, or normal worker behavior is changed.
- Scripts run only when manually invoked.
- Scripts validate payloads before writing. Candidate-profile writes verify profile source URL reachability. Candidate-record
  writes verify record source URL reachability and fail fast when rows need focused source/schema repair. Ballot-measure writes
  also verify `official_measure_url` and source URL reachability in dry-run and live mode; ballot-measure payloads with more
  than 20 unique source URLs fail fast instead of silently dropping citations. Dry-run prints the normalized URLs and explicit
  pass/fail fields for payload shape, official URL reachability, source URL reachability, and allowed tag slugs.
- Election and roster injection dry-runs validate payload shape without connecting to Postgres or Redis. Profile, roster fanout,
  candidate-record, and ballot-measure dry-runs also check target DB context so bad IDs fail before live writes.
- Live manual commands do not fall back to localhost Postgres or Redis when target env vars are missing.
- Election injection defaults to a stable `manual:elections:<district_id>:<currentYear>` ingest key so reruns repair the same
  staging row unless `--ingest-key` is explicitly provided.
- Both scripts connect to Redis before writing Postgres and mark the staging row `failed` if Redis publishing fails after the
  DB write.
- Election JSON may include optional top-level `family_source_urls`; the script stores it in `ai_raw_debug` so the existing
  election writer can populate `election_seed_urls`.
- Election injection still requires the existing `elections:validate` and `elections:write` steps.
- Candidate roster injection checks that the target `election_id` exists in `public.elections` and is an office race before
  staging the roster.
- If roster JSON includes `election_id`, it must match the `--election-id` CLI value.
- Candidate roster injection only pre-seeds a validated roster row; `manual:candidate-roster:fanout` is the preferred no-AI
  follow-up so stale Redis stream entries cannot cause a broad worker to process the wrong roster.
- Candidate profile writes require a hard identifier by default; use `--allow-no-hard-identifier` only after explicit operator
  review.
- Candidate profile writes do not emit campaign-finance sync jobs by default. `--emit-finance-sync` is an explicit, optional
  production-side-effect flag and should be used only when finance sync worker configuration is available and desired.
- Candidate profile and candidate-record writes can emit a `manual_research_repair_report.v1` JSON file with
  `--repair-report-file`. Use this for focused repair runs: validation/source/label failures stop the import and record the
  exact prompt file, target object, gap ID, source URL or field when available, and the narrow research pass to run next.
- `--strict-quality-gate` makes profile and record writers block on meaningful thin-data gaps instead of silently importing
  pipeline-valid but production-weak rows. `--confirmed-gap <id>` is only for the rerun after a focused repair pass proves the
  missing value or neutral-only label set is acceptable.
- Candidate record writes require an existing candidate/election link, an office-linked election, allowed research areas, and a
  label for every record. Reruns replace stale area tags for the records touched by that payload before upserting the current
  labels.
- Ballot-measure writes require an existing ballot-measure election and allowed ballot-measure research areas.
