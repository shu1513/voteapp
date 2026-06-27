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

Gap:

- The existing `--once` CLI consumes Redis stream entries, not a specific `election_id`. A tiny pilot wrapper may still be
  useful after the first manual payload is proven if we want exact one-election targeting without stream setup.

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

Gap:

- Candidate profile validation and write logic are embedded inside `runCandidateProfileEnricher` after the AI call.
- A manual adapter should call `parseCandidateProfilePayload`, then call `findOrCreateCandidateFromProfile` and
  `upsertCandidateElection` directly in one transaction.

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

Gap:

- Discovery, source repair, and area labeling are embedded inside `runCandidateRecordEnricher`.
- A manual adapter should validate the discovered record payload, verify URLs, upsert records, map returned record IDs to
  labels, validate labels, then upsert tags.

### Ballot Measures

Prompt brief:

- `src/ai/providers/ballotMeasuresPrompt.ts`

Existing flow:

- `runBallotMeasuresEnricher` in `src/pipeline/enrichers/ballotMeasuresEnricher.ts`

Reusable tag helpers:

- `loadAllowedBallotMeasureResearchAreas` and `upsertBallotMeasureResearchAreaTags` in
  `src/pipeline/ballotMeasures/ballotMeasureResearchAreaTags.ts`

Gap:

- Ballot-measure payload parsing and source verification are currently private inside `src/ai/enrichBallotMeasure.ts`.
- The smallest future code change is to export or extract a manual-safe validator for a researched ballot-measure payload.
  Until then, a pilot can either use the existing enricher path or add a narrow wrapper that mirrors the private parser.

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

## Expected Tiny Wrappers After Phase 1

The first pilot proved the election payload shape. Initial wrappers now exist for the two lowest-risk stream injection steps:

1. `manual:elections:inject` - accepts researched election JSON, validates it, writes staging, and emits the validator stream
   message.
2. `manual:candidate-roster:inject` - accepts an election ID and researched roster JSON, validates it, marks the roster
   staging row as validated, and emits the roster draft stream message.

Remaining wrapper gaps:

1. `manual:candidate-profile:write` - accepts an election ID and profile JSON, validates, finds/creates candidate, and links
   candidate to election.
2. `manual:candidate-records:write` - accepts candidate/election IDs plus record and label JSON, validates, upserts records,
   and upserts area tags.
3. `manual:ballot-measure:write` - accepts an election ID and researched ballot-measure JSON, validates, upserts measure, and
   upserts tags.

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
- The operator environment must have write access to Postgres and publish access to Redis.
- Run one district or election at a time and verify canonical rows after each stage before continuing.

Added two manually invoked scripts:

- `npm run manual:elections:inject -- --file payload.json [--ingest-key key] [--run-id id] [--dry-run]`
- `npm run manual:candidate-roster:inject -- --election-id uuid --file roster.json [--run-id id] [--dry-run]`

Safety properties:

- No API route, frontend, scheduler, or normal worker behavior is changed.
- Scripts run only when manually invoked.
- Both scripts validate payloads before writing.
- `--dry-run` validates and prints the planned staging keys without connecting to Postgres or Redis.
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
- Candidate roster injection only pre-seeds a validated roster row; the existing roster worker can then fan out profile drafts
  without making an AI roster call.
