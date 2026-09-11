# State Resources Pipeline Plan (Beginner Friendly)

This document explains how `state_resources` data should move from research to your production database safely.

## Goal
Build a safe pipeline where AI helps with research, but AI never writes directly to production tables.

## Big Picture Flow
1. Producer creates draft items in `staging:draft`.
2. Enricher consumes drafts, fills researched fields, and publishes to `staging:pending`.
3. Validator consumes `staging:pending` and checks quality/structure.
4. Writer consumes `staging:validated` and upserts to `state_resources`.
5. Scheduler runs the full flow yearly (after pipeline is stable).

---

## Phase 1: Data Contract (must be strict)
Define exactly what an enriched `state_resources` item must contain.

Required fields:
- `state_fips`
- `state_abbreviation`
- `state_name`
- `polling_place_url`
- `voter_registration_url`
- `vote_by_mail_info`
- `polling_hours`
- `id_requirements`
- `sources` (with per-field citation arrays)

Required metadata on `staging_items`:
- `schema_version`
- `prompt_version`

Schema version rules:
- Producer sets `schema_version = 'state_resources_draft_v1'` for draft items sent to `staging:draft`.
- Enricher sets `schema_version = 'state_resources_enrichment_v1'` for enriched items sent to `staging:pending`.
- Validator accepts only `schema_version = 'state_resources_enrichment_v1'` and rejects mismatches.

Required limits:
- `vote_by_mail_info` max 4000 characters
- `polling_hours` max 1000 characters

Rule:
- If required fields, required citations, metadata, or limits fail, reject the item.

Why:
- Prevents unpredictable AI output from entering your system.

---

## Phase 2: Model-Agnostic AI Interface
Create one backend interface so AI providers are interchangeable.

Interface contract (target shape):

```ts
type EnrichStateResourcesInput = {
  ingestKey: string;
  draft: StateResourceDraftPayload;
  evidence: Array<{ url: string; title: string; snippet: string }>;
  promptVersion: string;
};

type EnrichStateResourcesResult =
  | { ok: true; payload: StateResourcePayload; schemaVersion: "state_resources_enrichment_v1" }
  | { ok: false; retryable: true; reason: string; errorCode: "RATE_LIMIT" | "TIMEOUT" | "TEMP_PROVIDER_ERROR" }
  | { ok: false; retryable: false; reason: string; errorCode: "INVALID_JSON" | "SCHEMA_MISMATCH" | "MISSING_REQUIRED_FIELDS" };

enrichStateResources(input, config): Promise<EnrichStateResourcesResult>
```

Provider adapters:
- OpenAI (first)
- Claude (later)
- Gemini (later)

Why:
- You can switch models without rewriting pipeline logic.

---

## Phase 3: Retrieval/Evidence Layer
Before AI writes summaries, gather evidence text from real sources.

Start with seed URLs:
- vote.org
- nass.org
- usvotefoundation.org

Then allow additional web sources.

Implementation notes:
- Fetch seed pages and clean text into short evidence snippets.
- Discover additional URLs from fetched page links (bounded crawl).
- Store snippets with URL/title in `staging_items.payload.evidence`.
- AI citations must match collected evidence URLs.

Why:
- AI should summarize evidence, not guess.

---

## Phase 4A: Mock Enricher (plumbing test)
Build enricher worker with fake deterministic output first.

Worker behavior:
1. Read draft `state_resources` items.
2. Produce mock enriched payload in the exact final schema.
3. Update `staging_items` payload.
4. Push to validator stream.

Why:
- Proves stream/db pipeline works before adding real AI complexity.

---

## Phase 4B: Real AI Enricher
Swap mock logic for real AI provider calls.

Behavior:
1. Read draft item + evidence.
2. Ask AI to return strict JSON only.
3. Parse and validate response shape.
4. Save enriched payload to `staging_items`.
5. Send to validator.
6. Handle errors with explicit rules:
   - Retryable (`RATE_LIMIT`, `TIMEOUT`, temporary provider outage): keep `status='pending'`, set `reason`, and requeue to `staging:draft` with backoff.
   - Permanent (`INVALID_JSON`, `SCHEMA_MISMATCH`, missing required fields): set `status='rejected'` with `reason` and publish to `staging:rejected`.

Why:
- Keeps AI integration contained and auditable.

---

## Phase 5: Validator (hard quality gate)
Validator checks:
- required fields present
- URL format
- citation structure in `sources`
- metadata present (`schema_version`, `prompt_version`)
- schema_version match (`state_resources_enrichment_v1` only)
- state mapping consistency using deterministic in-code map (`STATE_ABBR_BY_FIPS`) for FIPS <-> abbreviation
- field length rules

Outputs:
- valid -> `staging_items.status='validated'` + `staging:validated`
- invalid -> `staging_items.status='rejected'` + reason + `staging:rejected`

Reliability:
- at-least-once delivery
- dedupe downstream by `ingest_key`
- reclaim stale pending messages with `XAUTOCLAIM`

---

## Phase 6: Writer (only DB writer)
Writer behavior:
1. Consume `staging:validated`.
2. Read authoritative payload from `staging_items`.
3. Upsert `state_resources` by `state_fips`.
4. Mark `staging_items.status='written'` and set `written_at`.

Reliability:
- idempotent upsert by `state_fips`
- recovers pending messages with `XAUTOCLAIM`
- supports recovery/republish path for already-written rows
- marks real write failures as `failed` with reason

---

## Phase 7: Quality Controls
Add controls around enrichment quality:
- URL reachability checks
- citation completeness checks
- domain preference scoring (official election sources preferred)
- confidence thresholds for auto-accept

---

## Phase 8: Human Review for Edge Cases
Route uncertain/conflicting items to manual review.

Examples:
- conflicting sources
- low confidence
- unclear legal wording

Why:
- Protects data quality in sensitive civic information.

---

## Phase 9: Observability
Track pipeline health:
- counts by status (`pending`, `validated`, `rejected`, `written`, `failed`)
- top rejection reasons
- retry/reclaim counts
- per-run timing and throughput

---

## Phase 10: Annual Scheduler (last)
After pipeline quality is stable:
- run a yearly job for all 50 states + DC
- keep manual trigger for ad-hoc reruns
- keep dry-run mode for safety checks

Why this is last:
- Scheduler automates timing, not quality. Stabilize quality first.

---

## Current Status (as of now)
Completed:
- Producer
- Mock enricher (Phase 4A)
- Retrieval/evidence baseline (Phase 3)
- Validator
- Writer
- Reliability hardening (reclaim + recovery behavior)

Next practical step:
- Swap to real AI enricher (Phase 4B) using collected evidence.
