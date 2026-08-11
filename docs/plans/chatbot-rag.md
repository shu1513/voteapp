# Chatbot (RAG) — build plan

Status: PLAN v2 (2026-08-11, revised after external review). Nothing built yet. Research record: memory `voteapp-chatbot-exploration`.

## Goal

An "Ask" feature where users ask questions about elections/candidates and get answers **only from VoteApp's own database**, at near-zero marginal cost:

```
question → intent router (free templates) → answer cache (free) →
hybrid retrieval (pgvector + tsvector + pg_trgm) → answerability gate →
LLM (capped, swappable; default gpt-5.6-luna) → answer with citations → question log
```

Cost target: ~$30/mo order of magnitude at ~1,000 model-answered messages/day (e.g. 30k calls × ~2k in + ~500 out tokens ≈ $30 at Luna's $0.20/$1.20 per M). Treat as an estimate to verify with measured token usage, not a promise. The router + cache absorb most traffic for free.

## Isolation contract

Everything removable without harming the rest of the app:

- **All backend code in `backend/src/chatbot/`**, wired into `apiServer.ts` through the existing options/DI pattern.
- **All tables in a dedicated Postgres schema `chatbot`** (`chatbot.chunks`, `chatbot.questions`, …). No FKs from core tables into it. Teardown = `DROP SCHEMA chatbot CASCADE`.
- **Kill switch:** `CHATBOT_ENABLED=false` → paths not in the API allowlist, page hidden. No other behavior changes.

Honest list of every touchpoint outside the module (all small, all enumerated so deletion is a checklist):
1. `apiServer.ts`: add chatbot paths to `isKnownApiPath` allowlist + mount handler + DI wiring (flag-guarded).
2. `frontend/src/routes.ts`: one route entry; `App.tsx`: one nav entry (flag-guarded); one API-client function.
3. `render.yaml`: TEI embeddings service + env vars.
4. `backend/package.json`: `chatbot:reindex`, `chatbot:report` scripts.
5. Migration (next free number; currently 232): schema + extensions + grants for the API DB role (see `docs/postgres-api-role.md`).
6. Privacy policy: chat section (required **before** enabling the LLM in prod — names OpenAI as a processor).
7. Tests in `backend/tests/chatbot/` (vitest only picks up `tests/**/*.test.ts`) + `frontend/src/pages/AskPage.test.tsx` + one module `packages/api-client/src/chatbot.ts` (one export line in its index).
8. `.claude/launch.json`: chatbot env on the worktree dev-server entries (local dev only).

## Flags (money = off by default)

| Env | Default | Meaning |
|---|---|---|
| `CHATBOT_ENABLED` | `false` | Master switch |
| `CHATBOT_LLM_ENABLED` | `false` | LLM calls allowed; off = retrieval-only answers |
| `CHATBOT_MODEL` | `gpt-5.6-luna` | |
| `CHATBOT_LLM_BASE_URL` | OpenAI | |
| `CHATBOT_LLM_API_KEY` | — | Separate key, in its own OpenAI **project** with a dashboard spend limit |
| `CHATBOT_REASONING_EFFORT` | `low` | Start low; raise to medium only if the golden set shows it helps |
| `CHATBOT_EMBEDDINGS_URL` | — | TEI service |
| `CHATBOT_USER_DAILY_LIMIT` | `20` | LLM answers per signed-in user per day |
| `CHATBOT_DAILY_TOKEN_BUDGET` | e.g. `5000000` | Global durable budget (Postgres) |

Provider swapping: one narrow adapter interface in `backend/src/chatbot/llm/` (`generateAnswer(prompt, chunks) → {answer, citations, refusalReason, usage}`). Implementation #1 = OpenAI Responses API. Swapping to an OpenAI-compatible chat-completions provider (DeepInfra, Groq, Cloudflare, self-host) = env change + the small chat-completions adapter (~50 lines, written when needed). We do not promise universal env-only compatibility — providers differ on reasoning params and usage reporting; the adapter is the boundary that keeps those differences in one file.

`aiCallGuard` stays untouched: it guards **unattended pipeline** spending. The chatbot is user-triggered spending with its own guards (flag + per-user cap + durable global budget + dashboard limit). Never set `AI_API_CALLS_ALLOWED` for this.

## Components

### 1. Migration

```sql
CREATE EXTENSION IF NOT EXISTS vector;    -- Render offers 0.8.1, local 0.8.2 (verified 2026-08-11; see Risks for why that's acceptable)
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- NOT installed today (only citext is)
CREATE SCHEMA chatbot;

CREATE TABLE chatbot.index_generations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  status text NOT NULL CHECK (status IN ('building','active','retired')),
  embedding_model text NOT NULL,          -- 'bge-small-en-v1.5'
  chunker_version int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz
);

CREATE TABLE chatbot.chunks (
  id bigserial PRIMARY KEY,
  generation_id uuid NOT NULL REFERENCES chatbot.index_generations(id) ON DELETE CASCADE,
  source_type text NOT NULL,              -- 'candidate_profile' | 'candidate_record' | 'finance_summary' | 'election' | 'ballot_measure'
  source_id uuid,                         -- VoteApp ids are UUIDs; plain column, no FK to core tables
  chunk_key text NOT NULL,                -- 'profile:experience', 'record:<uuid>', 'finance:<election_uuid>' — allows many chunks per source
  election_id uuid,
  district_id uuid,
  state text,
  title text NOT NULL,
  content text NOT NULL,                  -- ~150–350 tokens (bge-small caps at 512; TEI would silently truncate longer)
  evidence_urls jsonb,                    -- upstream source URLs carried from candidate_records.source_url etc.
  source_updated_at timestamptz,
  content_hash text NOT NULL,             -- hash(source data + chunker_version + embedding model/revision)
  content_tsv tsvector GENERATED ALWAYS AS
    (setweight(to_tsvector('english', title), 'A') || setweight(to_tsvector('english', content), 'B')) STORED,
  embedding halfvec(384),
  UNIQUE (generation_id, source_type, chunk_key)
);
-- NO HNSW index in v1 — deliberate. The shared index would be mutated by every
-- generation build (inserts) and retirement (deletes + vacuum), exactly the
-- paths the 0.8.2–0.8.4 corruption fixes cover, and Render is on 0.8.1. Exact
-- scan is fine at tens of thousands of chunks (a few ms with halfvec). Add
-- `CREATE INDEX ... USING hnsw (embedding halfvec_cosine_ops)` only when the
-- corpus outgrows exact scan AND Render ships pgvector >= 0.8.4.
CREATE INDEX ON chatbot.chunks USING gin (content_tsv);
CREATE INDEX ON chatbot.chunks USING gin (title gin_trgm_ops);
CREATE INDEX ON chatbot.chunks (generation_id);
CREATE INDEX ON chatbot.chunks (election_id);

CREATE TABLE chatbot.questions (          -- NO user identifier column at all
  id bigserial PRIMARY KEY,
  asked_at timestamptz NOT NULL DEFAULT now(),
  question_norm text,                     -- redacted + normalized; nullable: raw+norm deleted together after 90 days
  answered_by text NOT NULL,              -- 'intent:<name>' | 'cache' | 'llm' | 'refused' | 'rate_limited'
  scope_key text,
  matched_chunk_ids bigint[],
  latency_ms int,
  tokens_in int, tokens_out int
);
CREATE INDEX ON chatbot.questions (asked_at);

CREATE TABLE chatbot.question_stats (     -- durable aggregates that survive the 90-day purge
  week date NOT NULL,
  question_norm text NOT NULL,
  outcome text NOT NULL,
  count int NOT NULL,
  PRIMARY KEY (week, question_norm, outcome)
);

CREATE TABLE chatbot.daily_budget (
  day date PRIMARY KEY,
  tokens_reserved bigint NOT NULL DEFAULT 0
);
-- Grants for the API role. Verified 2026-08-11: docs/postgres-api-role.md grants
-- and ALTER DEFAULT PRIVILEGES are scoped to schema public ONLY, so a new schema
-- gets nothing automatically — the API role cannot even see `chatbot` without
-- these. Guarded because the role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT USAGE ON SCHEMA chatbot TO voteapp_api;
    -- Least privilege, per table. Deliberately NOT "SELECT ON ALL TABLES" and
    -- no ALTER DEFAULT PRIVILEGES: the API must not read the question log
    -- (privacy: insert-only from the request path) or question_stats, and any
    -- future table gets an explicit grant in its own migration.
    GRANT SELECT ON chatbot.index_generations, chatbot.chunks TO voteapp_api;
    GRANT INSERT ON chatbot.questions TO voteapp_api;
    GRANT SELECT, INSERT, UPDATE ON chatbot.daily_budget TO voteapp_api;
    GRANT USAGE ON ALL SEQUENCES IN SCHEMA chatbot TO voteapp_api;  -- bigserial inserts
    -- Indexer + reporting scripts run as the owner role, not voteapp_api.
  END IF;
END $$;
```

Notes: `question_norm` is redacted before insert (strip emails, phone numbers, street-address patterns, long digit runs).

### 2. Embeddings service

`ghcr.io/huggingface/text-embeddings-inference:cpu-1.9` + `BAAI/bge-small-en-v1.5` (384 dims) as a Render private service ($7–25/mo). Apply BGE's query instruction prefix **to query embeddings only**, never to document chunks. Local dev: one `docker run` line documented in the module. **Degraded mode:** service down → keyword-only retrieval + warning log; nothing else affected.

### 3. Indexer — `npm run chatbot:reindex` (snapshot generations)

Never mutate the active corpus row by row (a half-failed embed run must not leave a mixed corpus):
1. Insert a `building` generation; extract chunks from source tables through the existing canonical readers; embed; validate counts.
2. Atomically flip: new generation `active`, old `retired` (single transaction).
3. Delete retired generations after a grace period.

Retrieval and cache keys always carry the active generation id, so a flip instantly invalidates all cached answers and there is no partial-reindex state, no manual cache purge, and deleted sources disappear with the old generation. Chunk content spells out names/offices/dates in prose (that's what both search halves match). November-2026 cohort first. Free to run (local embeddings); manual after imports at first, nightly via scheduler later.

### 4. Retrieval (`retrieval.ts`)

Hybrid query over the **active generation** (v1 vector branch is an exact scan — no HNSW, so no `hnsw.iterative_scan` setting; add both together if the index is ever added):
- Entity resolution first: trgm match of the question against candidate/office names via existing canonical readers; resolves scope (election/district) and boosts. **Same-name candidates → return a clarification question, never silently pick.**
- Branch A: `content_tsv @@ websearch_to_tsquery(...)`, `ts_rank_cd` (title weighted A, body B).
- Branch B: cosine over `embedding` (top 20), scope-filtered when known.
- Merge with RRF for **ranking**; keep raw lexical + cosine + entity scores separately, because the **answerability gate thresholds on raw scores, not RRF rank** (RRF rank is relative, not absolute relevance).
- Source-aware filters: exclude retired records, withdrawn candidacies, merged candidates.
- Return top 3–5 chunks.

### 5. Intent router (`intents.ts`)

Deterministic templates + deep links, zero AI: ballot lookup, who's running for X in Y, election dates/registration (answered from the existing state voting resources — official sources, never the LLM), candidate finance summary, candidate profile. Plus **policy intents**: "who should I vote for?" → neutral refusal template (no endorsements, ever). Time-sensitive intents (deadlines, results, candidacy status) are always answered deterministically and are **never** served from the 24h cache or the LLM.

### 6. Answerability gate + LLM call (`answer.ts`)

- Gate on raw retrieval scores (tuned on the golden set). Below threshold → clean refusal ("I don't have that in my data"), with page links **only when entity confidence is high** — no misleading "nearest" links.
- LLM call (flag + caps + budget permitting), via the adapter: OpenAI Responses API, `store: false`, `reasoning.effort` from env (default low), `text.verbosity: "low"`, `max_output_tokens` with reasoning headroom, hashed `safety_identifier`. **Strict structured output:** `{answer, citations: [chunk_id], refusal_reason}`.
- Server-side validation: every cited `chunk_id` must be one we supplied (else drop the citation); all URLs are **server-constructed** from chunk metadata (`evidence_urls`, candidate/election pages) — never model-written. Answer rendered as escaped text.
- UI: answers labeled as AI-generated, show "data current as of <generation activated_at>", and carry the existing content-report control for corrections.
- Never pass ballot-lookup addresses or any user PII into the prompt — scope is passed as resolved district/election IDs only.

### 7. Caps, budget, cache (`limits.ts`)

- **Access tiers:** anonymous → retrieval-only (intents + search results). **LLM answers require a signed-in, verified account** at launch. Per-IP burst limiting reuses the existing limiter pattern.
- **Per-user daily cap:** Redis `INCR` with TTL, key from an HMAC of the user id used **only** here (transient, never logged). Redis is the free ephemeral instance — losing these counters on restart is harmless.
- **Global budget (durable, atomic):** single-row-per-day in `chatbot.daily_budget`. Two statements in one transaction — first `INSERT INTO chatbot.daily_budget (day) VALUES ($today) ON CONFLICT (day) DO NOTHING;` (a plain UPDATE alone would match no row on a fresh day and wrongly report "budget exhausted"), then `UPDATE ... SET tokens_reserved = tokens_reserved + $est WHERE day = $today AND tokens_reserved + $est <= $cap RETURNING ...` — reserve an estimated max **before** the call, reconcile with actual usage after. Concurrent requests cannot overshoot, and the counter survives restarts (the free Key Value store does not). Budget exhausted → retrieval-only for the rest of the day. Backstop: OpenAI project spend limit.
- **Exact answer cache:** Redis, TTL 24h, key = sha256(question_norm + scope_key + **generation_id + model + prompt_version**). Scope in the key means Seattle and Boston never share an entry; generation in the key means reindex = instant invalidation. Time-sensitive intents never reach this cache (they're deterministic). Concurrent-miss coalescing: deferred to Phase 4 (nice-to-have, not v1).

### 8. API + frontend

`POST /api/chatbot/ask` → `{answer, sources: [{title, url}], answeredBy, dataCurrentAsOf}`; paths added to `isKnownApiPath`; validated/rate-limited like existing endpoints; mounted only when `CHATBOT_ENABLED`. Frontend: one `ChatPage.tsx` + route + flag-guarded nav entry; single-turn Q&A UI at launch (no conversational promises). **Follow-up handling v1 is deterministic:** carry the previous turn's resolved candidate/election scope forward ("what about her voting record?" reuses the resolved candidate) — no LLM rewrite call in the MVP.

### 9. Question log + learning loop

- Fire-and-forget insert (redacted, anonymous) on every ask.
- `npm run chatbot:report`: rolls up into `chatbot.question_stats` with **write-time suppression** — a (week, question_norm) aggregate is only ever written when its weekly count is ≥ 5, so rare/unique question text never persists durably and can't be reconstructed after the 90-day purge; what survives is common, redacted, inherently non-identifying question text (that's the point: it's the candidate list for intent promotion). Lists top questions + refusals + cache hit rate + spend; deletes `question_norm` older than 90 days. Output drives intent promotion and content gaps.
- Privacy alignment (existing policy: civic activity is private; account deletion removes account data): the questions table holds **no user identifier**, so nothing links questions to accounts and account deletion is unaffected. UI note near the input: don't enter personal information.

## Behavior policy (prompt + intents + tests enforce)

No endorsements or vote recommendations · comparisons only across equivalent data fields · campaign claims attributed as claims · finance data never implies endorsement or influence · voting logistics only from official state resources · ambiguous questions get a clarifying question · unsupported questions get a clean refusal · every AI answer labeled, dated, citable, reportable.

## Phases (each a separate PR, shippable, disableable)

**Phase 0 — contract + golden set (small, no infra). DONE 2026-08-11:** `backend/src/chatbot/BEHAVIOR.md` (12-rule contract + release gates), `backend/src/chatbot/golden/goldenSet.ts` (66 cases across 12 categories, real Nov-2026 entities verified against local DB, incl. refusal/ambiguity/adversarial/follow-up), structural tests in `backend/tests/chatbot/goldenSet.test.ts`.

**Phase 1 — "Ask" (free, no LLM). BUILT 2026-08-11:** migration 234 (schema `chatbot`, halfvec chunks, exact scan — no HNSW), `backend/src/chatbot/` (chatbotConfig, embeddingsClient, chunker, indexer, retrieval, intents, redact, askService), scripts `chatbot:reindex` / `chatbot:report` / `chatbot:eval`, `POST /api/chatbot/ask` (404 when `CHATBOT_ENABLED=false`), frontend `/ask` page + flag-guarded nav (`VITE_CHATBOT_ENABLED`), TEI service documented (commented out) in render.yaml. Retrieval = 4 RRF branches (OR-lexical for ranking, exact-scan cosine, candidate-entity via `word_similarity`, election-title trgm); the answerability gate thresholds on RAW scores (strict AND-lexical, cosine 0.71, entity 0.75 — measured 2026-08-11 on the live local index). Deterministic extras that fell out of eval tuning: `untracked_data` (social posts) and `out_of_cycle` (non-2026 election years) refusal intents, `needs_scope` clarify intent, place-aware scope-ambiguity check. Indexer note: the reindex environment must load `backend/.env` finance read flags or the generation silently builds with ZERO finance chunks (bit us once). **Release gates measured 2026-08-11** (local index, 30,149 chunks / 6,315 elections / 2,368 finance summaries, hybrid): recall@5 **94%** (33/35; gate ≥85%), template routing 100%, refuse_policy 100%, clarify 100%, refuse_no_data 100%. The two recall misses are phrase-indirection cases (`finance-senate-most` — "the Georgia Senate race" outranks US Senate with State Senate districts; `followup-senate-republican-raised` — "the Republican candidate" has no name for the entity branch).

**Phase 2 — LLM answers (canary).** Adapter + Responses impl, structured citations + server validation, gate tuning on the golden set, exact cache, caps + durable budget, verified-users-only, small-percentage rollout, privacy-policy update **before** enabling. Effort starts low; medium only if golden-set evals show it helps.

**Phase 3 — quality + learning loop.** Intent promotion from stats, entity-resolution and gate tuning, adversarial/injection test cases in the golden set, citation-precision and refusal-precision checks, cost/latency dashboards from the log columns.

**Phase 4 — only with evidence of need.** Guarded LLM follow-up rewriting (if deterministic scope carry-over proves insufficient), semantic cache (similarity ≥0.97 + same scope + per-entry source refs), request coalescing, streaming, mobile entry.

## Explicitly rejected (research record in memory)

Self-hosted LLM (GPU $117+/mo; loses to APIs below ~1M queries/mo) · CPU inference (7–20s prompt reading) · serverless GPU (never idles at steady traffic) · AI frameworks (LlamaIndex.TS archived; plain SDK + adapter) · HyDE/multi-query expansion (hurts entity-heavy corpora) · Ollama embeddings (2–8× slower) · in-process Node embeddings (720 MB dep) · non-Render Postgres search extensions (built-ins reach ~84% retrieval accuracy) · fine-tuning (stales instantly) · LLM query rewriting in MVP (deterministic scope carry-over first).

## Risks

- **Hallucination/misattribution:** gate + structured citations validated server-side + server-built URLs + labels + report control. NIST AI 600-1 treats confident false generation as inherent; the mitigations are architectural, not prompt-only.
- **Cost runaway:** four layers — user cap, durable atomic budget, retrieval-only fallback, provider spend limit.
- **Prompt injection via indexed content:** model output is data (structured schema, escaped rendering, no model-authored URLs); adversarial cases in the golden set.
- **Embeddings down:** keyword-only degradation.
- **pgvector version:** verified 2026-08-11 — Render offers **0.8.1** (PG 18.4), local Postgres.app has 0.8.2; upstream is 0.8.6. Both below the ideal ≥0.8.4 (0.8.2–0.8.4 fixed HNSW build/vacuum corruption). Resolved by **not creating an HNSW index in v1** (external review correctly noted that a shared index across generations is still mutated by generation builds and retirement deletes + vacuum — "write-once rows" does not avoid those paths). Exact scan is accurate by definition and fast at our corpus size. Re-check `SELECT default_version FROM pg_available_extensions WHERE name='vector'` on Render occasionally; adopt HNSW (plus `hnsw.iterative_scan = relaxed_order`) only when corpus growth demands it and Render ships ≥0.8.4.
- **Privacy:** anonymous logs, redaction, 90-day purge, low-count suppression, policy update before LLM launch.
