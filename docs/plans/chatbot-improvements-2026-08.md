# Chatbot ("Ask") improvements — plan

Status: PLAN (2026-08-17). Parent: `docs/plans/chatbot-rag.md` (Phases 1–2 built, LIVE IN PROD).
Sources: internal deep review + external (Codex) review, both 2026-08-17. Items each
review raised that we judged overengineering are listed at the bottom as rejected,
so they are not re-proposed later.

## Principles

- No architecture changes. Every item rides the existing pipeline, isolation
  contract, and BEHAVIOR.md rules.
- Money stays guarded: nothing here adds LLM calls or new spend paths.
- Each PR small, shippable, and disableable on its own.

## PR 1 — Ops correctness (backend only, no user-visible change)

1. **Eval stops polluting analytics.** `chatbotEval.ts` runs the real ask
   service, which always fire-and-forget logs to `chatbot.questions` (and could
   write the answer cache if ever pointed at prod). Add
   `createAskService({ logQuestions: false })`; eval passes it. ~5 lines + test.
2. **Automated 90-day retention (compliance).** Privacy policy promises
   question text deleted after 90 days; before this the purge only ran inside
   manual `chatbot:report`, and Render cron requires a paid plan (blocked on
   billing, same as the digest cron). Shipped shape (revised after review):
   - Migration 241: `chatbot.roll_up_and_purge_questions()` SECURITY DEFINER
     (EXECUTE-only to `voteapp_api`; role stays unable to read the log). One
     statement does the write-time-suppressed rollup into `question_stats`
     THEN nulls >90-day text — same snapshot, so automated purging can never
     destroy text before it is aggregated.
   - API runs it via boot-time kick + hourly `setInterval` (unref'd), elected
     once per UTC day with Redis SET NX; a DB failure releases the day's
     guard so a later tick retries. Timer, not ask-piggybacking: covers
     "site traffic but no asks", and any ask implies the process is up.
     NOT behind `CHATBOT_ENABLED` — retention applies to rows logged while
     the feature was on even after the kill switch turns it off; teardown
     (DROP SCHEMA) removes data and obligation together.
   - `chatbot:report` calls the same function (single implementation).
3. **Report enrichment.** Add to `chatbot:report` JSON: token sums (in/out, 7
   days), today's budget consumed vs `CHATBOT_DAILY_TOKEN_BUDGET`, p50/p95
   latency by `answered_by`, active generation age in days. All from existing
   columns — no schema change. (Outcome mix incl. cache/llm/fallback rates
   already exists; degraded-mode counter deliberately skipped — it is not
   logged and a schema change is not worth the signal.)
4. **Docs truth.** Fix stale "Nothing built yet" header in
   `docs/plans/chatbot-rag.md`; one authoritative launch-status line.

## PR 2 — Feedback signal (the biggest quality win)

Anonymous 👍/👎 per answer — closes the learning loop: today the only signals
are refusal counts and the heavyweight content-report. Shipped shape (the
open decisions, resolved):

- Migration 242: `chatbot.answer_feedback` as its own TABLE (id, created_at,
  answered_by, verdict, token_nonce UNIQUE) — a column on `chatbot.questions`
  would need an UPDATE grant on the log the API role deliberately lacks. No
  user identifier and no question-row id: question logging is fire-and-forget
  with no returned id, and `created_at` + `answered_by` are all the report
  needs. `voteapp_api` gets INSERT only.
- API: every ask response carries `feedback_token` — a STATELESS HMAC-signed
  (answered_by, nonce) payload, not a wrapped row id (none exists; see
  `chatbot/feedback.ts`). Signed with CHATBOT_FEEDBACK_SECRET (render.yaml
  generateValue) — must be stable because the free plan spins the API down
  when idle and a per-boot secret would invalidate tokens across every
  wake-up (review catch); unset → per-boot fallback with a boot warning.
  Minted after the answer cache, so cache hits vote on their own
  token. `POST /api/chatbot/feedback` (token + up/down) is verified-accounts
  gated like ask, covered by the global IP rate limiter, 404 when
  CHATBOT_ENABLED is off. The UNIQUE nonce makes each token one-shot
  server-side (duplicates answer ok, first verdict stands).
- Widget: two small buttons under each answer; one-shot, no undo UI. Thanks
  copy only after the server confirms (review catch: optimistic thanks lost
  votes silently); transient failure reverts to the buttons (= the retry
  control), a 400 (token died with a restart) gives up honestly — retrying
  a rejected token can never succeed.
- Report: downvote rate per `answered_by` — the Phase 3 canary metric.

## PR 3 — Widget UX

Shipped shape:

1. **A11y:** focus moves into the question input on open (no-op on the
   register/verify walls — no input), Escape anywhere in the panel minimizes,
   focus returns to the launcher bubble on close (guarded so the initial
   mount never steals focus), and answers/pending/errors sit in an
   `aria-live="polite"` region. Follow-up chips render OUTSIDE the live
   region — suggestion buttons must not be read out as answer text.
2. **Post-answer follow-up chips.** `followUpQuestions()` (pure, exported,
   ChatWidget.tsx) maps the latest answer's cited `source_type`s to deictic
   next questions: `candidate_profile` → "Who is funding their campaign?"
   (suppressed when `finance_summary` is also cited — it was just answered),
   `election` → "Who is running in this election?". The question just asked
   is never re-suggested (chips would loop). Deictic phrasing rides the
   existing context/previous-question carry — no server change.
3. **Honest limit copy.** Ask response gained an optional `notice` field.
   `fallbackNotice()` (askService.ts) sets "Daily AI-answer limit reached —
   showing matching data instead." for the `rate_limited` AND
   `budget_exhausted` fallbacks (from the user's seat both mean "no AI answer
   today"); `llm_failed`/`invalid_output` stay silent on purpose — transient
   faults where the cards are the best next answer. Never cached (the cards
   path is never cached) and never model output. Widget renders it as a muted
   italic line between the answer and its cards.

## PR 4 — Retrieval tuning (timeboxed)

Fix the two known recall misses from the release-gate run
(`finance-senate-most`: "Georgia Senate race" outranked by State-Senate
districts; `followup-senate-republican-raised`: "the Republican candidate" has
no name for the entity branch). Eval-driven; stop if thresholds start
regressing other cases. Golden set additions welcome; gates re-run per
BEHAVIOR.md before ship.

## Scheduled, not now

- **Reindex cadence (accuracy).** Nightly cron blocked on Render billing.
  Interim policy: reindex after each prod data promotion, weekly floor;
  generation age in the report (PR 1.3) is the alert. Revisit real cron when
  billing unblocks. `source_updated_at` stays unwired (snapshot time is the
  honest "data current as of"; the dead column is harmless).
- **Election-results answers (~October 2026).** Deterministic entity-resolved
  "who won X" template reading live canonical result fields (never cache/LLM —
  rule 6). Matters Nov 4+; current generic template fine until then.

## Rejected (do not re-propose without new evidence)

- **Multi-turn scope echo** (server-resolved scope returned to client): parent
  plan gates this on evidence of need (Phase 4). Hidden hard part is when to
  DROP the echoed scope, plus cache-key interplay. Wait for feedback/log
  evidence (PR 2 provides exactly that).
- **Context-bypass tightening** (whitelist of "supported context asks"): the
  deictic design is deliberate and documented; worst case is cards about the
  page the user is already viewing. A classifier here is brittle and kills
  legit long-tail deictic questions. Add adversarial golden cases instead if
  concern persists.
- **Report-attribution chooser** (comparison answers attach report to first
  cited entity only): cosmetic misfile; reports still arrive labeled "this AI
  answer". Not worth new UI or a report type.
- **`source_updated_at` wiring / per-chunk freshness display**: complexity, no
  user value.
- **Streaming, semantic cache, LLM query rewrite, mobile entry**: already
  rejected/deferred in the parent plan; unchanged.

## Order + sizes

PR 1 (S) → PR 2 (M) → PR 3 (S) → PR 4 (S, timeboxed). PRs 1–3 independent of
each other except report downvote-rate (PR 2 → its report line lands with PR 2).
