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
2. **Automated 90-day purge (compliance).** Privacy policy promises question
   text deleted after 90 days; today the purge only runs inside manual
   `chatbot:report`, and Render cron requires a paid plan (blocked on billing,
   same as the digest cron). Fix: in-API opportunistic purge — once per UTC day,
   guarded by Redis SETNX, fire-and-forget after an ask. Needs a migration:
   column-level `GRANT UPDATE (question_norm) ON chatbot.questions TO
   voteapp_api` (role is INSERT-only today). Manual `chatbot:report` keeps
   doing the same purge; both are idempotent.
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
are refusal counts and the heavyweight content-report.

- Migration: `chatbot.answer_feedback` (id, asked_at, answered_by, verdict,
  question log row id nullable) or a nullable `feedback` column on
  `chatbot.questions`; decide in the PR. No user identifier, matching the
  question-log privacy model. `voteapp_api` gets the minimal grant.
- API: ask response carries an opaque feedback token (the question log row id,
  HMAC-wrapped so it is not enumerable); `POST /api/chatbot/feedback` accepts
  token + up/down. Rate-limited like other endpoints.
- Widget: two small buttons under each answer; one-shot, no undo UI.
- Report: downvote rate per `answered_by` — the Phase 3 canary metric.

## PR 3 — Widget UX (frontend only)

1. **A11y:** focus input on open, Escape to minimize, `aria-live="polite"` on
   the answer/pending region, focus restore to the launcher on close.
2. **Post-answer follow-up chips.** Deterministic next questions derived from
   the cited cards' `source_type` (profile cited → "Who is funding their
   campaign?"; election cited → "Who is running?"). Pure frontend function, no
   server change, drives traffic to free paths.
3. **Honest rate-limit copy.** `rate_limited` fallback currently serves cards
   silently. Ask response gains an optional `notice` field (deterministic
   server copy: "Daily AI-answer limit reached — showing matching data
   instead."); widget renders it as a muted line. No policy risk: static copy,
   not model output.

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
