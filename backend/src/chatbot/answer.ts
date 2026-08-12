// Phase 2 LLM answer step — docs/plans/chatbot-rag.md component 6.
//
// Runs ONLY after the retrieval answerability gate passed (below the gate the
// ask service refuses without ever reaching this file), and only for
// questions no deterministic intent claimed — so nothing time-sensitive or
// logistics-shaped can reach the cache or the model (BEHAVIOR.md rules 5/6).
//
// Every failure mode falls back to the Phase 1 retrieval cards: cache
// trouble, cap hit, budget exhausted, provider down, schema violation,
// invalid citations. The LLM path can only ever ADD an answer, never break
// the ask.

import type { Pool } from "pg";

import {
  answerCacheKey,
  consumeUserDailyAllowance,
  getCachedAnswer,
  hashUserId,
  reconcileDailyBudget,
  reserveDailyBudget,
  setCachedAnswer,
  type LimitsRedis,
} from "./limits.js";
import { LlmError, type LlmClient, type LlmUsage } from "./llm/adapter.js";
import { MAX_OUTPUT_TOKENS } from "./llm/openaiResponses.js";
import { CHATBOT_PROMPT_VERSION, SYSTEM_PROMPT, buildUserMessage } from "./llm/prompt.js";
import { REFUSAL_NO_DATA_ANSWER, chunkPageUrl, type AskResultCard } from "./shared.js";
import type { ChatbotLlmConfig } from "./chatbotConfig.js";
import type { RetrievedChunk } from "./retrieval.js";
import type { AskResponse } from "./askService.js";

export type LlmAnswering = {
  config: ChatbotLlmConfig;
  client: LlmClient;
  redis: LimitsRedis;
};

export type LlmStepResult =
  | {
      kind: "answered";
      response: AskResponse;
      tokensIn: number | null;
      tokensOut: number | null;
    }
  | {
      kind: "fallback";
      /** Why the caller should serve retrieval cards instead. rate_limited
       * is surfaced in the question log; the rest log as plain retrieval. */
      reason: "rate_limited" | "budget_exhausted" | "llm_failed" | "invalid_output";
      tokensIn: number | null;
      tokensOut: number | null;
    };

/** Everything that scopes an answer beyond its normalized text — part of the
 * exact-cache key so e.g. two states' "the senate race" never share an
 * entry. */
export function buildScopeKey(scopeState: string | null, context: { kind: string; id: string } | null): string {
  return `${scopeState ?? ""}|${context ? `${context.kind}:${context.id}` : ""}`;
}

// Pessimistic chars→tokens for the pre-call reservation (~4 chars/token in
// English; /3 over-reserves). Reconciled to actual usage after the call, so
// overshoot only ever briefly under-uses the budget, never overspends it.
function estimateCallTokens(question: string, chunks: readonly RetrievedChunk[]): number {
  const promptChars =
    SYSTEM_PROMPT.length + question.length + chunks.reduce((sum, c) => sum + c.title.length + c.content.length + 20, 0);
  return Math.ceil(promptChars / 3) + MAX_OUTPUT_TOKENS;
}

// Defense in depth (rule 9/10): the model is told not to emit URLs or
// markdown links; anything that slips through is stripped before the answer
// is stored or rendered. Sources are attached server-side only.
const URL_RE = /\bhttps?:\/\/\S+|\bwww\.\S+/gi;
const MARKDOWN_LINK_RE = /\[([^\]]*)\]\([^)]*\)/g;

export function sanitizeAnswerText(answer: string): string {
  return answer
    .replace(MARKDOWN_LINK_RE, "$1")
    .replace(URL_RE, "")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

/** Cached values are our own AskResponse JSON, but Redis content is still
 * validated structurally before being served (a truncated or foreign value
 * must miss, not crash). */
export function parseCachedAskResponse(value: unknown): AskResponse | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const record = value as Record<string, unknown>;
  if (
    typeof record.outcome !== "string" ||
    typeof record.answer !== "string" ||
    !Array.isArray(record.results) ||
    (record.data_current_as_of !== null && typeof record.data_current_as_of !== "string")
  ) {
    return null;
  }
  return value as AskResponse;
}

/** Pre-retrieval exact-cache lookup (the ask service checks it before paying
 * for retrieval + a query embedding). Same key the write side uses. */
export async function getCachedAskResponse(
  llm: LlmAnswering,
  parts: { questionNorm: string; scopeKey: string; generationId: string }
): Promise<AskResponse | null> {
  const key = answerCacheKey({
    questionNorm: parts.questionNorm,
    scopeKey: parts.scopeKey,
    generationId: parts.generationId,
    model: llm.config.model,
    promptVersion: CHATBOT_PROMPT_VERSION,
  });
  return parseCachedAskResponse(await getCachedAnswer(llm.redis, key));
}

export type AnswerWithLlmOptions = {
  db: Pool;
  llm: LlmAnswering;
  userId: string;
  /** The current question (raw), for the prompt. */
  question: string;
  /** Previous turn's question, ONLY when the ask service carried its scope
   * over (it then also participates in the cache key via questionNorm). */
  previousQuestion: string | null;
  /** normalizeQuestion() of the full retrieval text — the cache key's text
   * component. */
  questionNorm: string;
  scopeKey: string;
  generationId: string;
  generationActivatedAt: string;
  chunks: readonly RetrievedChunk[];
};

/** Source cards for the chunks the model actually cited, in retrieval rank
 * order, deduped per page — server-constructed URLs only. */
function toSourceCards(citedChunks: readonly RetrievedChunk[]): AskResultCard[] {
  const cards: AskResultCard[] = [];
  const seenUrls = new Set<string>();
  for (const chunk of citedChunks) {
    const url = chunkPageUrl(chunk);
    if (!url || seenUrls.has(url)) {
      continue;
    }
    seenUrls.add(url);
    cards.push({
      title: chunk.title,
      url,
      snippet: "Source from our election data.",
      source_type: chunk.sourceType,
    });
  }
  return cards;
}

/**
 * The full guarded LLM step: per-user cap → durable budget reservation →
 * adapter call → server-side validation → reconcile + cache write. The cache
 * READ happens earlier, in the ask service (getCachedAskResponse, before
 * retrieval is even paid for). Never throws.
 */
export async function answerWithLlm(options: AnswerWithLlmOptions): Promise<LlmStepResult> {
  const { db, llm, chunks } = options;
  const { config } = llm;

  const cacheKey = answerCacheKey({
    questionNorm: options.questionNorm,
    scopeKey: options.scopeKey,
    generationId: options.generationId,
    model: config.model,
    promptVersion: CHATBOT_PROMPT_VERSION,
  });

  // Per-user daily cap. The HMAC of the user id lives only in this scope —
  // never logged, never stored (BEHAVIOR.md rule 11).
  const hashedUserId = hashUserId(options.userId, config.apiKey);
  const underCap = await consumeUserDailyAllowance(llm.redis, hashedUserId, config.userDailyLimit);
  if (!underCap) {
    return { kind: "fallback", reason: "rate_limited", tokensIn: null, tokensOut: null };
  }

  // Durable global budget: reserve the worst case BEFORE the call.
  const estimatedTokens = estimateCallTokens(options.question, chunks);
  const reservation = await reserveDailyBudget(db, estimatedTokens, config.dailyTokenBudget);
  if (!reservation) {
    return { kind: "fallback", reason: "budget_exhausted", tokensIn: null, tokensOut: null };
  }

  let usage: LlmUsage;
  let rawAnswer: string;
  let rawCitations: string[];
  let refusalReason: string | null;
  try {
    const result = await llm.client.generateAnswer({
      question: options.previousQuestion
        ? `${options.question}\n(Previous question, for pronoun context only: ${options.previousQuestion})`
        : options.question,
      chunks: chunks.map((chunk) => ({ id: chunk.id, title: chunk.title, content: chunk.content })),
      safetyIdentifier: hashedUserId,
    });
    usage = result.usage;
    rawAnswer = result.answer;
    rawCitations = result.citations;
    refusalReason = result.refusalReason;
  } catch (error) {
    // Reconcile to whatever the provider actually billed (0 when unknown) so
    // failures do not eat the day's budget.
    const failedUsage = error instanceof LlmError ? error.usage : null;
    await reconcileDailyBudget(db, reservation, (failedUsage?.inputTokens ?? 0) + (failedUsage?.outputTokens ?? 0));
    console.warn(
      "chatbot LLM call failed; serving retrieval cards:",
      error instanceof Error ? error.message : String(error)
    );
    return {
      kind: "fallback",
      reason: "llm_failed",
      tokensIn: failedUsage?.inputTokens ?? null,
      tokensOut: failedUsage?.outputTokens ?? null,
    };
  }

  await reconcileDailyBudget(db, reservation, usage.inputTokens + usage.outputTokens);

  // Model-declared refusal: the chunks could not support an answer. Serve
  // the standard deterministic refusal copy (never model-authored refusal
  // prose) and cache it — the same question will keep refusing for free.
  if (refusalReason !== null) {
    const response: AskResponse = {
      outcome: "refuse_no_data",
      answer: REFUSAL_NO_DATA_ANSWER,
      results: [],
      data_current_as_of: null,
    };
    await setCachedAnswer(llm.redis, cacheKey, response);
    return { kind: "answered", response, tokensIn: usage.inputTokens, tokensOut: usage.outputTokens };
  }

  // Server-side citation validation (rule 9): only ids WE supplied count;
  // anything else is dropped. Zero surviving citations → the answer is not
  // verifiably grounded → cards, not prose.
  const suppliedById = new Map(chunks.map((chunk) => [chunk.id, chunk]));
  const citedIds = new Set(rawCitations.filter((id) => suppliedById.has(id)));
  const citedChunks = chunks.filter((chunk) => citedIds.has(chunk.id));
  const answer = sanitizeAnswerText(rawAnswer);
  if (answer.length === 0 || citedChunks.length === 0) {
    console.warn(
      `chatbot LLM output rejected (answer ${answer.length} chars, ${citedChunks.length} valid citations); serving retrieval cards`
    );
    return {
      kind: "fallback",
      reason: "invalid_output",
      tokensIn: usage.inputTokens,
      tokensOut: usage.outputTokens,
    };
  }

  const response: AskResponse = {
    outcome: "retrieval",
    answer,
    results: toSourceCards(citedChunks),
    data_current_as_of: options.generationActivatedAt,
    ai_generated: true,
  };
  await setCachedAnswer(llm.redis, cacheKey, response);
  return { kind: "answered", response, tokensIn: usage.inputTokens, tokensOut: usage.outputTokens };
}
