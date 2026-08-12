import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import {
  answerWithLlm,
  buildScopeKey,
  getCachedAskResponse,
  parseCachedAskResponse,
  sanitizeAnswerText,
  type LlmAnswering,
} from "../../src/chatbot/answer.js";
import { LlmError, type GenerateAnswerResult, type LlmClient } from "../../src/chatbot/llm/adapter.js";
import { REFUSAL_NO_DATA_ANSWER } from "../../src/chatbot/shared.js";
import type { ChatbotLlmConfig } from "../../src/chatbot/chatbotConfig.js";
import type { LimitsRedis } from "../../src/chatbot/limits.js";
import type { RetrievedChunk } from "../../src/chatbot/retrieval.js";

// answerWithLlm against fully faked db/redis/adapter — the guarded pipeline
// (cap → budget → call → validation → reconcile + cache) without any
// network, database, or spend.

const CONFIG: ChatbotLlmConfig = {
  model: "gpt-5.6-luna",
  baseUrl: "https://api.example.test/v1",
  apiKey: "test-key",
  reasoningEffort: "low",
  timeoutMs: 5_000,
  userDailyLimit: 20,
  dailyTokenBudget: 1_000_000,
};

function chunk(id: string, sourceType: string, sourceId: string, title: string): RetrievedChunk {
  return {
    id,
    sourceType,
    sourceId,
    electionId: null,
    state: "GA",
    title,
    content: `${title} content.`,
    evidenceUrls: [],
    lexicalScore: 0.5,
    cosineSimilarity: 0.8,
    rrfScore: 0.03,
  };
}

const CHUNKS = [
  chunk("101", "candidate_profile", "3d1f8a52-0000-4000-8000-000000000001", "Jon Ossoff — profile"),
  chunk("102", "finance_summary", "3d1f8a52-0000-4000-8000-000000000001", "Jon Ossoff — campaign finance"),
  chunk("103", "election", "9b3a0d8e-0000-4000-8000-000000000002", "Georgia US Senate"),
];

function fakeRedis(): LimitsRedis & { store: Map<string, string> } {
  const store = new Map<string, string>();
  const counters = new Map<string, number>();
  return {
    store,
    async incr(key) {
      const next = (counters.get(key) ?? 0) + 1;
      counters.set(key, next);
      return next;
    },
    async expire() {
      return 1;
    },
    async get(key) {
      return store.get(key) ?? null;
    },
    async set(key, value) {
      store.set(key, value);
      return "OK";
    },
  };
}

/** Budget-transaction fake: reservation always fits unless capacity=false;
 * records reconcile calls made through pool.query. */
function fakePool(hasCapacity = true): { pool: Pool; reconciles: unknown[][] } {
  const reconciles: unknown[][] = [];
  const client = {
    query: async (text: string) => {
      if (text.includes("UPDATE chatbot.daily_budget")) {
        return { rowCount: hasCapacity ? 1 : 0, rows: [] };
      }
      return { rowCount: 0, rows: [] };
    },
    release: () => undefined,
  };
  const pool = {
    connect: async () => client,
    query: vi.fn(async (text: string, values?: unknown[]) => {
      if (text.includes("UPDATE chatbot.daily_budget")) {
        reconciles.push(values ?? []);
      }
      return { rowCount: 1, rows: [] };
    }),
  } as unknown as Pool;
  return { pool, reconciles };
}

function llmReturning(result: GenerateAnswerResult | Error): LlmClient {
  return {
    generateAnswer: vi.fn(async () => {
      if (result instanceof Error) {
        throw result;
      }
      return result;
    }),
  };
}

function makeOptions(overrides: Partial<Parameters<typeof answerWithLlm>[0]> = {}) {
  const { pool } = fakePool();
  const llm: LlmAnswering = {
    config: CONFIG,
    client: llmReturning({
      answer: "Jon Ossoff is a US Senator from Georgia.",
      citations: ["101"],
      refusalReason: null,
      usage: { inputTokens: 1000, outputTokens: 150 },
    }),
    redis: fakeRedis(),
  };
  return {
    db: pool,
    llm,
    userId: "3d1f8a52-0000-4000-8000-00000000000a",
    question: "Who is Jon Ossoff?",
    previousQuestion: null,
    questionNorm: "who is jon ossoff",
    scopeKey: "GA|",
    generationId: "9b3a0d8e-0000-4000-8000-000000000001",
    generationActivatedAt: "2026-08-12T00:00:00Z",
    chunks: CHUNKS,
    ...overrides,
  };
}

describe("answerWithLlm", () => {
  it("returns a labeled, cited, dated answer and caches it", async () => {
    const options = makeOptions();
    const step = await answerWithLlm(options);
    expect(step.kind).toBe("answered");
    if (step.kind !== "answered") {
      return;
    }
    expect(step.response.outcome).toBe("retrieval");
    expect(step.response.ai_generated).toBe(true);
    expect(step.response.answer).toBe("Jon Ossoff is a US Senator from Georgia.");
    expect(step.response.data_current_as_of).toBe("2026-08-12T00:00:00Z");
    // Sources: only the CITED chunk, URL server-constructed from metadata.
    expect(step.response.results).toHaveLength(1);
    expect(step.response.results[0]?.url).toBe("/candidates/3d1f8a52-0000-4000-8000-000000000001");
    expect(step.tokensIn).toBe(1000);
    expect(step.tokensOut).toBe(150);
    // Cached for the exact-cache read path.
    const cached = await getCachedAskResponse(options.llm, {
      questionNorm: options.questionNorm,
      scopeKey: options.scopeKey,
      generationId: options.generationId,
    });
    expect(cached).toEqual(step.response);
  });

  it("drops citations for ids we never supplied and keeps valid ones", async () => {
    const options = makeOptions();
    options.llm.client = llmReturning({
      answer: "Grounded answer.",
      citations: ["101", "999", "103"],
      refusalReason: null,
      usage: { inputTokens: 900, outputTokens: 100 },
    });
    const step = await answerWithLlm(options);
    expect(step.kind).toBe("answered");
    if (step.kind !== "answered") {
      return;
    }
    expect(step.response.results.map((card) => card.url)).toEqual([
      "/candidates/3d1f8a52-0000-4000-8000-000000000001",
      "/elections/9b3a0d8e-0000-4000-8000-000000000002",
    ]);
  });

  it("falls back (invalid_output) when zero supplied ids were cited", async () => {
    const options = makeOptions();
    options.llm.client = llmReturning({
      answer: "Suspicious uncited answer.",
      citations: ["999"],
      refusalReason: null,
      usage: { inputTokens: 900, outputTokens: 100 },
    });
    const step = await answerWithLlm(options);
    expect(step).toMatchObject({ kind: "fallback", reason: "invalid_output" });
    // Not cached: the next ask retries fresh.
    expect((options.llm.redis as unknown as { store: Map<string, string> }).store.size).toBe(0);
  });

  it("serves the standard refusal copy (never model prose) on a model refusal, and caches it", async () => {
    const options = makeOptions();
    options.llm.client = llmReturning({
      answer: "",
      citations: [],
      refusalReason: "chunks do not cover this",
      usage: { inputTokens: 800, outputTokens: 50 },
    });
    const step = await answerWithLlm(options);
    expect(step.kind).toBe("answered");
    if (step.kind !== "answered") {
      return;
    }
    expect(step.response.outcome).toBe("refuse_no_data");
    expect(step.response.answer).toBe(REFUSAL_NO_DATA_ANSWER);
    expect(step.response.ai_generated).toBeUndefined();
    expect(step.response.results).toEqual([]);
    const cached = await getCachedAskResponse(options.llm, {
      questionNorm: options.questionNorm,
      scopeKey: options.scopeKey,
      generationId: options.generationId,
    });
    expect(cached?.outcome).toBe("refuse_no_data");
  });

  it("falls back (rate_limited) once the user's daily allowance is spent, without calling the LLM", async () => {
    const options = makeOptions();
    options.llm.config = { ...CONFIG, userDailyLimit: 1 };
    await answerWithLlm(options); // spends the single allowance
    (options.llm.redis as unknown as { store: Map<string, string> }).store.clear(); // drop the cached answer
    const step = await answerWithLlm(options);
    expect(step).toMatchObject({ kind: "fallback", reason: "rate_limited" });
    expect(options.llm.client.generateAnswer).toHaveBeenCalledTimes(1);
  });

  it("falls back (budget_exhausted) when the reservation does not fit, without calling the LLM", async () => {
    const { pool } = fakePool(false);
    const options = makeOptions({ db: pool });
    const step = await answerWithLlm(options);
    expect(step).toMatchObject({ kind: "fallback", reason: "budget_exhausted" });
    expect(options.llm.client.generateAnswer).not.toHaveBeenCalled();
  });

  it("falls back (llm_failed) on an adapter error and reconciles the failed call's real usage", async () => {
    const { pool, reconciles } = fakePool();
    const options = makeOptions({ db: pool });
    options.llm.client = llmReturning(
      new LlmError("truncated", { usage: { inputTokens: 700, outputTokens: 1200 } })
    );
    const step = await answerWithLlm(options);
    expect(step).toMatchObject({ kind: "fallback", reason: "llm_failed", tokensIn: 700, tokensOut: 1200 });
    expect(reconciles).toHaveLength(1);
    expect(reconciles[0]?.[2]).toBe(1900); // actual tokens replace the estimate
  });

  it("keeps the pessimistic reservation when a failure has UNKNOWN usage (timeout may still have billed)", async () => {
    const { pool, reconciles } = fakePool();
    const options = makeOptions({ db: pool });
    options.llm.client = llmReturning(new LlmError("LLM service unreachable: timeout"));
    const step = await answerWithLlm(options);
    expect(step).toMatchObject({ kind: "fallback", reason: "llm_failed", tokensIn: null, tokensOut: null });
    // No reconcile: releasing the reservation on repeated timeouts would let
    // real provider spend exceed the internal daily budget.
    expect(reconciles).toHaveLength(0);
  });

  it("reconciles the reservation down to actual usage on success", async () => {
    const { pool, reconciles } = fakePool();
    const options = makeOptions({ db: pool });
    await answerWithLlm(options);
    expect(reconciles).toHaveLength(1);
    expect(reconciles[0]?.[2]).toBe(1150);
    expect(reconciles[0]?.[1]).toBeGreaterThan(1150); // pessimistic estimate above actual
  });
});

describe("sanitizeAnswerText", () => {
  it("strips model-written URLs and markdown links but keeps the prose", () => {
    expect(
      sanitizeAnswerText("See [his page](https://evil.test/x) at https://evil.test/y or www.evil.test now.")
    ).toBe("See his page at or now.");
  });

  it("passes ordinary prose through unchanged", () => {
    const text = "Jon Ossoff (D) is running for US Senate in Georgia.";
    expect(sanitizeAnswerText(text)).toBe(text);
  });
});

describe("parseCachedAskResponse", () => {
  it("accepts a valid response and rejects malformed values", () => {
    const valid = { outcome: "retrieval", answer: "a", results: [], data_current_as_of: null };
    expect(parseCachedAskResponse(valid)).toEqual(valid);
    expect(parseCachedAskResponse(null)).toBeNull();
    expect(parseCachedAskResponse("string")).toBeNull();
    expect(parseCachedAskResponse({ outcome: 1, answer: "a", results: [], data_current_as_of: null })).toBeNull();
    expect(parseCachedAskResponse({ outcome: "retrieval", answer: "a", data_current_as_of: null })).toBeNull();
  });
});

describe("buildScopeKey", () => {
  it("separates state and context so different scopes never collide", () => {
    expect(buildScopeKey(null, null)).toBe("|");
    expect(buildScopeKey("GA", null)).toBe("GA|");
    expect(buildScopeKey(null, { kind: "candidate", id: "x" })).toBe("|candidate:x");
    expect(buildScopeKey("GA", { kind: "election", id: "y" })).toBe("GA|election:y");
  });
});
