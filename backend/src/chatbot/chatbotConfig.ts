// Chatbot ("Ask") feature configuration — docs/plans/chatbot-rag.md.
//
// Isolation contract: everything chatbot lives under backend/src/chatbot/ and
// the `chatbot` Postgres schema. CHATBOT_ENABLED (default false) is the
// master kill switch — when off, runAddressApiServer never wires the ask
// handler, so the path 404s like any unknown path and nothing else changes.
// Phase 2 adds the LLM flags: CHATBOT_LLM_ENABLED (default false — money off
// by default) plus model/key/caps. LLM misconfigured or off → the Phase 1
// retrieval-card answers, unchanged.

export const CHATBOT_EMBEDDING_MODEL = "bge-small-en-v1.5";
export const CHATBOT_EMBEDDING_DIMS = 384;

export const DEFAULT_CHATBOT_EMBEDDINGS_TIMEOUT_MS = 10_000;

export const DEFAULT_CHATBOT_MODEL = "gpt-5.6-luna";
export const DEFAULT_CHATBOT_LLM_BASE_URL = "https://api.openai.com/v1";
export const DEFAULT_CHATBOT_REASONING_EFFORT = "low";
export const DEFAULT_CHATBOT_LLM_TIMEOUT_MS = 30_000;
export const DEFAULT_CHATBOT_USER_DAILY_LIMIT = 20;
export const DEFAULT_CHATBOT_DAILY_TOKEN_BUDGET = 5_000_000;

const REASONING_EFFORTS = ["minimal", "low", "medium", "high"] as const;
export type ChatbotReasoningEffort = (typeof REASONING_EFFORTS)[number];

export type ChatbotLlmConfig = {
  model: string;
  baseUrl: string;
  /** Separate OpenAI key in its own project with a dashboard spend limit —
   * never the pipeline keys, never subject to aiCallGuard (user-triggered
   * spending with its own flag + cap + budget guards). */
  apiKey: string;
  reasoningEffort: ChatbotReasoningEffort;
  timeoutMs: number;
  /** LLM answers per signed-in user per day (Redis counter). */
  userDailyLimit: number;
  /** Global durable daily token budget (chatbot.daily_budget). */
  dailyTokenBudget: number;
};

export type ChatbotConfig = {
  enabled: boolean;
  /** TEI service base URL (e.g. http://localhost:8080). Unset → keyword-only
   * retrieval (degraded mode); the API still boots and answers. */
  embeddingsUrl: string | null;
  embeddingsTimeoutMs: number;
  /** Non-null only when CHATBOT_LLM_ENABLED=true AND an API key is set;
   * anything else means retrieval-only answers. */
  llm: ChatbotLlmConfig | null;
};

export type ChatbotEmbeddingsConfig = {
  url: string | null;
  timeoutMs: number;
};

/** Embeddings settings alone, independent of CHATBOT_ENABLED: the operator
 * scripts (reindex/eval) use the TEI service regardless of whether the API
 * surface is switched on. */
export function readChatbotEmbeddingsFromEnv(env: NodeJS.ProcessEnv = process.env): ChatbotEmbeddingsConfig {
  const rawUrl = env.CHATBOT_EMBEDDINGS_URL?.trim() || null;
  const timeoutRaw = env.CHATBOT_EMBEDDINGS_TIMEOUT_MS?.trim();
  let timeoutMs = DEFAULT_CHATBOT_EMBEDDINGS_TIMEOUT_MS;
  if (timeoutRaw) {
    // Digits-only before conversion: parseInt would accept "250ms" as 250
    // and "1.5" as 1, silently masking a config typo.
    const parsed = /^\d+$/.test(timeoutRaw) ? Number(timeoutRaw) : Number.NaN;
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      throw new Error(`Invalid CHATBOT_EMBEDDINGS_TIMEOUT_MS: ${timeoutRaw}`);
    }
    timeoutMs = parsed;
  }
  return { url: rawUrl ? rawUrl.replace(/\/+$/, "") : null, timeoutMs };
}

function isTruthyFlag(value: string | undefined): boolean {
  return ["1", "true", "yes", "on"].includes(value?.trim().toLowerCase() ?? "");
}

function readPositiveInteger(raw: string | undefined, name: string, fallback: number): number {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return fallback;
  }
  // Digits-only before conversion: parseInt would accept "20x" as 20,
  // silently masking a config typo (same policy as the embeddings timeout).
  const parsed = /^\d+$/.test(trimmed) ? Number(trimmed) : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${trimmed}`);
  }
  return parsed;
}

/** LLM settings, or null when the LLM path must stay off (flag unset or key
 * missing — money defaults off; docs/plans/chatbot-rag.md flag table). */
function readChatbotLlmFromEnv(env: NodeJS.ProcessEnv): ChatbotLlmConfig | null {
  if (!isTruthyFlag(env.CHATBOT_LLM_ENABLED)) {
    return null;
  }
  const apiKey = env.CHATBOT_LLM_API_KEY?.trim();
  if (!apiKey) {
    // Fail closed to retrieval-only, loudly: a booting API with a half-set
    // LLM config should say why generated answers are absent.
    console.warn("CHATBOT_LLM_ENABLED is set but CHATBOT_LLM_API_KEY is missing; LLM answers stay off");
    return null;
  }
  const effortRaw = env.CHATBOT_REASONING_EFFORT?.trim().toLowerCase() || DEFAULT_CHATBOT_REASONING_EFFORT;
  if (!REASONING_EFFORTS.includes(effortRaw as ChatbotReasoningEffort)) {
    throw new Error(`Invalid CHATBOT_REASONING_EFFORT: ${effortRaw} (expected one of ${REASONING_EFFORTS.join(", ")})`);
  }
  return {
    model: env.CHATBOT_MODEL?.trim() || DEFAULT_CHATBOT_MODEL,
    baseUrl: (env.CHATBOT_LLM_BASE_URL?.trim() || DEFAULT_CHATBOT_LLM_BASE_URL).replace(/\/+$/, ""),
    apiKey,
    reasoningEffort: effortRaw as ChatbotReasoningEffort,
    timeoutMs: readPositiveInteger(env.CHATBOT_LLM_TIMEOUT_MS, "CHATBOT_LLM_TIMEOUT_MS", DEFAULT_CHATBOT_LLM_TIMEOUT_MS),
    userDailyLimit: readPositiveInteger(
      env.CHATBOT_USER_DAILY_LIMIT,
      "CHATBOT_USER_DAILY_LIMIT",
      DEFAULT_CHATBOT_USER_DAILY_LIMIT
    ),
    dailyTokenBudget: readPositiveInteger(
      env.CHATBOT_DAILY_TOKEN_BUDGET,
      "CHATBOT_DAILY_TOKEN_BUDGET",
      DEFAULT_CHATBOT_DAILY_TOKEN_BUDGET
    ),
  };
}

export function readChatbotConfigFromEnv(env: NodeJS.ProcessEnv = process.env): ChatbotConfig {
  const enabled = isTruthyFlag(env.CHATBOT_ENABLED);
  // Kill-switch purity (API surface only): with the feature off, the rest of
  // the chatbot env is inert and must not be validated — a stale malformed
  // timeout var must not stop the whole API from booting.
  if (!enabled) {
    return {
      enabled: false,
      embeddingsUrl: null,
      embeddingsTimeoutMs: DEFAULT_CHATBOT_EMBEDDINGS_TIMEOUT_MS,
      llm: null,
    };
  }
  const embeddings = readChatbotEmbeddingsFromEnv(env);
  return {
    enabled,
    embeddingsUrl: embeddings.url,
    embeddingsTimeoutMs: embeddings.timeoutMs,
    llm: readChatbotLlmFromEnv(env),
  };
}
