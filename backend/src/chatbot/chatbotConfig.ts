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

export const DEFAULT_CHATBOT_LLM_BASE_URL = "https://api.openai.com/v1";
export const DEFAULT_CHATBOT_REASONING_EFFORT = "low";
export const DEFAULT_CHATBOT_LLM_TIMEOUT_MS = 30_000;
export const DEFAULT_CHATBOT_USER_DAILY_LIMIT = 20;
export const DEFAULT_CHATBOT_DAILY_TOKEN_BUDGET = 5_000_000;

// Current OpenAI reasoning models document none/low/medium/high/xhigh/max
// ("minimal" is legacy — it would pass local validation and then fail at
// the provider on every request, silently draining the daily budget through
// kept unknown-usage reservations). xhigh/max are deliberately excluded
// here as a cost ceiling: raising past high is a conscious code change,
// and a locally rejected value fails LOUD at boot instead.
const REASONING_EFFORTS = ["none", "low", "medium", "high"] as const;
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

export const DEFAULT_CHATBOT_EMBEDDINGS_PORT = 8080;

/** Embeddings settings alone, independent of CHATBOT_ENABLED: the operator
 * scripts (reindex/eval) use the TEI service regardless of whether the API
 * surface is switched on.
 *
 * Two ways to point at the service:
 *   - CHATBOT_EMBEDDINGS_URL — full URL (local dev, non-Render hosts). A
 *     scheme-less host:port is accepted (http assumed). Wins when both set.
 *   - CHATBOT_EMBEDDINGS_HOST [+ CHATBOT_EMBEDDINGS_PORT, default 8080] —
 *     Render deploys: the blueprint injects the GENERATED private hostname
 *     (fromService property: host — hardcoding it would break on service
 *     recreation), while the port is ours to pin: the same render.yaml sets
 *     it in dockerCommand. hostport is deliberately not used — observed
 *     2026-08-12 resolving a stale reserved :10000 while the app listened
 *     on 8080. */
export function readChatbotEmbeddingsFromEnv(env: NodeJS.ProcessEnv = process.env): ChatbotEmbeddingsConfig {
  let rawUrl = env.CHATBOT_EMBEDDINGS_URL?.trim() || null;
  if (!rawUrl) {
    const host = env.CHATBOT_EMBEDDINGS_HOST?.trim();
    if (host) {
      const port = readPositiveInteger(
        env.CHATBOT_EMBEDDINGS_PORT,
        "CHATBOT_EMBEDDINGS_PORT",
        DEFAULT_CHATBOT_EMBEDDINGS_PORT
      );
      rawUrl = `${host}:${port}`;
    }
  }
  // Private-network traffic is plain HTTP; a scheme-less value means http.
  if (rawUrl && !/^https?:\/\//i.test(rawUrl)) {
    rawUrl = `http://${rawUrl}`;
  }
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
  // No in-repo default ON PURPOSE: the model choice is deployment
  // configuration (env/dashboard only), so the codebase never reveals which
  // model runs in production and swapping it is a config change, not a PR.
  // TIER_FREE because every account answers on the free-tier model today; a
  // future paid tier adds its own variable alongside (plus routing code).
  const model = env.CHATBOT_MODEL_TIER_FREE?.trim();
  if (!model) {
    console.warn("CHATBOT_LLM_ENABLED is set but CHATBOT_MODEL_TIER_FREE is missing; LLM answers stay off");
    return null;
  }
  const effortRaw = env.CHATBOT_REASONING_EFFORT?.trim().toLowerCase() || DEFAULT_CHATBOT_REASONING_EFFORT;
  if (!REASONING_EFFORTS.includes(effortRaw as ChatbotReasoningEffort)) {
    throw new Error(`Invalid CHATBOT_REASONING_EFFORT: ${effortRaw} (expected one of ${REASONING_EFFORTS.join(", ")})`);
  }
  return {
    model,
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
