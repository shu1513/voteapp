// Chatbot ("Ask") feature configuration — docs/plans/chatbot-rag.md.
//
// Isolation contract: everything chatbot lives under backend/src/chatbot/ and
// the `chatbot` Postgres schema. CHATBOT_ENABLED (default false) is the
// master kill switch — when off, runAddressApiServer never wires the ask
// handler, so the path 404s like any unknown path and nothing else changes.
// Phase 1 is retrieval-only: no LLM flags exist yet (they arrive with the
// Phase 2 adapter so dead config can't drift in the meantime).

export const CHATBOT_EMBEDDING_MODEL = "bge-small-en-v1.5";
export const CHATBOT_EMBEDDING_DIMS = 384;

export const DEFAULT_CHATBOT_EMBEDDINGS_TIMEOUT_MS = 10_000;

export type ChatbotConfig = {
  enabled: boolean;
  /** TEI service base URL (e.g. http://localhost:8080). Unset → keyword-only
   * retrieval (degraded mode); the API still boots and answers. */
  embeddingsUrl: string | null;
  embeddingsTimeoutMs: number;
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

export function readChatbotConfigFromEnv(env: NodeJS.ProcessEnv = process.env): ChatbotConfig {
  const enabledRaw = env.CHATBOT_ENABLED?.trim().toLowerCase() ?? "";
  const enabled = ["1", "true", "yes", "on"].includes(enabledRaw);
  // Kill-switch purity (API surface only): with the feature off, the rest of
  // the chatbot env is inert and must not be validated — a stale malformed
  // timeout var must not stop the whole API from booting.
  if (!enabled) {
    return { enabled: false, embeddingsUrl: null, embeddingsTimeoutMs: DEFAULT_CHATBOT_EMBEDDINGS_TIMEOUT_MS };
  }
  const embeddings = readChatbotEmbeddingsFromEnv(env);
  return { enabled, embeddingsUrl: embeddings.url, embeddingsTimeoutMs: embeddings.timeoutMs };
}
