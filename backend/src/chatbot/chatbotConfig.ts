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

export function readChatbotConfigFromEnv(env: NodeJS.ProcessEnv = process.env): ChatbotConfig {
  const enabledRaw = env.CHATBOT_ENABLED?.trim().toLowerCase() ?? "";
  const enabled = ["1", "true", "yes", "on"].includes(enabledRaw);
  const embeddingsUrl = env.CHATBOT_EMBEDDINGS_URL?.trim() || null;
  const timeoutRaw = env.CHATBOT_EMBEDDINGS_TIMEOUT_MS?.trim();
  let embeddingsTimeoutMs = DEFAULT_CHATBOT_EMBEDDINGS_TIMEOUT_MS;
  if (timeoutRaw) {
    const parsed = Number.parseInt(timeoutRaw, 10);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new Error(`Invalid CHATBOT_EMBEDDINGS_TIMEOUT_MS: ${timeoutRaw}`);
    }
    embeddingsTimeoutMs = parsed;
  }
  return { enabled, embeddingsUrl: embeddingsUrl ? embeddingsUrl.replace(/\/+$/, "") : null, embeddingsTimeoutMs };
}
