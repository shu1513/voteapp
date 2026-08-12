// The LLM adapter boundary (docs/plans/chatbot-rag.md component 6 + "Provider
// swapping"). Everything provider-specific — request shape, reasoning params,
// usage reporting — stays inside backend/src/chatbot/llm/; the rest of the
// chatbot only sees generateAnswer(). Swapping providers = a new ~50-line
// implementation of LlmClient, nothing else.
//
// NOT covered by aiCallGuard (deliberately): that guard protects unattended
// pipeline spending. This is user-triggered spending behind its own guards
// (CHATBOT_LLM_ENABLED + per-user cap + durable daily budget + provider
// dashboard spend limit). Never set AI_API_CALLS_ALLOWED for the chatbot.

/** One retrieved chunk, reduced to what the model needs. Ids are the
 * chatbot.chunks bigserial ids as strings — the citation currency. */
export type LlmChunk = {
  id: string;
  title: string;
  content: string;
};

export type LlmUsage = {
  /** Prompt + reasoning + output token totals as the provider reports them —
   * what the daily budget reconciles against. */
  inputTokens: number;
  outputTokens: number;
};

export type GenerateAnswerResult = {
  /** Model answer text. Empty when refusing. UNVALIDATED — the caller
   * (answer.ts) strips URLs and escapes; never render this raw. */
  answer: string;
  /** Chunk ids the model claims to have used. UNVALIDATED — the caller drops
   * every id not in the supplied chunk set. */
  citations: string[];
  /** Non-null when the model judged the chunks insufficient (BEHAVIOR.md
   * rule 8: clean refusal, never a guess). */
  refusalReason: string | null;
  usage: LlmUsage;
};

export type GenerateAnswerInput = {
  question: string;
  chunks: readonly LlmChunk[];
  /** HMAC of the user id (limits.ts), forwarded as the provider's abuse
   * `safety_identifier`. Never a raw user id, never logged. */
  safetyIdentifier: string | null;
};

/** Any adapter failure (network, HTTP status, schema violation, truncation).
 * Callers catch it and fall back to retrieval cards — an LLM failure must
 * never fail the ask. `usage` is set when the provider DID bill tokens
 * before the failure (e.g. truncated output), so the budget can reconcile
 * real spend instead of assuming zero. */
export class LlmError extends Error {
  usage: LlmUsage | null;

  constructor(message: string, options?: { cause?: unknown; usage?: LlmUsage }) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "LlmError";
    this.usage = options?.usage ?? null;
  }
}

export type LlmClient = {
  generateAnswer: (input: GenerateAnswerInput) => Promise<GenerateAnswerResult>;
};
