// Client for the HuggingFace text-embeddings-inference (TEI) service running
// BAAI/bge-small-en-v1.5 (384 dims) — docs/plans/chatbot-rag.md component 2.
//
// Local dev (image is amd64-only, so Apple Silicon needs the platform flag):
//   docker run --rm -p 8080:80 --platform linux/amd64 \
//     ghcr.io/huggingface/text-embeddings-inference:cpu-1.9 \
//     --model-id BAAI/bge-small-en-v1.5
//
// Degraded mode is the CALLER's job: retrieval catches EmbeddingsError and
// falls back to keyword-only search; the indexer aborts instead (a corpus
// without embeddings would silently disable the vector branch for weeks).

import { CHATBOT_EMBEDDING_DIMS } from "./chatbotConfig.js";

// BGE models want this prefix on QUERY embeddings only — never on document
// chunks (asymmetric retrieval; the model was trained that way).
export const BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: ";

export class EmbeddingsError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "EmbeddingsError";
  }
}

export type EmbeddingsClient = {
  /** One query embedding, with the BGE query instruction prefix applied. */
  embedQuery: (text: string) => Promise<number[]>;
  /** Document embeddings, NO prefix. Callers batch (TEI default max batch
   * fits well under 64 inputs of our chunk size). */
  embedDocuments: (texts: readonly string[]) => Promise<number[][]>;
};

async function requestEmbeddings(
  baseUrl: string,
  timeoutMs: number,
  inputs: readonly string[]
): Promise<number[][]> {
  let response: Response;
  try {
    response = await fetch(`${baseUrl}/embed`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      // truncate: bge-small caps at 512 tokens; the chunker targets 150–350
      // so truncation should never fire, but a silent 413 would be worse.
      body: JSON.stringify({ inputs, truncate: true }),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error) {
    throw new EmbeddingsError(
      `embeddings service unreachable: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error }
    );
  }
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new EmbeddingsError(`embeddings service returned ${response.status}: ${body.slice(0, 200)}`);
  }
  const parsed: unknown = await response.json().catch((error: unknown) => {
    throw new EmbeddingsError("embeddings service returned invalid JSON", { cause: error });
  });
  if (
    !Array.isArray(parsed) ||
    parsed.length !== inputs.length ||
    parsed.some(
      (row) =>
        !Array.isArray(row) ||
        row.length !== CHATBOT_EMBEDDING_DIMS ||
        row.some((value) => typeof value !== "number" || !Number.isFinite(value))
    )
  ) {
    throw new EmbeddingsError(
      `embeddings service returned an unexpected shape (expected ${inputs.length} x ${CHATBOT_EMBEDDING_DIMS})`
    );
  }
  return parsed as number[][];
}

export function createEmbeddingsClient(options: { baseUrl: string; timeoutMs: number }): EmbeddingsClient {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  return {
    async embedQuery(text: string): Promise<number[]> {
      const rows = await requestEmbeddings(baseUrl, options.timeoutMs, [BGE_QUERY_PREFIX + text]);
      return rows[0] as number[];
    },
    async embedDocuments(texts: readonly string[]): Promise<number[][]> {
      if (texts.length === 0) {
        return [];
      }
      return requestEmbeddings(baseUrl, options.timeoutMs, texts);
    },
  };
}

/** Postgres halfvec/vector literal for a parameterized cast: `$1::halfvec`. */
export function toVectorLiteral(embedding: readonly number[]): string {
  return `[${embedding.join(",")}]`;
}
