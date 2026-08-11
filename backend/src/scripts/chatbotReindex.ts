// npm run chatbot:reindex — build a new chatbot index generation and flip it
// active (docs/plans/chatbot-rag.md component 3). Free to run: local/private
// TEI embeddings, no AI-provider calls, so no aiCallGuard involvement.
//
// Requires CHATBOT_EMBEDDINGS_URL (the TEI service). Local dev:
//   docker run --rm -p 8080:80 --platform linux/amd64 \
//     ghcr.io/huggingface/text-embeddings-inference:cpu-1.9 \
//     --model-id BAAI/bge-small-en-v1.5
//   CHATBOT_EMBEDDINGS_URL=http://localhost:8080 npm run chatbot:reindex

import { Pool } from "pg";

import { readChatbotConfigFromEnv } from "../chatbot/chatbotConfig.js";
import { createEmbeddingsClient } from "../chatbot/embeddingsClient.js";
import { reindexChatbotCorpus } from "../chatbot/indexer.js";
import { loadProjectEnv } from "../config/env.js";

// November-2026 cohort (the app's current research scope). The whole month,
// not just the 3rd: runoffs and specials share the cycle.
const COHORT_START = "2026-11-01";
const COHORT_END = "2026-11-30";

async function main(): Promise<void> {
  loadProjectEnv();
  const config = readChatbotConfigFromEnv();
  if (!config.embeddingsUrl) {
    throw new Error(
      "CHATBOT_EMBEDDINGS_URL is required for reindexing (a generation without embeddings would silently disable vector search)"
    );
  }
  const embeddings = createEmbeddingsClient({
    baseUrl: config.embeddingsUrl,
    timeoutMs: config.embeddingsTimeoutMs,
  });

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp",
  });
  try {
    const cohort = await pool.query<{ id: string }>(
      `
        SELECT id::text AS id
        FROM public.elections
        WHERE election_date BETWEEN $1 AND $2
        ORDER BY id
      `,
      [COHORT_START, COHORT_END]
    );
    console.log(`chatbot reindex: ${cohort.rows.length} elections in the ${COHORT_START}..${COHORT_END} cohort`);

    const startedAt = Date.now();
    const result = await reindexChatbotCorpus({
      db: pool,
      embeddings,
      electionIds: cohort.rows.map((row) => row.id),
      onProgress: (progress) => {
        console.log(`chatbot reindex [${progress.phase}] ${progress.done}/${progress.total}`);
      },
    });
    console.log(
      JSON.stringify(
        {
          ok: true,
          generation_id: result.generationId,
          chunk_count: result.chunkCount,
          election_count: result.electionCount,
          deleted_generations: result.deletedGenerations,
          took_seconds: Math.round((Date.now() - startedAt) / 1000),
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("chatbot reindex failed:", error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
