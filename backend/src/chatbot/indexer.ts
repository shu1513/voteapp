// Snapshot-generation indexer — docs/plans/chatbot-rag.md component 3.
//
// Never mutates the active corpus row by row: build a full new generation
// under status 'building', embed and validate it, then flip it to 'active'
// (and the old one to 'retired') in a single transaction. Retrieval and
// future cache keys carry the generation id, so the flip is atomic
// invalidation; a half-failed run leaves a 'building' carcass that the next
// run cleans up, never a mixed corpus.

import type { Pool } from "pg";

import { lookupElectionDetailById } from "../pipeline/address/ballotLookup.js";
import { CHATBOT_EMBEDDING_MODEL } from "./chatbotConfig.js";
import { CHUNKER_VERSION, extractChunksFromElection, type ChunkDraft } from "./chunker.js";
import { toVectorLiteral, type EmbeddingsClient } from "./embeddingsClient.js";

const EMBED_BATCH_SIZE = 32;
const INSERT_BATCH_SIZE = 200;

// Advisory lock so two reindex runs cannot interleave: without it, run B's
// carcass cleanup deletes run A's 'building' generation and A's flip then
// "activates" a deleted row — retiring the active generation and activating
// nothing. Arbitrary app-unique key (dbMigrate uses 780_001_001).
const REINDEX_LOCK_KEY = 780_001_002;

export type ReindexProgress = {
  phase: "extract" | "embed" | "insert" | "flip" | "cleanup";
  done: number;
  total: number;
};

export type ReindexOptions = {
  db: Pool;
  embeddings: EmbeddingsClient;
  /** Elections to index. The reindex script selects the November-2026 cohort;
   * injectable so tests can pass a fixed list. */
  electionIds: readonly string[];
  onProgress?: (progress: ReindexProgress) => void;
};

export type ReindexResult = {
  generationId: string;
  chunkCount: number;
  electionCount: number;
  deletedGenerations: number;
};

export class ReindexValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReindexValidationError";
  }
}

/** Dedupe drafts across elections: a candidate running in two indexed
 * elections carries the same record rows into both payloads, and the
 * (generation_id, source_type, chunk_key) unique constraint would reject the
 * second copy. First occurrence wins (drafts are deterministic). */
export function dedupeChunkDrafts(drafts: readonly ChunkDraft[]): ChunkDraft[] {
  const seen = new Set<string>();
  const result: ChunkDraft[] = [];
  for (const draft of drafts) {
    const key = `${draft.sourceType}\0${draft.chunkKey}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(draft);
  }
  return result;
}

export async function reindexChatbotCorpus(options: ReindexOptions): Promise<ReindexResult> {
  const { db, embeddings, electionIds, onProgress } = options;

  if (electionIds.length === 0) {
    throw new ReindexValidationError("no elections selected; refusing to build an empty generation");
  }

  // Serialize runs: try-lock fails fast instead of queueing a second 20-min
  // build behind the first. Held on a dedicated connection for the whole run
  // (advisory locks are session-scoped).
  const lockClient = await db.connect();
  try {
    const lock = await lockClient.query<{ locked: boolean }>(`SELECT pg_try_advisory_lock($1) AS locked`, [
      REINDEX_LOCK_KEY,
    ]);
    if (!lock.rows[0]?.locked) {
      throw new ReindexValidationError("another chatbot reindex is already running (advisory lock held)");
    }
    return await runReindex(options);
  } finally {
    try {
      await lockClient.query(`SELECT pg_advisory_unlock($1)`, [REINDEX_LOCK_KEY]);
    } catch {
      // best-effort; releasing the connection drops the lock anyway
    }
    lockClient.release();
  }
}

async function runReindex(options: ReindexOptions): Promise<ReindexResult> {
  const { db, embeddings, electionIds, onProgress } = options;

  // 1. Extract chunks through the canonical election detail reader (same
  // filters as the public API: withdrawn candidacies and deleted/merged
  // candidates never appear).
  const drafts: ChunkDraft[] = [];
  const missingElectionIds: string[] = [];
  let processed = 0;
  for (const electionId of electionIds) {
    const detail = await lookupElectionDetailById(db, electionId);
    if (detail) {
      drafts.push(...extractChunksFromElection(detail));
    } else {
      missingElectionIds.push(electionId);
    }
    processed += 1;
    if (processed % 200 === 0 || processed === electionIds.length) {
      onProgress?.({ phase: "extract", done: processed, total: electionIds.length });
    }
  }
  // Every selected election must load: a null here means the reader lost an
  // election the cohort query saw moments ago, and a total-chunk comparison
  // could not catch it (other elections' profile/record/finance chunks mask
  // a missing one).
  if (missingElectionIds.length > 0) {
    throw new ReindexValidationError(
      `${missingElectionIds.length} of ${electionIds.length} elections failed to load: ${missingElectionIds
        .slice(0, 10)
        .join(", ")}${missingElectionIds.length > 10 ? ", …" : ""}`
    );
  }
  const chunks = dedupeChunkDrafts(drafts);
  const electionChunkCount = chunks.filter((chunk) => chunk.sourceType === "election").length;
  if (electionChunkCount !== electionIds.length) {
    throw new ReindexValidationError(
      `extracted ${electionChunkCount} election chunks for ${electionIds.length} elections`
    );
  }

  // 2. Embed. Failures abort the build (a keyword-only corpus must be an
  // explicit runtime degradation, never a silently half-built index).
  const embeddingsByIndex: number[][] = [];
  for (let start = 0; start < chunks.length; start += EMBED_BATCH_SIZE) {
    const batch = chunks.slice(start, start + EMBED_BATCH_SIZE);
    const vectors = await embeddings.embedDocuments(batch.map((chunk) => chunk.content));
    embeddingsByIndex.push(...vectors);
    onProgress?.({ phase: "embed", done: Math.min(start + EMBED_BATCH_SIZE, chunks.length), total: chunks.length });
  }

  // 3. Insert under a 'building' generation.
  const generationResult = await db.query<{ id: string }>(
    `
      INSERT INTO chatbot.index_generations (status, embedding_model, chunker_version)
      VALUES ('building', $1, $2)
      RETURNING id::text AS id
    `,
    [CHATBOT_EMBEDDING_MODEL, CHUNKER_VERSION]
  );
  const generationId = (generationResult.rows[0] as { id: string }).id;

  for (let start = 0; start < chunks.length; start += INSERT_BATCH_SIZE) {
    const batch = chunks.slice(start, start + INSERT_BATCH_SIZE);
    const values: string[] = [];
    const params: unknown[] = [generationId];
    for (const [index, chunk] of batch.entries()) {
      const embedding = embeddingsByIndex[start + index] as number[];
      const base = params.length;
      values.push(
        `($1, $${base + 1}, $${base + 2}::uuid, $${base + 3}, $${base + 4}::uuid, $${base + 5}::uuid, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}::jsonb, $${base + 10}, $${base + 11}::halfvec)`
      );
      params.push(
        chunk.sourceType,
        chunk.sourceId,
        chunk.chunkKey,
        chunk.electionId,
        chunk.districtId,
        chunk.state,
        chunk.title,
        chunk.content,
        JSON.stringify(chunk.evidenceUrls),
        chunk.contentHash,
        toVectorLiteral(embedding)
      );
    }
    await db.query(
      `
        INSERT INTO chatbot.chunks
          (generation_id, source_type, source_id, chunk_key, election_id, district_id,
           state, title, content, evidence_urls, content_hash, embedding)
        VALUES ${values.join(", ")}
      `,
      params
    );
    onProgress?.({ phase: "insert", done: Math.min(start + INSERT_BATCH_SIZE, chunks.length), total: chunks.length });
  }

  // 4. Validate counts, then flip atomically.
  const countResult = await db.query<{ count: string }>(
    `SELECT count(*) AS count FROM chatbot.chunks WHERE generation_id = $1::uuid AND embedding IS NOT NULL`,
    [generationId]
  );
  const insertedCount = Number.parseInt((countResult.rows[0] as { count: string }).count, 10);
  if (insertedCount !== chunks.length) {
    throw new ReindexValidationError(
      `generation ${generationId} holds ${insertedCount} embedded chunks, expected ${chunks.length}; leaving it in 'building' (never activated)`
    );
  }

  onProgress?.({ phase: "flip", done: 0, total: 1 });
  const client = await db.connect();
  let justRetiredId: string | null = null;
  try {
    await client.query("BEGIN");
    const retired = await client.query<{ id: string }>(
      `UPDATE chatbot.index_generations SET status = 'retired' WHERE status = 'active' RETURNING id::text AS id`
    );
    justRetiredId = retired.rows[0]?.id ?? null;
    const activated = await client.query(
      `UPDATE chatbot.index_generations SET status = 'active', activated_at = now() WHERE id = $1::uuid`,
      [generationId]
    );
    // Zero rows = our generation vanished (it would take a concurrent run
    // past the advisory lock, or manual deletion); committing would retire
    // the active generation and activate nothing.
    if (activated.rowCount !== 1) {
      throw new ReindexValidationError(`generation ${generationId} disappeared before activation`);
    }
    await client.query("COMMIT");
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // best-effort rollback; the generation stays 'building'
    }
    throw error;
  } finally {
    client.release();
  }

  // 5. Cleanup. The generation just retired is KEPT until the next successful
  // run (its grace period: requests that resolved the active generation just
  // before the flip finish against still-present rows). Everything older —
  // retired generations from prior runs and 'building' carcasses from failed
  // ones — is deleted; the cascade drops their chunks.
  onProgress?.({ phase: "cleanup", done: 0, total: 1 });
  const deleted = await db.query(
    `
      DELETE FROM chatbot.index_generations
      WHERE (status = 'retired' AND id <> COALESCE($1::uuid, '00000000-0000-0000-0000-000000000000'::uuid))
         OR (status = 'building' AND id <> $2::uuid)
    `,
    [justRetiredId, generationId]
  );

  return {
    generationId,
    chunkCount: chunks.length,
    electionCount: electionIds.length,
    deletedGenerations: deleted.rowCount ?? 0,
  };
}
