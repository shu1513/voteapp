// Hybrid retrieval over the active chunk generation — docs/plans/chatbot-rag.md
// component 4.
//
// Three ranked lists merged with Reciprocal Rank Fusion:
//   A. lexical: content_tsv @@ websearch_to_tsquery, ranked by ts_rank_cd
//   B. vector: exact-scan cosine over halfvec embeddings (no HNSW in v1 —
//      see the migration comment), scope-filtered by state when known
//   C. entity: chunks belonging to candidates whose names fuzzy-match the
//      question (pg_trgm word_similarity against public.candidates)
// RRF decides ORDER; the answerability gate thresholds on the RAW scores
// (kept per chunk) because RRF rank is relative, never absolute relevance.
//
// The embeddings service being down degrades to lists A+C (keyword-only)
// with a warning log — nothing else changes.

import type { Pool } from "pg";

import { EmbeddingsError, toVectorLiteral, type EmbeddingsClient } from "./embeddingsClient.js";

export const RETRIEVAL_TOP_K = 5;
const BRANCH_LIMIT = 20;
const RRF_K = 60;

// Answerability gate thresholds on raw scores (tuned against the golden set
// via `npm run chatbot:eval` on the live local index, 2026-08-11; see
// BEHAVIOR.md release gates). A question is answerable when ANY holds:
//   - best cosine similarity >= GATE_MIN_COSINE. Measured bge-small
//     similarities: on-topic race questions 0.72-0.82, off-topic
//     (weather/celebrities/2028 races/foreign mayors) 0.58-0.70. The
//     threshold sits in the measured gap.
//   - best AND-lexical ts_rank_cd >= GATE_MIN_LEXICAL. The gate uses the
//     strict websearch AND query (every term must match), NOT the OR query
//     the ranking branch uses — under OR a single matched title word scores
//     ~1.0 and off-topic questions would sail through.
//   - a candidate-name match >= GATE_MIN_ENTITY_SIMILARITY. word_similarity
//     noise from unrelated questions reaches ~0.7 ("Tim Taylor" scores 0.7
//     against "What is Taylor Swift's net worth?"), so this sits above it.
export const GATE_MIN_COSINE = 0.71;
export const GATE_MIN_LEXICAL = 0.08;
export const GATE_MIN_ENTITY_SIMILARITY = 0.75;

// Entity matches at/above this are candidates for scope/boost resolution.
const ENTITY_MATCH_MIN_SIMILARITY = 0.45;
// Same-name variants ("Michael Smith" / "Michael L. Smith") score lower than
// the exact match; anything above this still counts as the same person-name
// for ambiguity detection.
const ENTITY_SAME_NAME_MIN_SIMILARITY = 0.6;

export type ActiveGeneration = {
  id: string;
  activatedAt: string;
};

export type CandidateEntityMatch = {
  candidateId: string;
  displayName: string;
  party: string;
  state: string;
  currentOffice: string | null;
  similarity: number;
};

export type RetrievedChunk = {
  id: string;
  sourceType: string;
  sourceId: string | null;
  electionId: string | null;
  state: string | null;
  title: string;
  content: string;
  evidenceUrls: string[];
  lexicalScore: number;
  cosineSimilarity: number;
  rrfScore: number;
};

export type ElectionTitleMatch = {
  state: string | null;
  title: string;
  similarity: number;
  /** How strongly the chunk's place name ("Los Angeles city") appears in the
   * question — the signal that the question is already place-scoped. */
  placeSimilarity: number;
};

export type RetrievalResult = {
  chunks: RetrievedChunk[];
  /** Election chunks whose titles fuzzy-match the question, best first —
   * the scope-ambiguity signal ("the sheriff race" ties across states). */
  electionTitleMatches: ElectionTitleMatch[];
  entityMatches: CandidateEntityMatch[];
  /** Same-name candidates the question cannot distinguish; non-empty →
   * caller must return a clarification, never silently pick (rule 7). */
  ambiguousEntities: CandidateEntityMatch[];
  bestLexicalScore: number;
  bestCosineSimilarity: number;
  bestEntitySimilarity: number;
  degradedToKeywordOnly: boolean;
};

export async function getActiveGeneration(db: Pool): Promise<ActiveGeneration | null> {
  const result = await db.query<{ id: string; activated_at: string }>(
    `
      SELECT id::text AS id, activated_at::text AS activated_at
      FROM chatbot.index_generations
      WHERE status = 'active'
      LIMIT 1
    `
  );
  const row = result.rows[0];
  return row ? { id: row.id, activatedAt: row.activated_at } : null;
}

type EntityRow = {
  candidate_id: string;
  display_name: string;
  party: string;
  state: string;
  current_office: string | null;
  similarity: number;
};

/** Fuzzy candidate-name resolution: word_similarity finds the best-matching
 * span of the question for each name, so "how much has jon ossoff raised"
 * scores Jon Ossoff highly. Seq scan over ~9k candidates, a few ms. */
export async function resolveCandidateEntities(db: Pool, question: string): Promise<CandidateEntityMatch[]> {
  const result = await db.query<EntityRow>(
    `
      SELECT
        candidate.id::text AS candidate_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          trim(concat_ws(' ', candidate.first_name, candidate.last_name))
        ) AS display_name,
        candidate.party,
        candidate.state,
        candidate.current_office,
        word_similarity(
          COALESCE(
            NULLIF(trim(candidate.display_name), ''),
            trim(concat_ws(' ', candidate.first_name, candidate.last_name))
          ),
          $1
        )::float8 AS similarity
      FROM public.candidates AS candidate
      WHERE candidate.deleted_at IS NULL
        AND candidate.merged_into_candidate_id IS NULL
        AND word_similarity(
          COALESCE(
            NULLIF(trim(candidate.display_name), ''),
            trim(concat_ws(' ', candidate.first_name, candidate.last_name))
          ),
          $1
        ) >= $2
      ORDER BY similarity DESC, display_name ASC
      LIMIT 10
    `,
    [question, ENTITY_MATCH_MIN_SIMILARITY]
  );
  return result.rows.map((row) => ({
    candidateId: row.candidate_id,
    displayName: row.display_name,
    party: row.party ?? "",
    state: row.state ?? "",
    currentOffice: row.current_office,
    similarity: row.similarity,
  }));
}

/** Normalized "first last" (middle names/initials dropped): the identity key
 * a question like "Michael Smith" actually names. */
function firstLastKey(displayName: string): string {
  const tokens = displayName
    .toLowerCase()
    .replace(/["'.()]/g, "")
    .split(/\s+/)
    .filter((token) => token.length > 0);
  if (tokens.length <= 1) {
    return tokens.join(" ");
  }
  return `${tokens[0]} ${tokens[tokens.length - 1]}`;
}

/** Same-name detection anchored on the BEST match: a strong best match with
 * 2+ candidates sharing its first+last name ("Michael Smith" in GA,
 * "Michael L. Smith" in OH, "Michael A. Smith" in IL) is ambiguous. A unique
 * strong match ("Mike Collins" = 1.0) is not, even when unrelated "Mike
 * Conway"s tie each other lower down. */
export function findAmbiguousEntities(matches: readonly CandidateEntityMatch[]): CandidateEntityMatch[] {
  const best = matches[0];
  if (!best || best.similarity < GATE_MIN_ENTITY_SIMILARITY) {
    return [];
  }
  const bestKey = firstLastKey(best.displayName);
  const sameName = matches.filter(
    (match) => match.similarity >= ENTITY_SAME_NAME_MIN_SIMILARITY && firstLastKey(match.displayName) === bestKey
  );
  return sameName.length >= 2 ? sameName : [];
}

type ChunkRow = {
  id: string;
  source_type: string;
  source_id: string | null;
  election_id: string | null;
  state: string | null;
  title: string;
  content: string;
  evidence_urls: unknown;
  score: number;
  /** Title branch only: place-name-in-question similarity. */
  place_score?: number;
};

function parseEvidenceUrls(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string");
}

function toBaseChunk(row: ChunkRow): Omit<RetrievedChunk, "lexicalScore" | "cosineSimilarity" | "rrfScore"> {
  return {
    id: row.id,
    sourceType: row.source_type,
    sourceId: row.source_id,
    electionId: row.election_id,
    state: row.state,
    title: row.title,
    content: row.content,
    evidenceUrls: parseEvidenceUrls(row.evidence_urls),
  };
}

export type RetrieveOptions = {
  db: Pool;
  embeddings: EmbeddingsClient | null;
  generationId: string;
  question: string;
  /** Restrict the vector branch when the scope is known (e.g. a resolved
   * candidate's state, or a state named in the question). */
  scopeState?: string | null;
};

export async function retrieveChunks(options: RetrieveOptions): Promise<RetrievalResult> {
  const { db, embeddings, generationId, question } = options;
  const scopeState = options.scopeState ?? null;

  const entityMatches = await resolveCandidateEntities(db, question);
  const ambiguousEntities = findAmbiguousEntities(entityMatches);
  const bestEntitySimilarity = entityMatches[0]?.similarity ?? 0;

  // Branch A: lexical. plainto_tsquery sanitizes arbitrary user text; its
  // AND semantics are then relaxed to OR because natural questions carry
  // words no chunk contains ("money", "race", "US" vs "United States") and a
  // single missing term would zero the whole branch. ts_rank_cd still ranks
  // more-terms-matched far above single-term noise, and the gate thresholds
  // are calibrated for OR-ranks.
  const lexicalResult = await db.query<ChunkRow>(
    `
      SELECT
        chunk.id::text AS id,
        chunk.source_type,
        chunk.source_id::text AS source_id,
        chunk.election_id::text AS election_id,
        chunk.state,
        chunk.title,
        chunk.content,
        chunk.evidence_urls,
        ts_rank_cd(chunk.content_tsv, query.tsquery)::float8 AS score
      FROM chatbot.chunks AS chunk,
        (SELECT replace(plainto_tsquery('english', $2)::text, ' & ', ' | ')::tsquery AS tsquery) AS query
      WHERE chunk.generation_id = $1::uuid
        AND chunk.content_tsv @@ query.tsquery
      ORDER BY score DESC, chunk.id ASC
      LIMIT $3
    `,
    [generationId, question, BRANCH_LIMIT]
  );

  // Gate signal: strict AND rank. Separate tiny query because the ranking
  // branch's OR rank cannot gate (see threshold comment above); this is also
  // what keeps keyword-exact questions answerable in degraded (no-embeddings)
  // mode.
  const gateLexicalResult = await db.query<{ score: number }>(
    `
      SELECT ts_rank_cd(chunk.content_tsv, query.tsquery)::float8 AS score
      FROM chatbot.chunks AS chunk,
        (SELECT websearch_to_tsquery('english', $2) AS tsquery) AS query
      WHERE chunk.generation_id = $1::uuid
        AND chunk.content_tsv @@ query.tsquery
      ORDER BY score DESC
      LIMIT 1
    `,
    [generationId, question]
  );
  const bestLexicalScore = gateLexicalResult.rows[0]?.score ?? 0;

  // Branch B: vector (exact scan). Down/unset service → keyword-only.
  let vectorRows: ChunkRow[] = [];
  let degradedToKeywordOnly = false;
  if (embeddings) {
    try {
      const queryEmbedding = await embeddings.embedQuery(question);
      const vectorResult = await db.query<ChunkRow>(
        `
          SELECT
            chunk.id::text AS id,
            chunk.source_type,
            chunk.source_id::text AS source_id,
            chunk.election_id::text AS election_id,
            chunk.state,
            chunk.title,
            chunk.content,
            chunk.evidence_urls,
            (1 - (chunk.embedding <=> $2::halfvec))::float8 AS score
          FROM chatbot.chunks AS chunk
          WHERE chunk.generation_id = $1::uuid
            AND chunk.embedding IS NOT NULL
            AND ($4::text IS NULL OR chunk.state = $4)
          ORDER BY chunk.embedding <=> $2::halfvec ASC, chunk.id ASC
          LIMIT $3
        `,
        [generationId, toVectorLiteral(queryEmbedding), BRANCH_LIMIT, scopeState]
      );
      vectorRows = vectorResult.rows;
    } catch (error) {
      if (!(error instanceof EmbeddingsError)) {
        throw error;
      }
      degradedToKeywordOnly = true;
      console.warn("chatbot retrieval degraded to keyword-only: embeddings unavailable:", error.message);
    }
  } else {
    degradedToKeywordOnly = true;
  }

  // Branch C: entity — the matched candidates' own chunks, best entity first.
  // Gives named-person questions their chunks even when phrasing defeats both
  // search branches.
  let entityRows: ChunkRow[] = [];
  const entityIds = entityMatches
    .filter((match) => match.similarity >= GATE_MIN_ENTITY_SIMILARITY)
    .map((match) => match.candidateId);
  if (entityIds.length > 0) {
    const entityResult = await db.query<ChunkRow>(
      `
        SELECT
          chunk.id::text AS id,
          chunk.source_type,
          chunk.source_id::text AS source_id,
          chunk.election_id::text AS election_id,
          chunk.state,
          chunk.title,
          chunk.content,
          chunk.evidence_urls,
          0::float8 AS score
        FROM chatbot.chunks AS chunk
        JOIN unnest($2::uuid[]) WITH ORDINALITY AS matched(candidate_id, rank)
          ON matched.candidate_id = chunk.source_id
        WHERE chunk.generation_id = $1::uuid
          AND chunk.source_type IN ('candidate_profile', 'candidate_record', 'finance_summary')
        -- Profile then finance then records: a named-candidate question must
        -- surface the finance summary inside the top-5 even when the
        -- candidate has many record chunks.
        ORDER BY
          matched.rank ASC,
          CASE chunk.source_type
            WHEN 'candidate_profile' THEN 0
            WHEN 'finance_summary' THEN 1
            ELSE 2
          END ASC,
          chunk.id ASC
        LIMIT $3
      `,
      [generationId, entityIds, BRANCH_LIMIT]
    );
    entityRows = entityResult.rows;
  }

  // Branch D: race listings — election chunks whose office phrase (the part
  // of the title before " — ") fuzzy-matches the question ("who's running
  // for <office> in <place>"). Profile chunks otherwise crowd the election
  // chunk out of the top-5 for offices with many candidates. Direction
  // matters for word_similarity: office (short) against the question (long)
  // finds the question extent naming the office; symmetric similarity() on
  // the full title breaks ties in favor of the title whose place tokens the
  // question also names. Seq scan over the generation's election chunks
  // (~6k), a few ms.
  const titleResult = await db.query<ChunkRow>(
    `
      SELECT
        chunk.id::text AS id,
        chunk.source_type,
        chunk.source_id::text AS source_id,
        chunk.election_id::text AS election_id,
        chunk.state,
        chunk.title,
        chunk.content,
        chunk.evidence_urls,
        GREATEST(
          word_similarity(split_part(chunk.title, ' — ', 1), $2),
          similarity(chunk.title, $2)
        )::float8 AS score,
        word_similarity(split_part(split_part(chunk.title, ' — ', 2), ',', 1), $2)::float8 AS place_score
      FROM chatbot.chunks AS chunk
      WHERE chunk.generation_id = $1::uuid
        AND chunk.source_type = 'election'
        AND GREATEST(
          word_similarity(split_part(chunk.title, ' — ', 1), $2),
          similarity(chunk.title, $2)
        ) >= 0.35
      ORDER BY (GREATEST(
          word_similarity(split_part(chunk.title, ' — ', 1), $2),
          similarity(chunk.title, $2)
        ) + word_similarity(split_part(split_part(chunk.title, ' — ', 2), ',', 1), $2)) DESC,
        chunk.id ASC
      LIMIT 10
    `,
    [generationId, question]
  );

  // RRF merge; raw scores ride along per chunk for the gate.
  type Merged = {
    base: Omit<RetrievedChunk, "lexicalScore" | "cosineSimilarity" | "rrfScore">;
    lexicalScore: number;
    cosineSimilarity: number;
    rrfScore: number;
  };
  const merged = new Map<string, Merged>();
  const fold = (rows: readonly ChunkRow[], kind: "lexical" | "vector" | "entity" | "title"): void => {
    rows.forEach((row, index) => {
      const existing = merged.get(row.id) ?? {
        base: toBaseChunk(row),
        lexicalScore: 0,
        cosineSimilarity: 0,
        rrfScore: 0,
      };
      existing.rrfScore += 1 / (RRF_K + index + 1);
      if (kind === "lexical") {
        existing.lexicalScore = Math.max(existing.lexicalScore, row.score);
      }
      if (kind === "vector") {
        existing.cosineSimilarity = Math.max(existing.cosineSimilarity, row.score);
      }
      merged.set(row.id, existing);
    });
  };
  fold(lexicalResult.rows, "lexical");
  fold(vectorRows, "vector");
  fold(entityRows, "entity");
  fold(titleResult.rows, "title");

  const chunks = [...merged.values()]
    .sort((a, b) => b.rrfScore - a.rrfScore || a.base.id.localeCompare(b.base.id))
    .slice(0, RETRIEVAL_TOP_K)
    .map((entry) => ({
      ...entry.base,
      lexicalScore: entry.lexicalScore,
      cosineSimilarity: entry.cosineSimilarity,
      rrfScore: entry.rrfScore,
    }));

  const bestCosineSimilarity = vectorRows[0]?.score ?? 0;

  return {
    chunks,
    electionTitleMatches: titleResult.rows.map((row) => ({
      state: row.state,
      title: row.title,
      similarity: row.score,
      placeSimilarity: row.place_score ?? 0,
    })),
    entityMatches,
    ambiguousEntities,
    bestLexicalScore,
    bestCosineSimilarity,
    bestEntitySimilarity,
    degradedToKeywordOnly,
  };
}

/** Answerability gate on RAW scores (never RRF rank). */
export function isAnswerable(result: RetrievalResult): boolean {
  if (result.chunks.length === 0) {
    return false;
  }
  return (
    result.bestCosineSimilarity >= GATE_MIN_COSINE ||
    result.bestLexicalScore >= GATE_MIN_LEXICAL ||
    result.bestEntitySimilarity >= GATE_MIN_ENTITY_SIMILARITY
  );
}
