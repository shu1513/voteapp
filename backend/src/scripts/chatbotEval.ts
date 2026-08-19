// npm run chatbot:eval — measures the BEHAVIOR.md release gates against the
// golden set using the ACTIVE index generation and the live intent router:
//   - 100% of template and refuse_policy cases route deterministically
//   - recall@5 >= 0.85 on retrieval cases (every expectedEntity referenced by
//     a top-5 chunk matching one of the expectedSourceTypes)
//   - 100% of clarify cases clarify; >= 90% of refuse_no_data cases refuse
// Needs a built generation (npm run chatbot:reindex) and, for the vector
// branch, CHATBOT_EMBEDDINGS_URL; without it the eval runs keyword-only and
// says so (scores will be lower than the real deployment's).

import { Pool } from "pg";

import { createAskService, type AskContext } from "../chatbot/askService.js";
import { readChatbotEmbeddingsFromEnv } from "../chatbot/chatbotConfig.js";
import { createEmbeddingsClient } from "../chatbot/embeddingsClient.js";
import { goldenSet, type GoldenCase } from "../chatbot/golden/goldenSet.js";
import { detectIntent, detectStateInQuestion } from "../chatbot/intents.js";
import { getActiveGeneration, retrieveChunks, type RetrievedChunk } from "../chatbot/retrieval.js";
import { loadProjectEnv } from "../config/env.js";

type CaseResult = {
  id: string;
  expected: string;
  actual: string;
  pass: boolean;
  detail?: string;
};

function chunkReferencesEntity(chunk: RetrievedChunk, entity: string): boolean {
  const needle = entity.toLowerCase();
  return chunk.title.toLowerCase().includes(needle) || chunk.content.toLowerCase().includes(needle);
}

/** Resolve a golden case's simulated viewed page to a real ask context: the
 * entity name must match exactly one source in the active generation (chunk
 * titles are "<name> — candidate, …" / "<race title> — …"). null → the case
 * fails with a diagnostic instead of silently running context-free. */
async function resolvePageContext(
  pool: Pool,
  generationId: string,
  pageContext: NonNullable<GoldenCase["pageContext"]>
): Promise<AskContext | null> {
  const result = await pool.query<{ source_id: string }>(
    `
      SELECT DISTINCT chunk.source_id::text AS source_id
      FROM chatbot.chunks AS chunk
      WHERE chunk.generation_id = $1::uuid
        AND chunk.source_type = $2
        AND chunk.title ILIKE $3
      LIMIT 2
    `,
    [
      generationId,
      pageContext.kind === "candidate" ? "candidate_profile" : "election",
      pageContext.kind === "candidate" ? `${pageContext.entityName} — candidate,%` : `${pageContext.entityName}%`,
    ]
  );
  const first = result.rows[0]?.source_id;
  return first !== undefined && result.rows.length === 1 ? { kind: pageContext.kind, id: first } : null;
}

async function main(): Promise<void> {
  loadProjectEnv();
  // Deliberately NOT gated on CHATBOT_ENABLED: the eval measures retrieval
  // quality whether or not the API surface is switched on.
  const embeddingsConfig = readChatbotEmbeddingsFromEnv();
  const embeddings = embeddingsConfig.url
    ? createEmbeddingsClient({ baseUrl: embeddingsConfig.url, timeoutMs: embeddingsConfig.timeoutMs })
    : null;
  if (!embeddings) {
    console.warn("CHATBOT_EMBEDDINGS_URL unset: evaluating KEYWORD-ONLY retrieval (deployment runs hybrid)");
  }
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp",
  });
  try {
    const generation = await getActiveGeneration(pool);
    if (!generation) {
      throw new Error("no active chatbot generation; run `npm run chatbot:reindex` first");
    }
    // logQuestions:false — a golden-set run is not user traffic; logging it
    // would swamp chatbot.questions (66 rows per run) and skew the report.
    const askService = createAskService({ db: pool, embeddings, logQuestions: false });

    const results: CaseResult[] = [];
    for (const goldenCase of goldenSet) {
      results.push(await evaluateCase(pool, embeddings, generation.id, askService, goldenCase));
    }

    const byExpected = new Map<string, CaseResult[]>();
    for (const result of results) {
      const list = byExpected.get(result.expected) ?? [];
      list.push(result);
      byExpected.set(result.expected, list);
    }
    // Exit code follows the BEHAVIOR.md thresholds, not perfection: 94%
    // recall PASSES the >=0.85 gate and must exit 0, or a CI wiring of this
    // script would fail every passing run. Individual failures still print
    // for diagnosis.
    const GATE_THRESHOLDS: Record<string, number> = {
      retrieval: 0.85,
      template: 1,
      refuse_policy: 1,
      clarify: 1,
      refuse_no_data: 0.9,
    };
    const gateReport: Record<string, string> = {};
    let allGatesPass = true;
    for (const [expected, threshold] of Object.entries(GATE_THRESHOLDS)) {
      const list = byExpected.get(expected) ?? [];
      const passed = list.filter((r) => r.pass).length;
      const ratio = list.length === 0 ? 1 : passed / list.length;
      const ok = ratio >= threshold;
      allGatesPass &&= ok;
      gateReport[`${expected} (>=${threshold})`] =
        list.length === 0 ? "n/a" : `${passed}/${list.length} (${(ratio * 100).toFixed(0)}%) ${ok ? "PASS" : "FAIL"}`;
    }

    const failures = results.filter((result) => !result.pass);
    console.log(
      JSON.stringify(
        {
          generation_id: generation.id,
          vector_branch: embeddings ? "hybrid" : "KEYWORD-ONLY",
          gates: gateReport,
          gates_pass: allGatesPass,
          failures: failures.map((f) => ({ id: f.id, expected: f.expected, actual: f.actual, detail: f.detail })),
        },
        null,
        2
      )
    );
    if (!allGatesPass) {
      process.exitCode = 1;
    }
  } finally {
    await pool.end();
  }
}

async function evaluateCase(
  pool: Pool,
  embeddings: ReturnType<typeof createEmbeddingsClient> | null,
  generationId: string,
  askService: ReturnType<typeof createAskService>,
  goldenCase: GoldenCase
): Promise<CaseResult> {
  let pageContext: AskContext | null = null;
  if (goldenCase.pageContext) {
    pageContext = await resolvePageContext(pool, generationId, goldenCase.pageContext);
    if (!pageContext) {
      return {
        id: goldenCase.id,
        expected: goldenCase.expected,
        actual: "error",
        pass: false,
        detail: `pageContext "${goldenCase.pageContext.entityName}" did not resolve to exactly one ${goldenCase.pageContext.kind}`,
      };
    }
  }
  const response = await askService.ask(goldenCase.question, goldenCase.previousQuestion ?? null, pageContext);
  const base = { id: goldenCase.id, expected: goldenCase.expected, actual: response.outcome };

  if (goldenCase.expected !== "retrieval") {
    return { ...base, pass: response.outcome === goldenCase.expected };
  }

  // Retrieval cases judge recall@5 at the CHUNK level, so re-run retrieval
  // with the same scope carry-over the ask service applies (append-only
  // previous-turn text; see askService.ts — keep in sync).
  if (response.outcome !== "retrieval") {
    return { ...base, pass: false, detail: "outcome was not retrieval" };
  }
  if (detectIntent(goldenCase.question)) {
    return { ...base, pass: false, detail: "unexpectedly routed to an intent" };
  }
  let scopeState = detectStateInQuestion(goldenCase.question);
  let retrievalText = goldenCase.question;
  if (goldenCase.previousQuestion && !scopeState) {
    scopeState = detectStateInQuestion(goldenCase.previousQuestion);
    retrievalText = `${goldenCase.question} ${goldenCase.previousQuestion}`;
  }
  // Context rides into the recall re-run exactly as passed to ask: retrieval
  // cases with pageContext use deictic/race-collective phrasing (the ask
  // service applies context on those), so passing it unconditionally here
  // stays in sync with what the ask actually saw.
  const retrieval = await retrieveChunks({
    db: pool,
    embeddings,
    generationId,
    question: retrievalText,
    scopeState,
    contextCandidateId: pageContext?.kind === "candidate" ? pageContext.id : null,
    contextElectionId: pageContext?.kind === "election" ? pageContext.id : null,
  });
  const expectedTypes = goldenCase.expectedSourceTypes ?? [];
  const missing: string[] = [];
  for (const entity of goldenCase.expectedEntities ?? []) {
    const found = retrieval.chunks.some(
      (chunk) =>
        (expectedTypes.length === 0 || (expectedTypes as readonly string[]).includes(chunk.sourceType)) &&
        chunkReferencesEntity(chunk, entity)
    );
    if (!found) {
      missing.push(entity);
    }
  }
  return {
    ...base,
    pass: missing.length === 0,
    detail:
      missing.length > 0
        ? `missing entities: ${missing.join(", ")}; top chunks: ${retrieval.chunks
            .map((chunk) => `${chunk.sourceType}:${chunk.title.slice(0, 60)}`)
            .join(" | ")}`
        : undefined,
  };
}

main().catch((error) => {
  console.error("chatbot eval failed:", error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
