import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { rewriteToPlainLanguage, verifyPlainLanguageRewrite } from "../ai/rewritePlainLanguage.js";
import {
  runPlainLanguageBackfill,
  type PlainLanguageBackfillFilter,
} from "../pipeline/content/plainLanguageBackfill.js";

const TARGET_TABLES = ["candidates", "ballot_measures", "candidate_records"] as const;

function readLimit(): number | undefined {
  const index = process.argv.indexOf("--limit");
  if (index === -1) {
    return undefined;
  }
  const raw = process.argv[index + 1];
  const parsed = Number.parseInt(raw ?? "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid --limit: ${raw ?? "(missing)"}`);
  }
  return parsed;
}

function readOnlyTable(): PlainLanguageBackfillFilter["onlyTable"] {
  const index = process.argv.indexOf("--only");
  if (index === -1) {
    return undefined;
  }
  const raw = process.argv[index + 1] ?? "";
  const match = TARGET_TABLES.find((table) => table === raw);
  if (!match) {
    throw new Error(`Invalid --only: ${raw || "(missing)"}. Expected one of ${TARGET_TABLES.join(", ")}.`);
  }
  return match;
}

/**
 * One candidate id per line; blank lines and `#` comments ignored so a scoping
 * list can be generated straight out of psql and annotated.
 */
async function readCandidateIds(): Promise<readonly string[] | undefined> {
  const index = process.argv.indexOf("--candidate-ids-file");
  if (index === -1) {
    return undefined;
  }
  const path = process.argv[index + 1];
  if (!path || path.startsWith("--")) {
    throw new Error("Missing value for --candidate-ids-file");
  }
  const ids = (await readFile(path, "utf8"))
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));
  if (ids.length === 0) {
    throw new Error(`--candidate-ids-file ${path} contained no ids`);
  }
  return ids;
}

/**
 * Operator-authored rewrites:
 * `[{ "targetId": uuid, "originalText": "...", "rewrittenText": "..." }]`.
 *
 * Lets a manual research pass reuse the whole backfill path — mechanical
 * checks, staleness guard, record_identity_key recompute, audit row — when no
 * AI provider is usable. The verifier is skipped (see manualAttestation).
 *
 * Keyed on originalText rather than targetId because the rewrite dependency
 * receives the text, not the row id. That is also the safety property: if the
 * stored text moved since the file was written, nothing matches and the run
 * fails loudly instead of pasting a rewrite onto changed content. targetId is
 * carried for traceability only.
 */
async function readManualRewrites(): Promise<
  { byOriginal: Map<string, string>; targetIds: string[] } | undefined
> {
  const index = process.argv.indexOf("--rewrites-file");
  if (index === -1) {
    return undefined;
  }
  const path = process.argv[index + 1];
  if (!path || path.startsWith("--")) {
    throw new Error("Missing value for --rewrites-file");
  }
  const parsed: unknown = JSON.parse(await readFile(path, "utf8"));
  if (!Array.isArray(parsed)) {
    throw new Error("rewrites file must contain a JSON array");
  }
  const byOriginal = new Map<string, string>();
  const targetIds: string[] = [];
  parsed.forEach((entry, position) => {
    const { targetId, originalText, rewrittenText } = (entry ?? {}) as Record<string, unknown>;
    if (typeof targetId !== "string" || targetId.trim().length === 0) {
      throw new Error(`rewrites[${position}].targetId must be a non-empty string`);
    }
    if (typeof originalText !== "string" || originalText.trim().length === 0) {
      throw new Error(`rewrites[${position}].originalText must be a non-empty string`);
    }
    if (typeof rewrittenText !== "string" || rewrittenText.trim().length === 0) {
      throw new Error(`rewrites[${position}].rewrittenText must be a non-empty string`);
    }
    byOriginal.set(originalText, rewrittenText.trim());
    targetIds.push(targetId.trim());
  });
  return { byOriginal, targetIds };
}

async function main(): Promise<void> {
  const env = getPipelineEnv();

  // One-off local data migration; the production database gets the corrected
  // rows through the normal deployment path, never from this machine.
  const hostname = new URL(env.DATABASE_URL).hostname;
  if (hostname !== "localhost" && hostname !== "127.0.0.1") {
    throw new Error(`Refusing to run against a non-local database: ${hostname}`);
  }

  const dryRun = process.argv.includes("--dry-run");
  const limit = readLimit();
  const onlyTable = readOnlyTable();
  const candidateIds = await readCandidateIds();
  const manualRewrites = await readManualRewrites();
  const filter: PlainLanguageBackfillFilter = {
    ...(onlyTable !== undefined ? { onlyTable } : {}),
    ...(candidateIds !== undefined ? { candidateIds } : {}),
    // The rewrites file IS the work list: without this, the first record the
    // file does not cover would abort the whole batch.
    ...(manualRewrites ? { onlyTable: "candidate_records" as const, recordIds: manualRewrites.targetIds } : {}),
  };
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const summary = await runPlainLanguageBackfill(pool, {
      rewrite: manualRewrites
        ? async (input) => {
            const rewrittenText = manualRewrites.byOriginal.get(input.text);
            if (rewrittenText === undefined) {
              return {
                ok: false as const,
                reason: `no operator rewrite supplied for this target; stored text may have changed since the rewrites file was written: ${input.text.slice(0, 120)}`,
              };
            }
            return { ok: true as const, provider: "manual" as const, model: "manual-research", rewrittenText };
          }
        : rewriteToPlainLanguage,
      verify: verifyPlainLanguageRewrite,
      aiConfig: {
        timeoutMs: env.AI_TIMEOUT_MS,
        openAiApiKey: env.OPENAI_API_KEY,
        anthropicApiKey: env.ANTHROPIC_API_KEY,
        geminiApiKey: env.GEMINI_API_KEY,
      },
      dryRun,
      ...(limit !== undefined ? { limit } : {}),
      ...(manualRewrites ? { manualAttestation: true } : {}),
      filter,
    });
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("plain-language backfill failed:", error);
  process.exit(1);
});
