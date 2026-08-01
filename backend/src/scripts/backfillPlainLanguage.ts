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
 * Keyed on targetId, with originalText required to EQUAL the stored text at
 * apply time: the id addresses the row (two rows can carry identical text,
 * and a text-keyed map would silently hand both the same replacement), and
 * the text equality is the staleness guard — if the stored text moved since
 * the file was written, the run fails loudly instead of pasting a rewrite
 * onto content nobody reviewed.
 */
export function parseManualRewritesFile(raw: string): {
  byTargetId: Map<string, { originalText: string; rewrittenText: string }>;
  targetIds: string[];
} {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("rewrites file must contain a JSON array");
  }
  const byTargetId = new Map<string, { originalText: string; rewrittenText: string }>();
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
    const id = targetId.trim();
    if (byTargetId.has(id)) {
      throw new Error(`rewrites[${position}].targetId ${id} appears more than once in the file`);
    }
    byTargetId.set(id, { originalText, rewrittenText: rewrittenText.trim() });
    targetIds.push(id);
  });
  return { byTargetId, targetIds };
}

async function readManualRewrites(): Promise<ReturnType<typeof parseManualRewritesFile> | undefined> {
  const index = process.argv.indexOf("--rewrites-file");
  if (index === -1) {
    return undefined;
  }
  const path = process.argv[index + 1];
  if (!path || path.startsWith("--")) {
    throw new Error("Missing value for --rewrites-file");
  }
  return parseManualRewritesFile(await readFile(path, "utf8"));
}

/**
 * A flagged audit row permanently blocks auto-retry — the right behavior for
 * model rewrites, where the flag routes the row to a human. In an operator
 * run the human IS here: an entry in the rewrites file is the manual-queue
 * resolution for that target. Clearing the flagged row (only for targets the
 * file names, only status 'flagged') lets the corrected rewrite re-run
 * through every mechanical check; the worst case for an UNcorrected rewrite
 * is an identical re-flag. Without this, fixing a flagged manual rewrite
 * required hand-written SQL against the audit table.
 */
async function clearFlaggedAuditRowsForRetry(
  pool: Pool,
  targetIds: readonly string[]
): Promise<number> {
  const result = await pool.query(
    `DELETE FROM public.plain_language_rewrites
      WHERE target_table = 'candidate_records'
        AND target_column = 'description'
        AND status = 'flagged'
        AND target_id = ANY($1::uuid[])`,
    [[...targetIds]]
  );
  return result.rowCount ?? 0;
}

/**
 * A mistyped or already-processed targetId loads no backfill target, so the
 * run can end "successful" having applied nothing for that entry. Explain
 * every unprocessed entry after the run: an existing audit row is a normal
 * resume (that entry was applied earlier), anything else is an error the
 * operator must see.
 */
async function reportUnmatchedRewriteEntries(
  pool: Pool,
  targetIds: readonly string[],
  processed: number
): Promise<boolean> {
  if (processed === targetIds.length) {
    return false;
  }
  const { rows } = await pool.query<{ id: string; in_audit: boolean; exists_live: boolean }>(
    `SELECT ids.id::text AS id,
            EXISTS (SELECT 1 FROM public.plain_language_rewrites r
                     WHERE r.target_table = 'candidate_records' AND r.target_id = ids.id
                       AND r.target_column = 'description') AS in_audit,
            EXISTS (SELECT 1 FROM public.candidate_records cr
                     WHERE cr.id = ids.id AND cr.retired_at IS NULL AND cr.description <> '') AS exists_live
       FROM unnest($1::uuid[]) AS ids(id)`,
    [[...targetIds]]
  );
  let hadError = false;
  for (const row of rows) {
    if (row.in_audit) {
      continue; // applied or flagged in an earlier run — normal resume
    }
    if (!row.exists_live) {
      hadError = true;
      console.error(
        `rewrites entry ${row.id}: no live candidate_records row (mistyped id, retired, or empty description); nothing was applied for it`
      );
    }
    // Live rows with no audit row were processed (or halted) this run; the
    // summary already accounts for them.
  }
  return hadError;
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
    if (manualRewrites) {
      if (dryRun) {
        // Dry runs must not mutate, so previously-flagged targets stay
        // excluded from the preview — say so instead of previewing a lie.
        const { rows } = await pool.query<{ target_id: string }>(
          `SELECT target_id::text AS target_id FROM public.plain_language_rewrites
            WHERE target_table = 'candidate_records' AND target_column = 'description'
              AND status = 'flagged' AND target_id = ANY($1::uuid[])`,
          [[...manualRewrites.targetIds]]
        );
        for (const row of rows) {
          console.log(
            `note: target ${row.target_id} was flagged in an earlier run; the dry run skips it, an --apply run will clear the flag and retry it`
          );
        }
      } else {
        const cleared = await clearFlaggedAuditRowsForRetry(pool, manualRewrites.targetIds);
        if (cleared > 0) {
          console.log(`cleared ${cleared} flagged audit row(s) for operator retry`);
        }
      }
    }
    const summary = await runPlainLanguageBackfill(pool, {
      rewrite: manualRewrites
        ? async (input) => {
            const entry = manualRewrites.byTargetId.get(input.targetId);
            if (entry === undefined) {
              return {
                ok: false as const,
                reason: `no operator rewrite supplied for target ${input.targetId}`,
              };
            }
            if (entry.originalText !== input.text) {
              return {
                ok: false as const,
                reason: `stored text for target ${input.targetId} does not match the rewrites file's originalText; the row changed since the file was written — re-review before rewriting: ${input.text.slice(0, 120)}`,
              };
            }
            return { ok: true as const, provider: "manual" as const, model: "manual-research", rewrittenText: entry.rewrittenText };
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
    if (manualRewrites && !dryRun) {
      const hadUnmatched = await reportUnmatchedRewriteEntries(
        pool,
        manualRewrites.targetIds,
        summary.processed
      );
      if (hadUnmatched) {
        process.exitCode = 1;
      }
    }
  } finally {
    await pool.end();
  }
}

// Entry guard so tests can import parseManualRewritesFile without running
// the backfill (same pattern as the other manual:* scripts).
if (process.argv[1] && process.argv[1].endsWith("backfillPlainLanguage.ts")) {
  main().catch((error) => {
    console.error("plain-language backfill failed:", error);
    process.exit(1);
  });
}
