import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { rewriteToPlainLanguage, verifyPlainLanguageRewrite } from "../ai/rewritePlainLanguage.js";
import { runPlainLanguageBackfill } from "../pipeline/content/plainLanguageBackfill.js";

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
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const summary = await runPlainLanguageBackfill(pool, {
      rewrite: rewriteToPlainLanguage,
      verify: verifyPlainLanguageRewrite,
      aiConfig: {
        timeoutMs: env.AI_TIMEOUT_MS,
        openAiApiKey: env.OPENAI_API_KEY,
        anthropicApiKey: env.ANTHROPIC_API_KEY,
        geminiApiKey: env.GEMINI_API_KEY,
      },
      dryRun,
      ...(limit !== undefined ? { limit } : {}),
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
