import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";
import { createClient } from "redis";

import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import {
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
} from "../config/electionsPipeline.js";
import {
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
  ELECTION_PROMPT_VERSION,
} from "../contracts/electionEnrichmentContract.js";
import { parseCanonicalElectionPayload } from "../contracts/electionPayloadContract.js";

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index >= 0) {
    const value = process.argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  const prefix = `${name}=`;
  const match = process.argv.find((token) => token.startsWith(prefix));
  if (!match) {
    return null;
  }
  const value = match.slice(prefix.length);
  if (value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:elections:inject -- --file payload.json [--ingest-key key] [--run-id id] [--review-approve] [--dry-run]",
    "",
    "Payload must match ElectionEnrichedPayload and will be staged for the existing elections validator.",
    "",
    "--review-approve is the manual equivalent of the AI review retry for validator soft-fails:",
    "the payload must carry review_decision: \"approve\" and a review_reason, and the row is staged",
    "with soft_retry_count already set so the validator's existing review-approve branch applies.",
    "Use it only after a previous inject soft-failed and the reason was researched and found acceptable.",
    "It applies only to scope-validation soft-fails. A payload whose entries were all filtered as",
    "presidential is still rejected regardless of review_decision: presidential contests belong to",
    "presidential_cycles, never district elections, and approval cannot override that.",
  ].join("\n");
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

// The validator only honors payload review_decision on a retry pass
// (soft_retry_count > 0), so a manual review approval must stage the row as if
// the retry already happened. This is the manual equivalent of the AI review
// pass for validator soft-fails.
export function resolveReviewApproveFailureDebugJson(
  payload: { review_decision?: "approve" | "reject"; review_reason?: string },
  reviewApprove: boolean
): string | null {
  if (!reviewApprove) {
    return null;
  }
  if (payload.review_decision !== "approve") {
    throw new Error(
      `--review-approve requires the payload to carry review_decision: "approve".\n${usage()}`
    );
  }
  if (!payload.review_reason?.trim()) {
    throw new Error(
      `--review-approve requires a non-empty payload review_reason documenting why the soft-fail is acceptable.\n${usage()}`
    );
  }
  return JSON.stringify({
    soft_retry_count: 1,
    manual_review_approved: true,
    manual_approve_at: new Date().toISOString(),
  });
}

function extractFamilySourceUrls(payload: unknown): Record<string, string[]> | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const raw = (payload as Record<string, unknown>).family_source_urls;
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return null;
  }

  const allowedFamilies = new Set([
    "all",
    "non_judicial_office",
    "judicial_office",
    "ballot_measure",
    "us_senate",
  ]);
  const normalized: Record<string, string[]> = {};
  for (const [family, urls] of Object.entries(raw as Record<string, unknown>)) {
    if (!allowedFamilies.has(family) || !Array.isArray(urls)) {
      continue;
    }
    const cleanUrls = [
      ...new Set(
        urls
          .filter((url): url is string => typeof url === "string")
          .map((url) => url.trim())
          .filter((url) => url.length > 0)
      ),
    ];
    if (cleanUrls.length > 0) {
      normalized[family] = cleanUrls;
    }
  }

  return Object.keys(normalized).length > 0 ? normalized : null;
}

function defaultIngestKey(districtId: string): string {
  const runYear = new Date().getUTCFullYear();
  return `manual:elections:${districtId}:${runYear}`;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual election injection`);
  }
  return value;
}

async function main(): Promise<void> {
  loadProjectEnv();

  const file = readFlag("--file");
  if (!file) {
    throw new Error(`Missing --file.\n${usage()}`);
  }

  const rawPayload = await readJsonFile(file);
  const parsed = parseCanonicalElectionPayload(rawPayload);
  if (!parsed.ok) {
    throw new Error(`Election payload failed validation: ${parsed.reason}`);
  }

  const dryRun = hasFlag("--dry-run");
  const reviewApprove = hasFlag("--review-approve");
  const failureDebugJson = resolveReviewApproveFailureDebugJson(parsed.payload, reviewApprove);
  const ingestKey =
    readFlag("--ingest-key") ??
    defaultIngestKey(parsed.payload.district_id);
  const runId = readFlag("--run-id") ?? `manual_elections_${new Date().toISOString()}`;
  const payloadJson = JSON.stringify(parsed.payload);
  const familySourceUrls = extractFamilySourceUrls(rawPayload);
  const aiRawDebug = {
    manual_research: true,
    ...(familySourceUrls ? { family_source_urls: familySourceUrls } : {}),
    ...(reviewApprove ? { manual_review_approved: true } : {}),
  };

  if (dryRun) {
    console.log(
      JSON.stringify(
        {
          dryRun: true,
          ingestKey,
          runId,
          districtId: parsed.payload.district_id,
          entryCount: parsed.payload.entries.length,
          familySourceUrlFamilies: familySourceUrls ? Object.keys(familySourceUrls) : [],
          reviewApprove,
        },
        null,
        2
      )
    );
    return;
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const redis = createClient({ url: requireEnv("REDIS_URL") });

  try {
    await redis.connect();

    await pool.query(
      `
        INSERT INTO staging_items (
          ingest_key,
          item_type,
          payload,
          status,
          reason,
          run_id,
          model,
          schema_version,
          prompt_version,
          validated_at,
          written_at,
          failure_debug,
          ai_raw_debug
        )
        VALUES ($1, $2, $3::jsonb, 'pending', NULL, $4, $5, $6, $7, NULL, NULL, $8::jsonb, $9::jsonb)
        ON CONFLICT (ingest_key) DO UPDATE SET
          item_type = EXCLUDED.item_type,
          payload = EXCLUDED.payload,
          status = 'pending',
          reason = NULL,
          run_id = EXCLUDED.run_id,
          model = EXCLUDED.model,
          schema_version = EXCLUDED.schema_version,
          prompt_version = EXCLUDED.prompt_version,
          validated_at = NULL,
          written_at = NULL,
          failure_debug = EXCLUDED.failure_debug,
          ai_raw_debug = EXCLUDED.ai_raw_debug,
          updated_at = now()
      `,
      [
        ingestKey,
        STAGING_ITEM_TYPE_ELECTION,
        payloadJson,
        runId,
        "manual-research:codex",
        ELECTION_ENRICHMENT_SCHEMA_VERSION,
        ELECTION_PROMPT_VERSION,
        failureDebugJson,
        JSON.stringify(aiRawDebug),
      ]
    );

    let redisMessageId: string;
    try {
      redisMessageId = await redis.xAdd(STAGING_PENDING_STREAM, "*", {
        ingest_key: ingestKey,
        item_type: STAGING_ITEM_TYPE_ELECTION,
        run_id: runId,
      });
    } catch (error) {
      await pool.query(
        `
          UPDATE staging_items
          SET status = 'failed',
              reason = $2,
              updated_at = now()
          WHERE ingest_key = $1
            AND item_type = $3
        `,
        [
          ingestKey,
          `manual inject redis publish failed: ${toReason(error)}`,
          STAGING_ITEM_TYPE_ELECTION,
        ]
      );
      throw error;
    }

    console.log(
      JSON.stringify(
        {
          ingestKey,
          runId,
          redisMessageId,
          districtId: parsed.payload.district_id,
          entryCount: parsed.payload.entries.length,
          familySourceUrlFamilies: familySourceUrls ? Object.keys(familySourceUrls) : [],
          reviewApprove,
          next: [
            "npm run elections:validate -- --once",
            "npm run elections:write -- --once",
          ],
        },
        null,
        2
      )
    );
  } finally {
    await redis.quit().catch(() => undefined);
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual elections inject failed:", message);
    process.exitCode = 1;
  });
}
