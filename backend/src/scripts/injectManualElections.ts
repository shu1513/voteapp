import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { createClient } from "redis";

import { loadProjectEnv } from "../config/env.js";
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
    return process.argv[index + 1] ?? null;
  }
  const prefix = `${name}=`;
  const match = process.argv.find((token) => token.startsWith(prefix));
  return match ? match.slice(prefix.length) : null;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:elections:inject -- --file payload.json [--ingest-key key] [--run-id id] [--dry-run]",
    "",
    "Payload must match ElectionEnrichedPayload and will be staged for the existing elections validator.",
  ].join("\n");
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
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
  const ingestKey =
    readFlag("--ingest-key") ??
    `manual:elections:${parsed.payload.district_id}:${new Date().toISOString()}`;
  const runId = readFlag("--run-id") ?? `manual_elections_${new Date().toISOString()}`;
  const payloadJson = JSON.stringify(parsed.payload);
  const familySourceUrls = extractFamilySourceUrls(rawPayload);
  const aiRawDebug = {
    manual_research: true,
    ...(familySourceUrls ? { family_source_urls: familySourceUrls } : {}),
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
        },
        null,
        2
      )
    );
    return;
  }

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp",
  });
  const redis = createClient({ url: process.env.REDIS_URL ?? "redis://localhost:6379" });

  try {
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
        VALUES ($1, $2, $3::jsonb, 'pending', NULL, $4, $5, $6, $7, NULL, NULL, NULL, $8::jsonb)
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
          failure_debug = NULL,
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
        JSON.stringify(aiRawDebug),
      ]
    );

    await redis.connect();
    const redisMessageId = await redis.xAdd(STAGING_PENDING_STREAM, "*", {
      ingest_key: ingestKey,
      item_type: STAGING_ITEM_TYPE_ELECTION,
      run_id: runId,
      payload: payloadJson,
    });

    console.log(
      JSON.stringify(
        {
          ingestKey,
          runId,
          redisMessageId,
          districtId: parsed.payload.district_id,
          entryCount: parsed.payload.entries.length,
          familySourceUrlFamilies: familySourceUrls ? Object.keys(familySourceUrls) : [],
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

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual elections inject failed:", message);
  process.exitCode = 1;
});
