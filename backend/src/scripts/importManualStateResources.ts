import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { createClient } from "redis";

import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import {
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_PENDING_STREAM,
} from "../config/stateResourcePipeline.js";
import { STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION } from "../contracts/stateResourceEnrichmentContract.js";
import { parseCanonicalStateResourcePayload } from "../contracts/stateResourcePayloadContract.js";
import type { StateResourcePayload } from "../types/stateResource.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

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
    "  npm run manual:state-resources:import -- --file payloads.json [--run-id id] [--dry-run]",
    "",
    "payloads.json holds either one enriched state_resources payload object or an",
    "array of them. Each payload must match the current enrichment contract",
    `(${STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION}), including mail_ballot_request_url,`,
    "mail_ballot_request_type, and per-field sources buckets.",
    "",
    "Rows are staged as pending and published to the state_resources pending",
    "stream; run the production validator and writer afterwards:",
    "  npm run state-resources:validate -- --once",
    "  npm run state-resources:write -- --once",
  ].join("\n");
}

/**
 * Same deterministic per-state key the producer uses, so a manual import and
 * the annual AI refresh share one staging row per state per year.
 */
function buildIngestKey(stateFips: string, runYear: number): string {
  return `state_resources:${stateFips}:${runYear}`;
}

async function readPayloadsFile(path: string): Promise<unknown[]> {
  const raw = await readFile(path, "utf8");
  const parsed = JSON.parse(raw) as unknown;
  if (Array.isArray(parsed)) {
    return parsed;
  }
  return [parsed];
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:state-resources:import", process.argv.slice(2), [
    { name: "--file", value: "space" },
    { name: "--run-id", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();
  requireLocalDatabaseTarget();

  const file = readFlag("--file");
  if (!file) {
    throw new Error(`Missing --file.\n${usage()}`);
  }

  const rawPayloads = await readPayloadsFile(file);
  if (rawPayloads.length === 0) {
    throw new Error(`Payload file is empty.\n${usage()}`);
  }

  const parsedPayloads: StateResourcePayload[] = [];
  const failures: string[] = [];
  for (const [index, rawPayload] of rawPayloads.entries()) {
    const parsed = parseCanonicalStateResourcePayload(rawPayload);
    if (!parsed.ok) {
      const label =
        typeof rawPayload === "object" && rawPayload !== null && "state_abbreviation" in rawPayload
          ? String((rawPayload as Record<string, unknown>).state_abbreviation)
          : `index ${index}`;
      failures.push(`${label}: ${parsed.reason}`);
      continue;
    }
    parsedPayloads.push(parsed.payload);
  }

  if (failures.length > 0) {
    throw new Error(`state_resources payload(s) failed contract parsing:\n- ${failures.join("\n- ")}`);
  }

  const seenFips = new Set<string>();
  for (const payload of parsedPayloads) {
    if (seenFips.has(payload.state_fips)) {
      throw new Error(`Duplicate state_fips ${payload.state_fips} in payload file.`);
    }
    seenFips.add(payload.state_fips);
  }

  const runYear = new Date().getUTCFullYear();
  const runId = readFlag("--run-id") ?? `manual_state_resources_${new Date().toISOString()}`;
  const dryRun = hasFlag("--dry-run");

  if (dryRun) {
    console.log(
      JSON.stringify(
        {
          dryRun: true,
          runId,
          payloads: parsedPayloads.map((payload) => ({
            state_abbreviation: payload.state_abbreviation,
            ingestKey: buildIngestKey(payload.state_fips, runYear),
          })),
        },
        null,
        2
      )
    );
    return;
  }

  const databaseUrl = process.env.DATABASE_URL?.trim();
  const redisUrl = process.env.REDIS_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for manual state_resources import");
  }
  if (!redisUrl) {
    throw new Error("REDIS_URL is required for manual state_resources import");
  }

  const promptVersion = process.env.STATE_RESOURCES_PROMPT_VERSION?.trim() || "state_resources_v2";

  const pool = new Pool({ connectionString: databaseUrl });
  const redis = createClient({ url: redisUrl });
  await redis.connect();

  let staged = 0;
  const stagedKeys: string[] = [];

  try {
    for (const payload of parsedPayloads) {
      const ingestKey = buildIngestKey(payload.state_fips, runYear);
      const payloadJson = JSON.stringify(payload);

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
          STAGING_ITEM_TYPE_STATE_RESOURCES,
          payloadJson,
          runId,
          "manual:claude-research",
          STATE_RESOURCE_ENRICHMENT_SCHEMA_VERSION,
          promptVersion,
          JSON.stringify({ manual_research: true }),
        ]
      );

      try {
        await redis.xAdd(STAGING_PENDING_STREAM, "*", {
          ingest_key: ingestKey,
          item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
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
              AND run_id = $4
          `,
          [
            ingestKey,
            `manual import redis publish failed: ${toReason(error)}`,
            STAGING_ITEM_TYPE_STATE_RESOURCES,
            runId,
          ]
        );
        throw error;
      }

      staged += 1;
      stagedKeys.push(ingestKey);
    }
  } finally {
    await redis.quit();
    await pool.end();
  }

  console.log(
    JSON.stringify(
      {
        staged,
        runId,
        ingestKeys: stagedKeys,
        next: [
          "npm run state-resources:validate -- --once",
          "npm run state-resources:write -- --once",
        ],
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error("manual state_resources import failed:", toReason(error));
  process.exit(1);
});
