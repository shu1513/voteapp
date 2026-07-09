import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";
import { createClient } from "redis";

import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { parseCanonicalElectionPayload } from "../contracts/electionPayloadContract.js";
import { stageManualElectionPayload } from "./injectManualElections.js";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:elections:inject-no-results -- --districts-file ids.txt [--dry-run]",
    "",
    "Bulk stages verified no-election districts (one district id per line; blank lines and",
    "# comments ignored) as entries: [] election payloads — the exact payload a single",
    "manual:elections:inject would stage — in one process instead of thousands of wrapper",
    "invocations. Built for structurally election-free districts like Census-designated",
    "places (CDPs). The no-contest fact must be verified per district class before running;",
    "this wrapper does not verify it for you.",
    "",
    "Districts that already have a FUTURE election row in the DB are skipped and reported:",
    "staging no_results for them would contradict known data.",
    "Existing non-terminal or written staging rows are also skipped so curated payloads are preserved.",
    "",
    "After injecting, drain with:",
    "  npm run elections:validate -- --once --batch-size=<count>",
    "  npm run elections:write -- --once --batch-size=<count>",
    "(check redis-cli XLEN staging:elections:pending first — a drain processes everything pending)",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index >= 0) {
    const value = process.argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  return null;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual election injection`);
  }
  return value;
}

async function readDistrictIdsFile(path: string): Promise<string[]> {
  const raw = await readFile(path, "utf8");
  const ids = raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));
  const unique = [...new Set(ids)];
  if (unique.length === 0) {
    throw new Error(`No district ids found in ${path}.`);
  }
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const invalid = unique.filter((id) => !uuidPattern.test(id));
  if (invalid.length > 0) {
    throw new Error(`Invalid district id(s) in ${path}: ${invalid.slice(0, 5).join(", ")}`);
  }
  return unique;
}

const REPLACEABLE_STAGING_STATUSES = new Set(["failed", "rejected", "no_results"]);

type BulkNoResultFailure = {
  districtId: string;
  reason: string;
};

export type BulkNoResultSummary = {
  staged: number;
  missingDistricts: string[];
  skippedWithFutureElections: string[];
  skippedWithExistingStaging: Array<{ districtId: string; status: string }>;
  failed: BulkNoResultFailure[];
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

export async function bulkInjectManualNoResultElections(
  pool: Pool,
  redis: ReturnType<typeof createClient> | null,
  options: {
    districtIds: string[];
    dryRun: boolean;
    runId: string;
    runYear: number;
  }
): Promise<BulkNoResultSummary> {
  const { districtIds, dryRun, runId, runYear } = options;
  if (!dryRun && !redis) {
    throw new Error("Redis client is required outside dry-run mode");
  }

  const districts = await pool.query<{
    id: string;
    name: string;
    district_type: string;
    state: string;
    has_future_election: boolean;
    existing_staging_status: string | null;
  }>(
    `
      SELECT d.id, d.name, d.district_type, d.state,
             EXISTS (
               SELECT 1 FROM public.elections e
               WHERE e.district_id = d.id AND e.election_date >= CURRENT_DATE
             ) AS has_future_election,
             (
               SELECT si.status
               FROM staging_items si
               WHERE si.ingest_key = 'manual:elections:' || d.id::text || ':' || $2::text
                 AND si.item_type = 'election'
             ) AS existing_staging_status
      FROM public.districts d
      WHERE d.id = ANY($1::uuid[])
    `,
    [districtIds, runYear]
  );
  const districtById = new Map(districts.rows.map((row) => [row.id, row]));

  let staged = 0;
  const missingDistricts: string[] = [];
  const skippedWithFutureElections: string[] = [];
  const skippedWithExistingStaging: Array<{ districtId: string; status: string }> = [];
  const failed: BulkNoResultFailure[] = [];

  for (const districtId of districtIds) {
    const district = districtById.get(districtId);
    if (!district) {
      missingDistricts.push(districtId);
      continue;
    }
    if (district.has_future_election) {
      skippedWithFutureElections.push(districtId);
      continue;
    }
    if (
      district.existing_staging_status &&
      !REPLACEABLE_STAGING_STATUSES.has(district.existing_staging_status)
    ) {
      skippedWithExistingStaging.push({
        districtId,
        status: district.existing_staging_status,
      });
      continue;
    }

    try {
      const parsed = parseCanonicalElectionPayload({
        district_id: district.id,
        district_name: district.name,
        district_type: district.district_type,
        state: district.state,
        entries: [],
      });
      if (!parsed.ok) {
        throw new Error(`payload failed validation: ${parsed.reason}`);
      }

      if (!dryRun && redis) {
        const result = await stageManualElectionPayload(pool, redis, {
          ingestKey: `manual:elections:${district.id}:${runYear}`,
          runId,
          payloadJson: JSON.stringify(parsed.payload),
          failureDebugJson: null,
          aiRawDebugJson: JSON.stringify({ manual_research: true, bulk_no_results: true }),
          overwriteExisting: false,
        });
        if (!result.staged) {
          skippedWithExistingStaging.push({
            districtId,
            status: district.existing_staging_status ?? "changed_concurrently",
          });
          continue;
        }
      }
      staged += 1;
    } catch (error) {
      failed.push({ districtId, reason: toReason(error) });
    }
  }

  return {
    staged,
    missingDistricts,
    skippedWithFutureElections,
    skippedWithExistingStaging,
    failed,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();

  const districtsFile = readFlag("--districts-file");
  if (!districtsFile) {
    throw new Error(`Missing --districts-file.\n${usage()}`);
  }
  const dryRun = hasFlag("--dry-run");
  const districtIds = await readDistrictIdsFile(districtsFile);

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const redis = dryRun ? null : createClient({ url: requireEnv("REDIS_URL") });

  const runId = `manual_elections_no_results_bulk_${new Date().toISOString()}`;
  const runYear = new Date().getUTCFullYear();

  try {
    if (redis) {
      await redis.connect();
    }

    const summary = await bulkInjectManualNoResultElections(pool, redis, {
      districtIds,
      dryRun,
      runId,
      runYear,
    });

    console.log(
      JSON.stringify(
        {
          dryRun,
          runId,
          requested: districtIds.length,
          ...summary,
          next: dryRun
            ? []
            : [
                "npm run elections:validate -- --once --batch-size=<count>",
                "npm run elections:write -- --once --batch-size=<count>",
              ],
        },
        null,
        2
      )
    );

    if (
      summary.missingDistricts.length > 0 ||
      summary.skippedWithFutureElections.length > 0 ||
      summary.skippedWithExistingStaging.length > 0 ||
      summary.failed.length > 0
    ) {
      process.exitCode = 1;
    }
  } finally {
    if (redis) {
      await redis.quit().catch(() => undefined);
    }
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("manual bulk no-results inject failed:", toReason(error));
    process.exitCode = 1;
  });
}
