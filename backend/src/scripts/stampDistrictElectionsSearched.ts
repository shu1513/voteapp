import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

import { assertKnownCliFlags } from "./manualCliFlags.js";

/**
 * Stamps districts.last_elections_searched_at for a district whose research
 * pass verified there is nothing WRITABLE right now — e.g. every discovered
 * contest was deferred (roster not final) or out of scope — without staging
 * an entries: [] payload, which would assert the stronger "no elections
 * exist" claim (that path is manual:elections:inject-no-results).
 *
 * Guards:
 * - Refuses when the district already has a future election row: the writer
 *   stamped the district when that row was written, and "nothing writable"
 *   contradicts known data.
 * - Refuses when an election staging row for the district is still in flight
 *   (pending/validated): the write that drains it stamps the district itself.
 *
 * Usage: npm run manual:districts:stamp-searched -- --district-id uuid --reason "why nothing was writable" [--dry-run]
 */

type Queryable = Pick<PoolClient, "query">;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function usage(): string {
  return [
    "Usage:",
    '  npm run manual:districts:stamp-searched -- --district-id uuid --reason "why nothing was writable" [--dry-run]',
    "",
    "Stamps districts.last_elections_searched_at = now() for a district verified to have",
    "nothing writable this pass (e.g. all contests deferred). Does NOT assert that no",
    "elections exist — use manual:elections:inject-no-results for structurally",
    "election-free districts. The reason is echoed in the output for the run report;",
    "it is not persisted.",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) {
    return null;
  }
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--") || value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value.trim();
}

export type StampDistrictSearchedResult = {
  dryRun: boolean;
  districtId: string;
  districtName: string;
  districtType: string;
  state: string;
  previousLastElectionsSearchedAt: string | null;
  stamped: boolean;
};

export async function runStampDistrictElectionsSearched(
  client: Queryable,
  options: { districtId: string; dryRun: boolean }
): Promise<StampDistrictSearchedResult> {
  const { districtId, dryRun } = options;

  const district = await client.query<{
    id: string;
    name: string;
    district_type: string;
    state: string;
    last_elections_searched_at: string | null;
    has_future_election: boolean;
    in_flight_staging_status: string | null;
  }>(
    `
      SELECT d.id::text AS id, d.name, d.district_type, d.state,
             d.last_elections_searched_at::text AS last_elections_searched_at,
             EXISTS (
               SELECT 1 FROM public.elections e
               WHERE e.district_id = d.id AND e.election_date >= CURRENT_DATE
             ) AS has_future_election,
             (
               SELECT si.status
               FROM staging_items si
               WHERE si.item_type = 'election'
                 AND si.ingest_key LIKE 'manual:elections:' || d.id::text || ':%'
                 AND si.status IN ('pending', 'validated')
               LIMIT 1
             ) AS in_flight_staging_status
      FROM public.districts d
      WHERE d.id = $1::uuid
    `,
    [districtId]
  );

  const row = district.rows[0];
  if (!row) {
    throw new Error(`District not found: ${districtId}`);
  }
  if (row.has_future_election) {
    throw new Error(
      `District ${districtId} (${row.name}) has a future election row — "nothing writable" contradicts known data, and the writer already stamps the district when elections are written. Nothing to do.`
    );
  }
  if (row.in_flight_staging_status) {
    throw new Error(
      `District ${districtId} (${row.name}) has an election staging row in flight (status=${row.in_flight_staging_status}). Drain it first (elections:validate / elections:write) — the write stamps the district itself.`
    );
  }

  if (!dryRun) {
    await client.query(
      `
        UPDATE public.districts
        SET last_elections_searched_at = now()
        WHERE id = $1::uuid
      `,
      [districtId]
    );
  }

  return {
    dryRun,
    districtId: row.id,
    districtName: row.name,
    districtType: row.district_type,
    state: row.state,
    previousLastElectionsSearchedAt: row.last_elections_searched_at,
    stamped: !dryRun,
  };
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:districts:stamp-searched", process.argv.slice(2), [
    { name: "--district-id", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const districtId = readFlag("--district-id");
  const reason = readFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");
  if (!districtId || !UUID_PATTERN.test(districtId)) {
    throw new Error(`--district-id must be a UUID.\n${usage()}`);
  }
  if (!reason) {
    throw new Error(
      `--reason is required: document why nothing was writable this pass.\n${usage()}`
    );
  }

  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for manual district stamp");
  }
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const result = await runStampDistrictElectionsSearched(pool, { districtId, dryRun });
    console.log(JSON.stringify({ ...result, reason }, null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual district stamp failed:", message);
    process.exitCode = 1;
  });
}
