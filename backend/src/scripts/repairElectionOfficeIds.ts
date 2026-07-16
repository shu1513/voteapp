// Re-run the office matcher over stranded office-race election shells.
//
// Elections written before a catalog office/alias (or matcher fix) existed
// carry office_id = NULL, which hard-blocks the candidate-records stage for
// every candidate linked to them. The elections upsert backfills office_id on
// re-inject (COALESCE), but a re-inject needs the original payload; this
// wrapper is the payload-free repair path: it re-resolves each stranded
// shell's stored official ballot title through the CURRENT OfficeMatcher
// (same inputs the elections writer uses — district scope/name/state, title,
// stored discovery contest family) and backfills office_id in place.
//
// Rows the matcher still cannot resolve are reported, never guessed: no
// title text is altered, no confidence floor is bypassed. Learned aliases
// are persisted exactly as the elections writer persists them, so a repaired
// title form matches directly on the next fresh injection.
//
// Scope guard: only future-dated shells (US-latest local date boundary) are
// touched — past shells are historical records whose repair is a separate
// user decision.
import { pathToFileURL } from "node:url";

import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { OfficeMatcher } from "../pipeline/elections/officeMatcher.js";
import type { ElectionDistrictType } from "../types/election.js";
import { US_LATEST_LOCAL_DATE_SQL } from "../utils/usLocalDate.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

export type OfficeRepairClient = Pick<PoolClient, "query">;

export type OfficeRepairOptions = {
  dryRun: boolean;
};

type StrandedElectionRow = {
  id: string;
  official_ballot_title: string;
  discovery_contest_family: string | null;
  district_name: string;
  state: string;
  district_type: ElectionDistrictType;
};

type OfficeNameRow = {
  id: string;
  canonical_name: string;
};

export type OfficeRepairMatch = {
  electionId: string;
  scope: ElectionDistrictType;
  officialBallotTitle: string;
  officeId: string;
  officeCanonicalName: string;
  method: "alias_exact" | "deterministic_fallback";
  confidence: number;
};

export type OfficeRepairUnmatched = {
  electionId: string;
  scope: ElectionDistrictType;
  officialBallotTitle: string;
  method: "none" | "ambiguous";
  confidence: number;
};

export type OfficeRepairSummary = {
  dryRun: boolean;
  examined: number;
  repaired: OfficeRepairMatch[];
  unmatched: OfficeRepairUnmatched[];
  aliasRowsPersisted: number;
};

export async function runElectionOfficeIdRepair(
  client: OfficeRepairClient,
  options: OfficeRepairOptions
): Promise<OfficeRepairSummary> {
  await client.query("BEGIN");
  try {
    const stranded = await client.query<StrandedElectionRow>(
      `
        SELECT
          e.id,
          e.official_ballot_title,
          e.discovery_contest_family,
          d.name AS district_name,
          d.state,
          d.district_type
        FROM public.elections e
        JOIN public.districts d ON d.id = e.district_id
        WHERE e.office_id IS NULL
          AND e.race_type = 'office'
          AND e.election_date >= ${US_LATEST_LOCAL_DATE_SQL}
        ORDER BY d.district_type, d.state, d.name, e.official_ballot_title
        FOR UPDATE OF e
      `
    );

    const officeNames = await client.query<OfficeNameRow>(
      `SELECT id, canonical_name FROM public.offices`
    );
    const officeNameById = new Map(
      (officeNames.rows ?? []).map((row) => [row.id, row.canonical_name])
    );

    const matcher = new OfficeMatcher(client);
    const repaired: OfficeRepairMatch[] = [];
    const unmatched: OfficeRepairUnmatched[] = [];
    // Same dedupe the elections writer applies to learned aliases: one row
    // per (scope, matcher key) per run; ON CONFLICT covers cross-run repeats.
    const aliasRowsToInsert: Array<{
      office_id: string;
      scope: string;
      alias_text: string;
      normalized_alias: string;
    }> = [];
    const seenAliasKeys = new Set<string>();

    for (const row of stranded.rows ?? []) {
      const match = await matcher.resolve({
        scope: row.district_type,
        districtName: row.district_name,
        state: row.state,
        officialBallotTitle: row.official_ballot_title,
        discoveryContestFamily:
          (row.discovery_contest_family as OfficeRepairMatchInputFamily) ?? undefined,
      });

      if (!match.officeId || match.method === "none" || match.method === "ambiguous") {
        unmatched.push({
          electionId: row.id,
          scope: row.district_type,
          officialBallotTitle: row.official_ballot_title,
          method: match.method === "ambiguous" ? "ambiguous" : "none",
          confidence: match.confidence,
        });
        continue;
      }

      await client.query(
        `
          UPDATE public.elections
          SET office_id = $2::uuid,
              updated_at = now()
          WHERE id = $1::uuid
            AND office_id IS NULL
        `,
        [row.id, match.officeId]
      );

      repaired.push({
        electionId: row.id,
        scope: row.district_type,
        officialBallotTitle: row.official_ballot_title,
        officeId: match.officeId,
        officeCanonicalName: officeNameById.get(match.officeId) ?? match.officeId,
        method: match.method,
        confidence: match.confidence,
      });

      if (match.shouldPersistAlias && match.aliasMemoryKey.length > 0) {
        const aliasKey = `${row.district_type}::${match.aliasMemoryKey}`;
        if (!seenAliasKeys.has(aliasKey)) {
          seenAliasKeys.add(aliasKey);
          aliasRowsToInsert.push({
            office_id: match.officeId,
            scope: row.district_type,
            alias_text: row.official_ballot_title,
            normalized_alias: match.aliasMemoryKey,
          });
        }
      }
    }

    if (aliasRowsToInsert.length > 0) {
      await client.query(
        `
          INSERT INTO public.office_title_aliases (
            office_id,
            scope,
            alias_text,
            normalized_alias
          )
          SELECT a.office_id, a.scope, a.alias_text, a.normalized_alias
          FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[]) AS a(
            office_id,
            scope,
            alias_text,
            normalized_alias
          )
          ON CONFLICT (scope, normalized_alias) DO NOTHING
        `,
        [
          aliasRowsToInsert.map((row) => row.office_id),
          aliasRowsToInsert.map((row) => row.scope),
          aliasRowsToInsert.map((row) => row.alias_text),
          aliasRowsToInsert.map((row) => row.normalized_alias),
        ]
      );
    }

    if (options.dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }

    return {
      dryRun: options.dryRun,
      examined: stranded.rows?.length ?? 0,
      repaired,
      unmatched,
      aliasRowsPersisted: aliasRowsToInsert.length,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

// The matcher accepts the payload contract's contest-family union; the stored
// column is free text from that same contract, so the cast is a narrowing of
// values the writer itself persisted.
type OfficeRepairMatchInputFamily = Parameters<
  OfficeMatcher["resolve"]
>[0]["discoveryContestFamily"];

function usage(): string {
  return [
    "Re-run the office matcher over future office-race elections with office_id = NULL",
    "and backfill the ones the current catalog/matcher can resolve.",
    "",
    "Usage:",
    "  npm run manual:elections:repair-office-ids -- [--dry-run]",
  ].join("\n");
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for office-id repair.\n${usage()}`);
  return value;
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:elections:repair-office-ids", process.argv.slice(2), [
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const dryRun = process.argv.includes("--dry-run");
  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();

  try {
    const summary = await runElectionOfficeIdRepair(client, { dryRun });

    for (const match of summary.repaired) {
      console.log(
        `repaired scope=${match.scope} method=${match.method} confidence=${match.confidence.toFixed(3)} ` +
          `title=${JSON.stringify(match.officialBallotTitle)} office=${JSON.stringify(match.officeCanonicalName)} ` +
          `election_id=${match.electionId}`
      );
    }
    for (const miss of summary.unmatched) {
      console.log(
        `unmatched scope=${miss.scope} method=${miss.method} confidence=${miss.confidence.toFixed(3)} ` +
          `title=${JSON.stringify(miss.officialBallotTitle)} election_id=${miss.electionId}`
      );
    }
    console.log(
      `office-id repair summary dry_run=${summary.dryRun} examined=${summary.examined} ` +
        `repaired=${summary.repaired.length} unmatched=${summary.unmatched.length} ` +
        `alias_rows=${summary.aliasRowsPersisted}`
    );
  } finally {
    client.release();
    await pool.end();
  }
}

const isDirectExecution = import.meta.url === pathToFileURL(process.argv[1] ?? "").href;
if (isDirectExecution) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
