// Guarded retirement of a superseded duplicate election shell.
//
// Title-variant duplicates leave a generic shell next to the real
// contest(s): a "Chicago Board of Education Member" shell beside 21
// per-seat contests (ERR-367), a "Governing Board" shell beside its
// per-seat splits (WESD, ERR-343), or a second spelling of the same
// contest. The injector cannot retire them — its upsert identity is
// (district_id, official_ballot_title_key, election_date), so the wrong
// title stays forever. This wrapper DELETES one explicitly identified
// shell after verifying its named replacement contests exist; per the
// 2026-07-16 product decision there is no soft-delete/superseded marker —
// the shell's sources are appended to each survivor so provenance outlives
// the delete.
//
// Guard rails, all of which must pass before the row is deleted:
// - every --superseded-by election exists and shares district_id AND
//   election_date with the retired shell (anything else is not a
//   supersession);
// - ZERO rows in ANY table reference the shell (checked dynamically
//   against every foreign key on public.elections, so candidate links,
//   follows, results, ballot measures, notification events, and every
//   present or future state finance table all block). Links are moved
//   first with manual:candidate-elections:move; anything else (a follow,
//   a result row) is a user decision, not a side effect;
// - local-database guard, row lock, single transaction, --dry-run.
//
// Redis-side state needs no handling here: the results scheduler and
// enrichers re-read the election row by id at execution time, and the
// zero-reference guard means a shell with any polled results cannot be
// deleted in the first place.
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

type QueryResultLike<T> = { rows: T[] };

export type SupersedeElectionClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type SupersedeElectionOptions = {
  electionId: string;
  supersededByIds: string[];
  dryRun: boolean;
};

export type SupersedeElectionResult = {
  dryRun: boolean;
  deletedElectionId: string;
  deletedElectionTitle: string;
  supersededBy: { electionId: string; title: string; sourcesAppended: number }[];
  referencingTablesChecked: number;
};

type ElectionRow = {
  id: string;
  district_id: string;
  election_date: string;
  official_ballot_title: string;
  sources: unknown;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// Same semantics as correctManualElectionDate's appendElectionSource:
// elections.sources is a jsonb array of URL strings everywhere the pipeline
// writes it; non-string entries are dropped, entries are trimmed so
// whitespace variants dedupe.
export function normalizeElectionSources(sources: unknown): string[] {
  return Array.isArray(sources)
    ? sources
        .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        .map((value) => value.trim())
    : [];
}

function usage(): string {
  return [
    "Delete one superseded duplicate election shell after verifying its replacement contests.",
    "",
    "Usage:",
    "  npm run manual:elections:supersede -- --election-id uuid --superseded-by uuid[,uuid...] --reason text [--dry-run]",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) return null;
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value.trim();
}

function requireFlag(name: string): string {
  const value = readFlag(name);
  if (!value) throw new Error(`Missing ${name}.\n${usage()}`);
  return value;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for manual election supersession`);
  return value;
}

/**
 * Every (table, column) pair with a foreign key onto public.elections,
 * straight from the catalog — the blocking check derives from this so a
 * newly added referencing table can never be forgotten.
 */
export async function listElectionFkReferences(
  client: SupersedeElectionClient
): Promise<{ table: string; column: string }[]> {
  const result = await client.query<{ table_name: string; column_name: string }>(
    `
      SELECT DISTINCT c.conrelid::regclass::text AS table_name, a.attname AS column_name
      FROM pg_constraint c
      JOIN unnest(c.conkey) AS k(attnum) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
      WHERE c.contype = 'f'
        AND c.confrelid = 'public.elections'::regclass
      ORDER BY 1, 2
    `
  );
  return result.rows.map((row) => ({ table: row.table_name, column: row.column_name }));
}

export async function runSupersedeElection(
  client: SupersedeElectionClient,
  options: SupersedeElectionOptions
): Promise<SupersedeElectionResult> {
  const { electionId, supersededByIds, dryRun } = options;
  if (supersededByIds.length === 0) {
    throw new Error("--superseded-by must name at least one replacement election");
  }
  if (new Set(supersededByIds).size !== supersededByIds.length) {
    throw new Error("--superseded-by contains duplicate election ids");
  }
  if (supersededByIds.includes(electionId)) {
    throw new Error("--superseded-by must not include the election being retired");
  }

  await client.query("BEGIN");
  try {
    const lockedResult = await client.query<ElectionRow>(
      `
        SELECT id, district_id, election_date::text, official_ballot_title, sources
        FROM public.elections
        WHERE id = $1::uuid
        FOR UPDATE
      `,
      [electionId]
    );
    const retired = lockedResult.rows[0];
    if (!retired) throw new Error(`Election not found: ${electionId}`);

    const survivorsResult = await client.query<ElectionRow>(
      `
        SELECT id, district_id, election_date::text, official_ballot_title, sources
        FROM public.elections
        WHERE id = ANY($1::uuid[])
        FOR UPDATE
      `,
      [supersededByIds]
    );
    const survivorsById = new Map(survivorsResult.rows.map((row) => [row.id, row]));
    for (const id of supersededByIds) {
      const survivor = survivorsById.get(id);
      if (!survivor) throw new Error(`Superseding election not found: ${id}`);
      if (survivor.district_id !== retired.district_id) {
        throw new Error(
          `Superseding election ${id} belongs to a different district; not a supersession`
        );
      }
      if (survivor.election_date !== retired.election_date) {
        throw new Error(
          `Superseding election ${id} has a different date (${survivor.election_date} vs ${retired.election_date}); not a supersession`
        );
      }
    }

    // Zero-reference guard. Identifiers come from the catalog (regclass /
    // pg_attribute), not from user input.
    const references = await listElectionFkReferences(client);
    const blocking: string[] = [];
    for (const { table, column } of references) {
      const countResult = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${table} WHERE ${column} = $1::uuid`,
        [electionId]
      );
      const n = Number(countResult.rows[0]?.n ?? "0");
      if (n > 0) blocking.push(`${table}.${column} (${n})`);
    }
    if (blocking.length > 0) {
      throw new Error(
        `Election ${electionId} is still referenced and cannot be retired: ${blocking.join(", ")}. ` +
          "Move candidate links with manual:candidate-elections:move; anything else is a user decision."
      );
    }

    // Preserve provenance across the delete: the retired shell's sources
    // (often the official page that seeded the split contests) are appended
    // to each survivor.
    const retiredSources = normalizeElectionSources(retired.sources);
    const supersededBy: SupersedeElectionResult["supersededBy"] = [];
    for (const id of supersededByIds) {
      const survivor = survivorsById.get(id)!;
      const existingSources = normalizeElectionSources(survivor.sources);
      const merged = [...new Set([...existingSources, ...retiredSources])];
      const sourcesAppended = merged.length - existingSources.length;
      if (sourcesAppended > 0 && !dryRun) {
        await client.query(
          `UPDATE public.elections SET sources = $2::jsonb, updated_at = now() WHERE id = $1::uuid`,
          [id, JSON.stringify(merged)]
        );
      }
      supersededBy.push({
        electionId: id,
        title: survivor.official_ballot_title,
        sourcesAppended,
      });
    }

    if (!dryRun) {
      await client.query(`DELETE FROM public.elections WHERE id = $1::uuid`, [electionId]);
      await client.query("COMMIT");
    } else {
      await client.query("ROLLBACK");
    }

    return {
      dryRun,
      deletedElectionId: electionId,
      deletedElectionTitle: retired.official_ballot_title,
      supersededBy,
      referencingTablesChecked: references.length,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:elections:supersede", process.argv.slice(2), [
    { name: "--election-id", value: "space" },
    { name: "--superseded-by", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const electionId = requireFlag("--election-id");
  const supersededByRaw = requireFlag("--superseded-by");
  const reason = requireFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");

  const supersededByIds = supersededByRaw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  if (!UUID_RE.test(electionId)) throw new Error(`Invalid --election-id: ${electionId}`);
  for (const id of supersededByIds) {
    if (!UUID_RE.test(id)) throw new Error(`Invalid --superseded-by id: ${id}`);
  }
  if (reason.length < 20) {
    throw new Error("--reason must explain the supersession in at least 20 characters");
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();
  try {
    const result = await runSupersedeElection(client, { electionId, supersededByIds, dryRun });
    console.log(JSON.stringify({ ...result, reason }, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  void main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
