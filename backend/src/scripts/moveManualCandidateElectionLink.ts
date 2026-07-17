// Guarded single-link move between sibling election shells.
//
// Title-variant duplicate shells (a generic "Governing Board" shell next to
// its per-seat contests, or two spellings of the same contest) leave
// candidate_elections rows pointing at the wrong shell. The election
// injector can never repair that: its upsert identity is
// (district_id, official_ballot_title_key, election_date), so reinjecting
// under the right title creates the right shell but strands the links on
// the wrong one. This wrapper moves EXACTLY ONE explicitly identified link
// to an explicitly identified sibling shell — no heuristics, no bulk mode —
// so an operator can empty a duplicate shell link by link before retiring
// it with manual:elections:supersede.
//
// Guard rails, all of which must pass before a single row changes:
// - the link (candidate, from-election) exists and is row-locked;
// - both elections exist, are row-locked, and share district_id AND
//   election_date (crossing either means this is not a duplicate-shell
//   repair — wrong tool);
// - the from-election has no persisted election_results rows: their
//   winners JSON references candidate_elections ids, which a move or
//   merge-delete would corrupt;
// - no state campaign-finance link rows exist for (candidate, from-election):
//   finance tables denormalize election_id + election_year and their
//   summaries join on the election, so a bare link move would strand them
//   (checked dynamically against every FK table so new finance states are
//   covered automatically);
// - if the candidate is already linked to the target shell, the move
//   converges only when both rows agree on status/is_incumbent/running mate
//   (the from-link is then deleted as a duplicate); disagreeing rows are a
//   research question, not a merge;
// - before that duplicate-merge delete, no rows may reference the from-link
//   id itself: tables that FK onto candidate_elections(id) (e.g.
//   fl_candidate_finance_outside_group_links) carry no candidate_id or
//   election_id of their own, so the election-scoped finance scan above
//   cannot see them, and ON DELETE CASCADE would silently take their rows
//   with the link (checked dynamically against the catalog; FK shapes the
//   check cannot count — composite, or not referencing id — are refused
//   outright);
// - running-mate uniqueness on the target shell is pre-checked so the move
//   cannot trip uq_candidate_elections_election_running_mate_candidate_id;
// - local-database guard, single transaction, --dry-run.
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

type QueryResultLike<T> = { rows: T[] };

export type MoveCandidateElectionLinkClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type MoveCandidateElectionLinkOptions = {
  candidateId: string;
  fromElectionId: string;
  toElectionId: string;
  dryRun: boolean;
};

export type MoveCandidateElectionLinkResult = {
  action: "moved" | "merged_duplicate";
  dryRun: boolean;
  candidateId: string;
  fromElectionId: string;
  fromElectionTitle: string;
  toElectionId: string;
  toElectionTitle: string;
};

type LinkRow = {
  id: string;
  is_incumbent: boolean;
  status: string;
  running_mate_candidate_id: string | null;
};

type ElectionRow = {
  id: string;
  district_id: string;
  election_date: string;
  official_ballot_title: string;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function usage(): string {
  return [
    "Move one candidate_elections link from a duplicate election shell to its sibling.",
    "",
    "Usage:",
    "  npm run manual:candidate-elections:move -- --candidate-id uuid --from-election-id uuid --to-election-id uuid --reason text [--dry-run]",
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
  if (!value) throw new Error(`${name} is required for manual candidate-election link move`);
  return value;
}

/**
 * Every table that (a) has a foreign key to public.elections and (b) also
 * carries a candidate_id column is treated as a per-candidate finance/link
 * table whose rows would be stranded by a bare link move. Discovered from
 * the catalog at run time so a newly added state finance table is covered
 * without touching this wrapper.
 */
export async function listCandidateScopedElectionFkTables(
  client: MoveCandidateElectionLinkClient
): Promise<{ table: string; electionColumn: string }[]> {
  const result = await client.query<{ table_name: string; election_column: string }>(
    `
      SELECT DISTINCT c.conrelid::regclass::text AS table_name,
             quote_ident(a.attname) AS election_column
      FROM pg_constraint c
      JOIN unnest(c.conkey) AS k(attnum) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
      WHERE c.contype = 'f'
        AND c.confrelid = 'public.elections'::regclass
        AND c.conrelid <> 'public.candidate_elections'::regclass
        AND EXISTS (
          SELECT 1 FROM pg_attribute ca
          WHERE ca.attrelid = c.conrelid AND ca.attname = 'candidate_id' AND NOT ca.attisdropped
        )
      ORDER BY 1
    `
  );
  return result.rows.map((row) => ({ table: row.table_name, electionColumn: row.election_column }));
}

/**
 * Every foreign key onto public.candidate_elections references link rows the
 * duplicate-merge delete would cascade (or orphan), and none of them is
 * visible to the election-scoped scan above. conkey and confkey are unnested
 * together (positional pairing) so each child column is reported with the
 * link column it actually references: the guard can only count single-column
 * FKs onto id, and must refuse any other shape rather than compare the wrong
 * column against the link id and under-count. Discovered from the catalog at
 * run time so a newly added link-scoped table is covered without touching
 * this wrapper.
 */
export async function listCandidateElectionLinkFkReferences(
  client: MoveCandidateElectionLinkClient
): Promise<
  { constraintName: string; table: string; column: string; referencedColumn: string; columnCount: number }[]
> {
  const result = await client.query<{
    constraint_name: string;
    table_name: string;
    column_name: string;
    referenced_column: string;
    column_count: number;
  }>(
    `
      SELECT c.conname AS constraint_name,
             c.conrelid::regclass::text AS table_name,
             quote_ident(a.attname) AS column_name,
             fa.attname AS referenced_column,
             cardinality(c.conkey) AS column_count
      FROM pg_constraint c
      JOIN unnest(c.conkey, c.confkey) WITH ORDINALITY AS u(attnum, fattnum, ord) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
      JOIN pg_attribute fa ON fa.attrelid = c.confrelid AND fa.attnum = u.fattnum
      WHERE c.contype = 'f'
        AND c.confrelid = 'public.candidate_elections'::regclass
      ORDER BY 1, 2, 3
    `
  );
  return result.rows.map((row) => ({
    constraintName: row.constraint_name,
    table: row.table_name,
    column: row.column_name,
    referencedColumn: row.referenced_column,
    columnCount: Number(row.column_count),
  }));
}

export async function runMoveCandidateElectionLink(
  client: MoveCandidateElectionLinkClient,
  options: MoveCandidateElectionLinkOptions
): Promise<MoveCandidateElectionLinkResult> {
  // PostgreSQL returns uuid columns lowercased; a valid uppercase input
  // would otherwise fail the row-matching below with a false "not found".
  const candidateId = options.candidateId.toLowerCase();
  const fromElectionId = options.fromElectionId.toLowerCase();
  const toElectionId = options.toElectionId.toLowerCase();
  const { dryRun } = options;
  if (fromElectionId === toElectionId) {
    throw new Error("--from-election-id and --to-election-id must differ");
  }

  await client.query("BEGIN");
  try {
    const linkResult = await client.query<LinkRow>(
      `
        SELECT id, is_incumbent, status, running_mate_candidate_id
        FROM public.candidate_elections
        WHERE candidate_id = $1::uuid AND election_id = $2::uuid
        FOR UPDATE
      `,
      [candidateId, fromElectionId]
    );
    const link = linkResult.rows[0];
    if (!link) {
      throw new Error(
        `No candidate_elections link found for candidate ${candidateId} on election ${fromElectionId}`
      );
    }

    // Locked so the district/date sibling guard cannot pass on stale rows
    // while a concurrent edit (e.g. manual:election-date:correct) changes
    // one of them mid-transaction. Deterministic order avoids deadlocks
    // between two concurrent movers.
    const electionsResult = await client.query<ElectionRow>(
      `
        SELECT id, district_id, election_date::text, official_ballot_title
        FROM public.elections
        WHERE id = ANY($1::uuid[])
        ORDER BY id
        FOR UPDATE
      `,
      [[fromElectionId, toElectionId]]
    );
    const fromElection = electionsResult.rows.find((row) => row.id === fromElectionId);
    const toElection = electionsResult.rows.find((row) => row.id === toElectionId);
    if (!fromElection) throw new Error(`Election not found: ${fromElectionId}`);
    if (!toElection) throw new Error(`Election not found: ${toElectionId}`);
    if (fromElection.district_id !== toElection.district_id) {
      throw new Error(
        "Elections belong to different districts; a cross-district move is not a duplicate-shell repair"
      );
    }
    if (fromElection.election_date !== toElection.election_date) {
      throw new Error(
        `Elections have different dates (${fromElection.election_date} vs ${toElection.election_date}); ` +
          "a cross-date move is not a duplicate-shell repair (see manual:election-date:correct for date fixes)"
      );
    }

    // Persisted-results guard: election_results.winners stores
    // candidate_election_id inside JSON (the matcher resolves winners by
    // exact link id against the election's roster), and the FK/finance scan
    // below cannot see into JSON. Moving a link out from under a persisted
    // result — or deleting it on the merge path — would leave the winner
    // JSON pointing at another election's link, or dangling. Same rule as
    // manual:election-date:correct: resolve the result rows first.
    const persistedResults = await client.query<{ id: string }>(
      `SELECT id FROM public.election_results WHERE election_id = $1::uuid LIMIT 1`,
      [fromElectionId]
    );
    if (persistedResults.rows[0]) {
      throw new Error(
        `Election ${fromElectionId} has persisted election_results rows whose winners reference ` +
          "candidate_elections ids; refusing link move — resolve the result rows first (user decision), then re-run."
      );
    }

    // Finance-link stranding guard. Identifiers come from the catalog
    // (regclass / pg_attribute), not from user input.
    const financeTables = await listCandidateScopedElectionFkTables(client);
    const stranded: string[] = [];
    for (const { table, electionColumn } of financeTables) {
      const countResult = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${table} WHERE candidate_id = $1::uuid AND ${electionColumn} = $2::uuid`,
        [candidateId, fromElectionId]
      );
      const n = Number(countResult.rows[0]?.n ?? "0");
      if (n > 0) stranded.push(`${table} (${n})`);
    }
    if (stranded.length > 0) {
      throw new Error(
        `Candidate has election-scoped rows on the from-election that a link move would strand: ${stranded.join(", ")}. ` +
          "Resolve those rows first (user decision), then re-run."
      );
    }

    const targetLinkResult = await client.query<LinkRow>(
      `
        SELECT id, is_incumbent, status, running_mate_candidate_id
        FROM public.candidate_elections
        WHERE candidate_id = $1::uuid AND election_id = $2::uuid
        FOR UPDATE
      `,
      [candidateId, toElectionId]
    );
    const targetLink = targetLinkResult.rows[0];

    let action: MoveCandidateElectionLinkResult["action"];
    if (targetLink) {
      const identical =
        targetLink.is_incumbent === link.is_incumbent &&
        targetLink.status === link.status &&
        targetLink.running_mate_candidate_id === link.running_mate_candidate_id;
      if (!identical) {
        throw new Error(
          "Candidate is already linked to the target election and the two links disagree " +
            `(from: status=${link.status}, is_incumbent=${link.is_incumbent}, running_mate=${link.running_mate_candidate_id ?? "null"}; ` +
            `to: status=${targetLink.status}, is_incumbent=${targetLink.is_incumbent}, running_mate=${targetLink.running_mate_candidate_id ?? "null"}). ` +
            "Which row is right is a research question; resolve it, then re-run."
        );
      }
      // Link-scoped cascade guard: rows that FK onto the from-link's id
      // (invisible to the election-scoped scan above) would be silently
      // cascaded away by the duplicate-merge delete. Identifiers come from
      // the catalog, not from user input.
      const linkFkReferences = await listCandidateElectionLinkFkReferences(client);
      // The count below keys each child column on the from-link id, which is
      // only meaningful for a single-column FK onto id. Any other shape
      // (composite, or referencing another unique column) would compare the
      // wrong value, count zero, and let the delete cascade — so it is
      // refused outright instead of guessed at.
      const unsupported = [
        ...new Set(
          linkFkReferences
            .filter((ref) => ref.columnCount !== 1 || ref.referencedColumn !== "id")
            .map((ref) => `${ref.table}.${ref.constraintName}`)
        ),
      ];
      if (unsupported.length > 0) {
        throw new Error(
          `Foreign keys onto candidate_elections whose shape this guard cannot check ` +
            `(composite, or not referencing id): ${unsupported.join(", ")}. ` +
            "Refusing the duplicate-merge delete; extend the guard before merging under such constraints."
        );
      }
      const cascading: string[] = [];
      const counted = new Set<string>();
      for (const { table, column } of linkFkReferences) {
        const key = `${table}.${column}`;
        if (counted.has(key)) continue;
        counted.add(key);
        const countResult = await client.query<{ n: string }>(
          `SELECT count(*)::text AS n FROM ${table} WHERE ${column} = $1::uuid`,
          [link.id]
        );
        const n = Number(countResult.rows[0]?.n ?? "0");
        if (n > 0) cascading.push(`${key} (${n})`);
      }
      if (cascading.length > 0) {
        throw new Error(
          `From-link ${link.id} is referenced by rows the duplicate-merge delete would cascade away: ${cascading.join(", ")}. ` +
            "Resolve those rows first (user decision), then re-run."
        );
      }
      if (!dryRun) {
        await client.query(`DELETE FROM public.candidate_elections WHERE id = $1::uuid`, [link.id]);
      }
      action = "merged_duplicate";
    } else {
      if (link.running_mate_candidate_id) {
        // FOR UPDATE pins a found collision row for the friendly error; a
        // concurrent INSERT of a colliding row after a not-found result is
        // still possible (no predicate locks at read committed) and falls
        // through to the unique-constraint error, which keeps data correct.
        const mateCollision = await client.query<{ id: string }>(
          `
            SELECT id FROM public.candidate_elections
            WHERE election_id = $1::uuid AND running_mate_candidate_id = $2::uuid
            LIMIT 1
            FOR UPDATE
          `,
          [toElectionId, link.running_mate_candidate_id]
        );
        if (mateCollision.rows[0]) {
          throw new Error(
            `Target election already has a link carrying running mate ${link.running_mate_candidate_id} ` +
              `(link ${mateCollision.rows[0].id}); resolve the duplicate ticket first, then re-run.`
          );
        }
      }
      if (!dryRun) {
        await client.query(
          `
            UPDATE public.candidate_elections
            SET election_id = $2::uuid, updated_at = now()
            WHERE id = $1::uuid
          `,
          [link.id, toElectionId]
        );
      }
      action = "moved";
    }

    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }
    return {
      action,
      dryRun,
      candidateId,
      fromElectionId,
      fromElectionTitle: fromElection.official_ballot_title,
      toElectionId,
      toElectionTitle: toElection.official_ballot_title,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidate-elections:move", process.argv.slice(2), [
    { name: "--candidate-id", value: "space" },
    { name: "--from-election-id", value: "space" },
    { name: "--to-election-id", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const candidateId = requireFlag("--candidate-id");
  const fromElectionId = requireFlag("--from-election-id");
  const toElectionId = requireFlag("--to-election-id");
  const reason = requireFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");

  for (const [name, value] of [
    ["--candidate-id", candidateId],
    ["--from-election-id", fromElectionId],
    ["--to-election-id", toElectionId],
  ] as const) {
    if (!UUID_RE.test(value)) throw new Error(`Invalid ${name}: ${value}`);
  }
  if (reason.length < 20) {
    throw new Error("--reason must explain the move in at least 20 characters");
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();
  try {
    const result = await runMoveCandidateElectionLink(client, {
      candidateId,
      fromElectionId,
      toElectionId,
      dryRun,
    });
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
