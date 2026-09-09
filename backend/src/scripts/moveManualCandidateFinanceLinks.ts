// Guarded move of one candidate's election-scoped finance rows between two
// of that candidate's elections.
//
// Every state and city finance sync auto-links a committee to a (candidate,
// election) pair read from candidate_elections, denormalises election_id
// (and election_year) onto that link row, and hangs the summaries and
// breakdowns off the link id. When the roster link itself was wrong — a
// duplicate election shell, a district the candidate is not running in, a
// primary they never appeared on — the finance rows sit on the wrong
// election too, and manual:candidate-elections:move / :unlink deliberately
// refuse to strand or contradict them. This wrapper repoints EXACTLY that
// candidate's finance rows from one explicitly identified election to
// another, so the link repair can then run unchanged.
//
// Guard rails, all of which must pass before a single row changes:
// - both elections exist, are row-locked, and share an election YEAR: the
//   finance tables denormalise election_year next to election_id and their
//   totals are computed for that cycle, so a cross-year repoint would make
//   the row lie about its cycle. Dates MAY differ — a primary-shell row
//   moving to the general is a live case;
// - both elections share district_id AND office_id: the link row also stores
//   the district and office name the auto-linker derived from the election,
//   and every batch sync reads them back as matching context (Texas filters
//   outside spending by that district). No wrapper reconciles those columns
//   per state, so a move that would change them is refused;
// - the candidate is linked to the TARGET election in candidate_elections:
//   the batch syncs refresh a finance row only through that join, so a row
//   on an election the roster does not show is exactly the stranding the
//   link wrappers refuse;
// - every table with a foreign key to public.elections and a candidate_id
//   column is handled (catalog scan shared with the link wrappers), except
//   the two user tables (choices, follow-notification events), which are
//   user decisions the link wrappers guard themselves;
// - each table's unique key containing (candidate_id, election_id) comes
//   from the catalog. A from-row whose key already exists on the target is a
//   duplicate of a row the same sync already wrote there (a sibling shell
//   synced twice): the twin WITHOUT dependent rows (summaries, breakdowns —
//   found through the catalog's foreign keys onto the link id) is deleted,
//   the target twin on a tie, so an auto-linked bare target can never
//   cascade away the only populated snapshot; every other row is repointed.
//   A table holding rows for the pair without such a key is refused rather
//   than guessed at;
// - local-database guard, single transaction, --dry-run (executes everything
//   and rolls back, so the reported counts are real).
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { listCandidateScopedElectionFkTables } from "./moveManualCandidateElectionLink.js";

type QueryResultLike<T> = { rows: T[]; rowCount?: number | null };

export type MoveCandidateFinanceLinksClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type MoveCandidateFinanceLinksOptions = {
  candidateId: string;
  fromElectionId: string;
  toElectionId: string;
  dryRun: boolean;
};

export type MoveCandidateFinanceLinksResult = {
  dryRun: boolean;
  candidateId: string;
  fromElectionId: string;
  fromElectionTitle: string;
  toElectionId: string;
  toElectionTitle: string;
  /**
   * One entry per table that held rows for (candidate, from-election).
   * duplicatesDeleted: from-rows the target already held (target twin kept);
   * emptyTargetsReplaced: bare target twins deleted because only the from-row
   * carried dependent rows, which was then repointed in their place.
   */
  tables: { table: string; repointed: number; duplicatesDeleted: number; emptyTargetsReplaced: number }[];
};

type ElectionRow = {
  id: string;
  official_ballot_title: string;
  election_year: number;
  district_id: string;
  office_id: string | null;
};

/** Guarded as USER decisions by the link wrappers; never repointed here. */
const USER_TABLES = new Set(["user_election_choices", "user_candidate_follow_notification_events"]);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function stripPublicSchema(table: string): string {
  return table.startsWith("public.") ? table.slice("public.".length) : table;
}

function usage(): string {
  return [
    "Repoint one candidate's finance-link rows from one of their elections to another of the same year.",
    "",
    "Usage:",
    "  npm run manual:candidate-finance-links:move -- --candidate-id uuid --from-election-id uuid --to-election-id uuid --reason text [--dry-run]",
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
  if (!value) throw new Error(`${name} is required for manual candidate finance-link move`);
  return value;
}

/**
 * The table's unique (or primary) key that contains both candidate_id and
 * election_id, reported as its OTHER columns — the per-source identity
 * (committee_id, filer_id, ...) that decides whether a from-row duplicates a
 * row already on the target. Null when the table has no such key.
 */
export async function findFinanceLinkIdentityKey(
  client: MoveCandidateFinanceLinksClient,
  table: string
): Promise<{ constraintName: string; columns: string[] } | null> {
  const result = await client.query<{ constraint_name: string; column_name: string | null }>(
    `
      WITH key AS (
        SELECT c.conname, c.conkey
        FROM pg_constraint c
        WHERE c.conrelid = $1::regclass
          AND c.contype IN ('u', 'p')
          AND (SELECT attnum FROM pg_attribute WHERE attrelid = c.conrelid AND attname = 'candidate_id' AND NOT attisdropped) = ANY(c.conkey)
          AND (SELECT attnum FROM pg_attribute WHERE attrelid = c.conrelid AND attname = 'election_id' AND NOT attisdropped) = ANY(c.conkey)
        ORDER BY c.conname
        LIMIT 1
      )
      SELECT key.conname AS constraint_name, extra.column_name
      FROM key
      LEFT JOIN LATERAL (
        SELECT quote_ident(a.attname) AS column_name
        FROM unnest(key.conkey) AS k(attnum)
        JOIN pg_attribute a ON a.attrelid = $1::regclass AND a.attnum = k.attnum
        WHERE a.attname NOT IN ('candidate_id', 'election_id')
        ORDER BY k.attnum
      ) extra ON true
    `,
    [table]
  );
  const first = result.rows[0];
  if (!first) return null;
  return {
    constraintName: first.constraint_name,
    columns: result.rows.flatMap((row) => (row.column_name ? [row.column_name] : [])),
  };
}

/**
 * Every foreign key onto the link table's id (single-column, or composite
 * like (link_id, election_year) -> (id, election_year)), reported as the
 * child column that references id — where summaries and breakdowns hang.
 */
export async function listLinkRowChildReferences(
  client: MoveCandidateFinanceLinksClient,
  table: string
): Promise<{ table: string; column: string }[]> {
  const result = await client.query<{ table_name: string; column_name: string }>(
    `
      SELECT DISTINCT c.conrelid::regclass::text AS table_name,
             quote_ident(a.attname) AS column_name
      FROM pg_constraint c
      JOIN unnest(c.conkey, c.confkey) AS u(attnum, fattnum) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
      JOIN pg_attribute fa ON fa.attrelid = c.confrelid AND fa.attnum = u.fattnum
      WHERE c.contype = 'f'
        AND c.confrelid = $1::regclass
        AND fa.attname = 'id'
      ORDER BY 1, 2
    `,
    [table]
  );
  return result.rows.map((row) => ({ table: row.table_name, column: row.column_name }));
}

async function countChildRows(
  client: MoveCandidateFinanceLinksClient,
  children: { table: string; column: string }[],
  rowId: string
): Promise<number> {
  let total = 0;
  for (const { table, column } of children) {
    const result = await client.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM ${table} WHERE ${column} = $1::uuid`,
      [rowId]
    );
    total += Number(result.rows[0]?.n ?? "0");
  }
  return total;
}

export async function runMoveCandidateFinanceLinks(
  client: MoveCandidateFinanceLinksClient,
  options: MoveCandidateFinanceLinksOptions
): Promise<MoveCandidateFinanceLinksResult> {
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
    // Locked in deterministic order, like the link wrappers, so the year
    // guard cannot pass on a row a concurrent date correction is changing.
    const electionsResult = await client.query<ElectionRow>(
      `
        SELECT id, official_ballot_title, extract(year FROM election_date)::int AS election_year,
               district_id, office_id
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
    if (fromElection.election_year !== toElection.election_year) {
      throw new Error(
        `Elections are in different years (${fromElection.election_year} vs ${toElection.election_year}); ` +
          "finance rows carry that cycle's totals and cannot change cycle"
      );
    }
    if (fromElection.district_id !== toElection.district_id || fromElection.office_id !== toElection.office_id) {
      throw new Error(
        "Elections differ in district or office; the finance rows store the district and office name the " +
          "syncs match against, and no wrapper reconciles them per state. Refusing (user decision)."
      );
    }

    const targetLink = await client.query<{ id: string }>(
      `SELECT id FROM public.candidate_elections WHERE candidate_id = $1::uuid AND election_id = $2::uuid`,
      [candidateId, toElectionId]
    );
    if (!targetLink.rows[0]) {
      throw new Error(
        `Candidate ${candidateId} is not linked to election ${toElectionId} in candidate_elections; ` +
          "finance rows follow the roster, so link the candidacy first, then re-run."
      );
    }

    // Identifiers below come from the catalog (regclass / pg_attribute),
    // not from user input.
    const financeTables = (await listCandidateScopedElectionFkTables(client)).filter(
      ({ table }) => !USER_TABLES.has(stripPublicSchema(table))
    );
    const tables: MoveCandidateFinanceLinksResult["tables"] = [];
    const unsupported: string[] = [];
    for (const { table, electionColumn } of financeTables) {
      const countResult = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${table} WHERE candidate_id = $1::uuid AND ${electionColumn} = $2::uuid`,
        [candidateId, fromElectionId]
      );
      if (Number(countResult.rows[0]?.n ?? "0") === 0) continue;

      const key = await findFinanceLinkIdentityKey(client, table);
      if (!key) {
        unsupported.push(table);
        continue;
      }
      // Plain equality, matching the unique key's own semantics (NULLs are
      // distinct), so a from-row is a duplicate only when the target holds
      // a row the constraint would reject on repoint.
      const sameIdentity = key.columns.map((column) => `AND tgt.${column} = src.${column}`).join(" ");
      let duplicatesDeleted = 0;
      let emptyTargetsReplaced = 0;
      const children = await listLinkRowChildReferences(client, table);
      if (children.length === 0) {
        // Nothing hangs off these rows, so the twins are interchangeable.
        const deleted = await client.query(
          `
            DELETE FROM ${table} AS src
            WHERE src.candidate_id = $1::uuid AND src.${electionColumn} = $2::uuid
              AND EXISTS (
                SELECT 1 FROM ${table} AS tgt
                WHERE tgt.candidate_id = $1::uuid AND tgt.${electionColumn} = $3::uuid ${sameIdentity}
              )
          `,
          [candidateId, fromElectionId, toElectionId]
        );
        duplicatesDeleted = deleted.rowCount ?? 0;
      } else {
        // Auto-linking writes a bare link first and the batch sync fills its
        // summaries later, so a target twin can be empty while the from-row
        // holds the only snapshot. Keep whichever twin has dependent rows;
        // on a tie (both or neither) keep the target.
        const pairs = await client.query<{ src_id: string; tgt_id: string }>(
          `
            SELECT src.id AS src_id, tgt.id AS tgt_id
            FROM ${table} AS src
            JOIN ${table} AS tgt
              ON tgt.candidate_id = src.candidate_id AND tgt.${electionColumn} = $3::uuid ${sameIdentity}
            WHERE src.candidate_id = $1::uuid AND src.${electionColumn} = $2::uuid
          `,
          [candidateId, fromElectionId, toElectionId]
        );
        for (const pair of pairs.rows) {
          const sourceChildren = await countChildRows(client, children, pair.src_id);
          const targetChildren = await countChildRows(client, children, pair.tgt_id);
          const dropTarget = sourceChildren > 0 && targetChildren === 0;
          await client.query(`DELETE FROM ${table} WHERE id = $1::uuid`, [dropTarget ? pair.tgt_id : pair.src_id]);
          if (dropTarget) emptyTargetsReplaced += 1;
          else duplicatesDeleted += 1;
        }
      }
      const repointed = await client.query(
        `UPDATE ${table} SET ${electionColumn} = $3::uuid WHERE candidate_id = $1::uuid AND ${electionColumn} = $2::uuid`,
        [candidateId, fromElectionId, toElectionId]
      );
      tables.push({ table, repointed: repointed.rowCount ?? 0, duplicatesDeleted, emptyTargetsReplaced });
    }
    if (unsupported.length > 0) {
      throw new Error(
        `Tables holding rows for this candidacy without a unique key on (candidate_id, election_id, ...), ` +
          `whose duplicates this wrapper cannot detect: ${unsupported.join(", ")}. Refusing; extend the guard first.`
      );
    }
    if (tables.length === 0) {
      throw new Error(
        `No election-scoped finance rows found for candidate ${candidateId} on election ${fromElectionId}; nothing to move`
      );
    }

    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }
    return {
      dryRun,
      candidateId,
      fromElectionId,
      fromElectionTitle: fromElection.official_ballot_title,
      toElectionId,
      toElectionTitle: toElection.official_ballot_title,
      tables,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidate-finance-links:move", process.argv.slice(2), [
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
    const result = await runMoveCandidateFinanceLinks(client, {
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
