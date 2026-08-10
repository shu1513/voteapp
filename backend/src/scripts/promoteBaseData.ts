import { writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  assertConfirmedTarget,
  assertPromotionEndpoints,
  chunk,
  confirmationTokenFor,
  describeEndpoint,
  diffMigrationSets,
  listMigrationFilenames,
  readFlagValue,
  readMigrationSet,
  type PromotionClient,
} from "./promoteResearchData.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Promotes BASE rows — elections, candidates, candidate_elections,
// ballot_measures — from the local database to another database (production),
// in foreign-key order. This is the sibling of promoteResearchData.ts, which
// promotes research children (records, tags, labels) and refuses when their
// parents are missing on the target; this script is how those parents get
// there. The 2026-08-09 prod sync did this by hand with staged \copy scripts;
// this replaces that.
//
// Semantics are deliberately narrower than the research promoter:
//   - INSERT-ONLY, by id. A row whose id already exists on the target is left
//     completely untouched — no update, no diff, no touch of updated_at. Base
//     rows are edited by pipelines on both sides, so "local differs from prod"
//     does not mean "local is right"; reconciling content is a human job.
//   - Never deletes. Production is allowed to be a superset.
//   - FK closure is guarded row by row: a row whose parent is absent on the
//     target AND not being inserted in this same run is SKIPPED and reported,
//     never inserted (the database would reject it) and never fatal (one
//     missing district must not abort promoting thousands of unrelated rows).
//     Skips cascade naturally: a skipped election disqualifies its
//     candidate_elections and ballot_measures in the same pass.

// ---------------------------------------------------------------------------
// Table catalog
//
// Declared in insert order; every FK must point at a table earlier in the
// list, at an external (never-promoted) table, or at the table itself.
// ---------------------------------------------------------------------------

export type FkKind =
  /** Parent table is not promoted by this tool; the parent must already exist on the target. */
  | "external"
  /** Parent table is promoted earlier in this run (or is this table itself); a planned insert satisfies it. */
  | "promoted";

export type FkSpec = {
  column: string;
  parentTable: string;
  kind: FkKind;
};

export type BaseTableSpec = {
  table: string;
  fks: FkSpec[];
  /**
   * SQL predicate over a target row that makes it UNUSABLE as a parent for
   * newly promoted children while still occupying its id. A soft-deleted or
   * merged-away candidate is the live case: its id must never be re-inserted
   * (insert-only means never resurrecting it either), but attaching new
   * candidate_elections to it would file activity under an identity the
   * target has retired. promoteResearchData's findUnresolvableCandidates
   * enforces the same rule for records by aborting; here it is a skip,
   * consistent with how this tool treats absent parents.
   */
  targetUnusableWhenSql?: string;
};

export const BASE_TABLES: readonly BaseTableSpec[] = [
  {
    table: "elections",
    fks: [
      { column: "district_id", parentTable: "districts", kind: "external" },
      { column: "office_id", parentTable: "offices", kind: "external" },
    ],
  },
  {
    table: "candidates",
    // Self-FK: a merged-away candidate points at its survivor. The planner
    // iterates to a fixpoint so a whole merge chain can land in one run, in
    // an order where every survivor precedes the rows that point at it.
    fks: [{ column: "merged_into_candidate_id", parentTable: "candidates", kind: "promoted" }],
    targetUnusableWhenSql: "deleted_at IS NOT NULL OR merged_into_candidate_id IS NOT NULL",
  },
  {
    table: "candidate_elections",
    fks: [
      { column: "candidate_id", parentTable: "candidates", kind: "promoted" },
      { column: "election_id", parentTable: "elections", kind: "promoted" },
      { column: "running_mate_candidate_id", parentTable: "candidates", kind: "promoted" },
    ],
  },
  {
    table: "ballot_measures",
    fks: [
      { column: "district_id", parentTable: "districts", kind: "external" },
      { column: "election_id", parentTable: "elections", kind: "promoted" },
    ],
  },
];

// ---------------------------------------------------------------------------
// Projections
//
// Planning only needs ids and FK values, so the first pass carries just those
// — candidates alone can be six figures of rows, and shipping full rows for
// all of them to decide "already on target" would be waste. Full rows are
// fetched later, only for the ids actually being inserted.
// ---------------------------------------------------------------------------

export type BaseIdRow = {
  id: string;
  /** FK column -> parent id (null when the FK is null). */
  fks: Record<string, string | null>;
};

export function idProjectionSql(spec: BaseTableSpec): string {
  const fkColumns = spec.fks.map((fk) => `${fk.column}::text AS ${fk.column}`);
  return `SELECT id::text AS id${fkColumns.length > 0 ? `, ${fkColumns.join(", ")}` : ""} FROM public.${spec.table}`;
}

export function toBaseIdRow(spec: BaseTableSpec, raw: Record<string, unknown>): BaseIdRow {
  const fks: Record<string, string | null> = {};
  for (const fk of spec.fks) {
    fks[fk.column] = (raw[fk.column] as string | null) ?? null;
  }
  return { id: String(raw.id), fks };
}

/**
 * Target-side id projection. For a table with a usability predicate the
 * projection also reports whether each row can adopt NEW children — the id
 * itself still counts as occupied either way, which is what keeps insert-only
 * from resurrecting a retired row.
 */
export function targetIdProjectionSql(spec: BaseTableSpec): string {
  const unusable = spec.targetUnusableWhenSql
    ? `, (${spec.targetUnusableWhenSql}) AS unusable`
    : "";
  return `SELECT id::text AS id${unusable} FROM public.${spec.table}`;
}

/**
 * Full rows travel as to_jsonb(t), matched back by NAME with
 * jsonb_populate_record on the target — never positionally, so a dropped
 * column or historical column-order difference between the two databases
 * cannot shift values into the wrong columns. to_jsonb also renders dates and
 * timestamps in ISO 8601 regardless of the session's DateStyle, which is the
 * same hazard RECORD_PROJECTION_SQL handles with to_char; timestamptz values
 * carry their UTC offset, so differing session timezones cannot corrupt them
 * either.
 */
export function rowProjectionSql(table: string): string {
  return `SELECT to_jsonb(t) AS row FROM public.${table} AS t WHERE t.id = ANY($1::uuid[])`;
}

// ---------------------------------------------------------------------------
// FK-closure planning
// ---------------------------------------------------------------------------

export type SkipReason =
  /** The parent id does not exist on the target and is not being inserted by this run. */
  | "absent_on_target"
  /** The parent id EXISTS on the target but is soft-deleted or merged away — a retired identity must not gain new children. */
  | "deleted_or_merged_on_target";

export type SkippedRow = {
  id: string;
  /** The FK column that failed closure. */
  column: string;
  /** The parent id that could not be resolved. */
  parentId: string;
  /** Why — the repair differs: absent means promote/repair the parent; retired means resolve the divergence by hand. */
  reason: SkipReason;
};

export type TablePlan = {
  /**
   * Ids to insert, in an order safe for the table's self-FK: every row's
   * in-batch parent appears earlier in the list. Statements are batched, and
   * an immediate FK constraint is checked per statement — so the order must
   * hold across batch boundaries, not just within one.
   */
  insertIds: string[];
  skipped: SkippedRow[];
  alreadyOnTargetCount: number;
  /** Rows on the target with no local counterpart. Reported, never deleted. */
  targetOnlyCount: number;
};

/**
 * Plans one table's inserts under FK closure.
 *
 * A parent reference resolves iff it is null, on the target, or being
 * inserted by this same run (promoted FKs only — an external parent must
 * already exist on the target, because this tool never invents one). The
 * self-FK fixpoint admits merge chains in dependency order; whatever is left
 * unadmitted after the loop stalls has a genuinely unresolvable parent —
 * absent everywhere, itself skipped, or part of a cycle, which the
 * database's own FK ordering could not accept either.
 */
export function planTableInserts(input: {
  spec: BaseTableSpec;
  sourceRows: readonly BaseIdRow[];
  targetIds: ReadonlySet<string>;
  /**
   * Per parent table: ids USABLE as parents on the target (external parents:
   * only the referenced ones need be present in the set). For a parent table
   * with a usability predicate, the caller passes the filtered set — a
   * retired row's id belongs in targetIds (occupied) but not here.
   */
  targetParentIds: ReadonlyMap<string, ReadonlySet<string>>;
  /** Per parent table promoted earlier this run: ids planned for insert. */
  plannedParentIds: ReadonlyMap<string, ReadonlySet<string>>;
  /** Per parent table: target ids excluded from targetParentIds by the usability predicate. Distinguishes the skip reason. */
  unusableParentIds?: ReadonlyMap<string, ReadonlySet<string>>;
}): TablePlan {
  const { spec, sourceRows, targetIds } = input;

  const missing: BaseIdRow[] = [];
  let alreadyOnTargetCount = 0;
  const sourceIds = new Set<string>();
  for (const row of sourceRows) {
    sourceIds.add(row.id);
    if (targetIds.has(row.id)) {
      alreadyOnTargetCount += 1;
    } else {
      missing.push(row);
    }
  }
  let targetOnlyCount = 0;
  for (const id of targetIds) {
    if (!sourceIds.has(id)) {
      targetOnlyCount += 1;
    }
  }

  const selfFkColumns = spec.fks.filter((fk) => fk.parentTable === spec.table).map((fk) => fk.column);
  const admitted = new Set<string>();
  const insertIds: string[] = [];
  let pending = missing;

  // One pass suffices unless the table references itself; then admitting a
  // survivor can unblock the row that points at it, so loop to a fixpoint.
  for (;;) {
    const stillPending: BaseIdRow[] = [];
    for (const row of pending) {
      if (blockingFk(row) === null) {
        admitted.add(row.id);
        insertIds.push(row.id);
      } else {
        stillPending.push(row);
      }
    }
    const progressed = stillPending.length < pending.length;
    pending = stillPending;
    if (pending.length === 0 || !progressed || selfFkColumns.length === 0) {
      break;
    }
  }

  const skipped: SkippedRow[] = pending.map((row) => {
    const fk = blockingFk(row)!;
    const parentId = row.fks[fk.column]!;
    const reason: SkipReason = input.unusableParentIds?.get(fk.parentTable)?.has(parentId)
      ? "deleted_or_merged_on_target"
      : "absent_on_target";
    return { id: row.id, column: fk.column, parentId, reason };
  });

  return { insertIds, skipped, alreadyOnTargetCount, targetOnlyCount };

  function blockingFk(row: BaseIdRow): FkSpec | null {
    for (const fk of spec.fks) {
      const parentId = row.fks[fk.column];
      if (parentId === null || parentId === undefined) {
        continue;
      }
      if (input.targetParentIds.get(fk.parentTable)?.has(parentId)) {
        continue;
      }
      if (fk.kind === "promoted") {
        // Same-table parents must already be ADMITTED, not merely planned:
        // "planned" is only known once this function returns. Earlier tables'
        // planned sets are final by the time this table is planned.
        const plannedSet =
          fk.parentTable === spec.table ? admitted : input.plannedParentIds.get(fk.parentTable);
        if (plannedSet?.has(parentId)) {
          continue;
        }
      }
      return fk;
    }
    return null;
  }
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

/**
 * ON CONFLICT (id) DO NOTHING is a race guard, not an update path: the plan
 * only carries ids the target did not have, so a conflict means the target
 * gained the row after planning. The apply loop compares rows written against
 * rows planned and refuses to commit on a shortfall rather than guessing.
 *
 * A NATURAL-key conflict (the same election under a different id, e.g.
 * uq_elections_district_title_key_date) is deliberately not caught: it
 * errors and rolls the whole run back. Swallowing it would leave the planned
 * child rows pointing at a parent id that never landed.
 */
export function baseInsertSql(table: string): string {
  return `
    INSERT INTO public.${table}
    SELECT (jsonb_populate_record(NULL::public.${table}, e.value)).*
    FROM jsonb_array_elements($1::jsonb) AS e(value)
    ON CONFLICT (id) DO NOTHING
  `;
}

// Table names reach SQL by interpolation, so the two functions that EXECUTE
// queries refuse anything outside the literal catalog. Every call site passes
// a catalog constant today; the guards keep the exported surface safe by
// construction rather than by convention. The pure SQL-string builders above
// stay unguarded — building a string executes nothing.
const PROMOTED_TABLE_NAMES: ReadonlySet<string> = new Set(BASE_TABLES.map((spec) => spec.table));
const EXTERNAL_PARENT_TABLE_NAMES: ReadonlySet<string> = new Set(
  BASE_TABLES.flatMap((spec) =>
    spec.fks.filter((fk) => fk.kind === "external").map((fk) => fk.parentTable)
  )
);

/**
 * Fetches full rows for the planned ids and inserts them, preserving the
 * plan's id order (the self-FK order matters — see TablePlan.insertIds).
 * One id-batch at a time, fetch then insert: chunk preserves plan order, so
 * the ordering guarantee holds across batches while resident memory stays
 * bounded to one batch even on a first promotion into an empty target. A
 * vanished row throws mid-stream; the caller's transaction (apply wraps every
 * table in one) unwinds whatever earlier batches inserted. Returns rows
 * actually written.
 */
export async function insertPlannedRows(input: {
  source: PromotionClient;
  target: PromotionClient;
  table: string;
  insertIds: readonly string[];
}): Promise<number> {
  if (!PROMOTED_TABLE_NAMES.has(input.table)) {
    throw new Error(`insertPlannedRows: refusing to write to unknown table "${input.table}"`);
  }
  const sql = baseInsertSql(input.table);
  let written = 0;
  for (const idBatch of chunk(input.insertIds)) {
    const result = await input.source.query(rowProjectionSql(input.table), [idBatch]);
    const rowById = new Map<string, unknown>();
    for (const raw of result.rows as { row: Record<string, unknown> }[]) {
      rowById.set(String(raw.row.id), raw.row);
    }
    const orderedRows = idBatch.map((id) => {
      const row = rowById.get(id);
      if (row === undefined) {
        throw new Error(
          `Refusing to continue: planned ${input.table} row ${id} vanished from the source between ` +
            "planning and fetch. The source changed under us; re-run."
        );
      }
      return row;
    });
    const inserted = await input.target.query(sql, [JSON.stringify(orderedRows)]);
    written += inserted.rowCount ?? 0;
  }
  return written;
}

/** Ids from the referenced set that exist on the target — presence proof for external parents. */
export async function findPresentIds(
  target: PromotionClient,
  table: string,
  ids: readonly string[]
): Promise<Set<string>> {
  if (!EXTERNAL_PARENT_TABLE_NAMES.has(table)) {
    throw new Error(`findPresentIds: refusing to query unknown table "${table}"`);
  }
  const present = new Set<string>();
  for (const batch of chunk(ids)) {
    const result = await target.query(
      `SELECT id::text AS id FROM public.${table} WHERE id = ANY($1::uuid[])`,
      [batch]
    );
    for (const row of result.rows as { id: string }[]) {
      present.add(row.id);
    }
  }
  return present;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const SCRIPT_LABEL = "promote base data";

function usage(): string {
  return [
    "Usage:",
    "  npm run research:promote:base                  # dry run, writes nothing",
    "  npm run research:promote:base:apply -- --confirm-target <host>:<port>/<database>",
    "",
    "Endpoints:",
    "  source  DATABASE_URL                     (must be local; read-only)",
    "  target  PROMOTION_TARGET_DATABASE_URL    (env only — never a flag, so a",
    "                                            password cannot land in shell",
    "                                            history or the process list)",
  ].join("\n");
}

export type BasePromotionReport = {
  mode: "dry_run" | "apply";
  source: string;
  target: string;
  tables: Record<
    string,
    {
      inserts: number;
      alreadyOnTarget: number;
      targetOnly: number;
      skipped: number;
      /**
       * EVERY skipped row, never a sample: apply mode permits skips and
       * commits, so this list is the operator's only record of what still
       * needs repair. The console prints a truncated copy (consoleReportView);
       * the --report-file copy is always complete.
       */
      skippedRows: SkippedRow[];
      /** Set only on the console copy: how many skippedRows were withheld from this printout. */
      skippedRowsOmitted?: number;
      written?: number;
    }
  >;
};

export const CONSOLE_SKIPPED_ROWS_LIMIT = 10;

/**
 * The console copy of the report. A pathological run (a whole missing
 * district tree) can skip thousands of rows; flooding the terminal with them
 * helps nobody, but silently truncating would misrepresent the run — so the
 * console copy caps skippedRows and says exactly how many it withheld. The
 * report object itself is never mutated: the full version is what lands in
 * --report-file.
 */
export function consoleReportView(report: BasePromotionReport): BasePromotionReport {
  return {
    ...report,
    tables: Object.fromEntries(
      Object.entries(report.tables).map(([table, stats]) => [
        table,
        stats.skippedRows.length > CONSOLE_SKIPPED_ROWS_LIMIT
          ? {
              ...stats,
              skippedRows: stats.skippedRows.slice(0, CONSOLE_SKIPPED_ROWS_LIMIT),
              skippedRowsOmitted: stats.skippedRows.length - CONSOLE_SKIPPED_ROWS_LIMIT,
            }
          : stats,
      ])
    ),
  };
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--apply", value: "none" },
    { name: "--confirm-target", value: "space" },
    { name: "--report-file", value: "space" },
  ]);

  loadProjectEnv();
  const apply = argv.includes("--apply");
  const endpoints = assertPromotionEndpoints({
    sourceUrl: process.env.DATABASE_URL ?? "",
    targetUrl: process.env.PROMOTION_TARGET_DATABASE_URL ?? "",
  });
  if (apply) {
    assertConfirmedTarget(endpoints.target, readFlagValue(argv, "--confirm-target") ?? "");
  }

  console.log(`source: ${describeEndpoint(endpoints.source)}`);
  console.log(`target: ${describeEndpoint(endpoints.target)}`);
  console.log(`mode:   ${apply ? "APPLY (writes)" : "dry run (writes nothing)"}`);

  // Bounded timeouts on BOTH pools. The target for the same reason as
  // promoteResearchData: the apply path holds one transaction across every
  // table's batched inserts, and a hung remote must fail predictably instead
  // of holding locks indefinitely. The source because — unlike the research
  // promoter, which pre-loads everything — insertPlannedRows reads it from
  // INSIDE that open transaction, so a hung local query (say, a migration
  // holding a lock on candidates) would hold the target's locks just as long.
  const sourcePool = new Pool({
    connectionString: process.env.DATABASE_URL,
    connectionTimeoutMillis: 30_000,
    statement_timeout: 300_000,
  });
  const targetPool = new Pool({
    connectionString: process.env.PROMOTION_TARGET_DATABASE_URL,
    connectionTimeoutMillis: 30_000,
    statement_timeout: 300_000,
  });
  const source: PromotionClient = { query: (text, values) => sourcePool.query(text, values as unknown[]) };
  const target: PromotionClient = { query: (text, values) => targetPool.query(text, values as unknown[]) };

  try {
    const migrationDiff = diffMigrationSets({
      source: await readMigrationSet(source),
      target: await readMigrationSet(target),
      knownFilenames: await listMigrationFilenames(),
    });
    if (
      migrationDiff.missingOnSource.length > 0 ||
      migrationDiff.missingOnTarget.length > 0 ||
      migrationDiff.checksumMismatch.length > 0
    ) {
      throw new Error(
        `Refusing to promote across differing schemas: ${JSON.stringify(migrationDiff)}. ` +
          "Run db:migrate on the lagging database first; this tool never migrates."
      );
    }

    // Plan in FK order. Each table's planned-insert set feeds the closure
    // check of every later table (and, for candidates, its own).
    const plans = new Map<string, TablePlan>();
    const plannedParentIds = new Map<string, ReadonlySet<string>>();
    // Per promoted table, captured as each table is planned so later tables
    // can resolve promoted parents without re-querying: ids USABLE as parents
    // on the target, and the retired ids the usability predicate excluded.
    const usableTargetIdSets = new Map<string, ReadonlySet<string>>();
    const unusableTargetIdSets = new Map<string, ReadonlySet<string>>();
    for (const spec of BASE_TABLES) {
      const [sourceRaw, targetRaw] = await Promise.all([
        source.query(idProjectionSql(spec)),
        target.query(targetIdProjectionSql(spec)),
      ]);
      const sourceRows = (sourceRaw.rows as Record<string, unknown>[]).map((raw) => toBaseIdRow(spec, raw));
      // targetIds answers "is this id occupied" (insert-only never touches an
      // occupied id, retired or not); usableTargetIds answers "may a child
      // attach to it". A soft-deleted or merged-away candidate is in the
      // first set and not the second.
      const targetIds = new Set<string>();
      const unusableTargetIds = new Set<string>();
      for (const row of targetRaw.rows as { id: string; unusable?: boolean }[]) {
        targetIds.add(row.id);
        if (row.unusable === true) {
          unusableTargetIds.add(row.id);
        }
      }
      const usableTargetIds =
        unusableTargetIds.size === 0
          ? targetIds
          : new Set([...targetIds].filter((id) => !unusableTargetIds.has(id)));

      // External parents (districts, offices): prove presence of exactly the
      // referenced ids on the target, not the whole table.
      const targetParentIds = new Map<string, ReadonlySet<string>>();
      for (const fk of spec.fks) {
        if (fk.kind !== "external") {
          continue;
        }
        const referenced = [
          ...new Set(
            sourceRows
              .filter((row) => !targetIds.has(row.id))
              .map((row) => row.fks[fk.column])
              .filter((id): id is string => id !== null)
          ),
        ];
        targetParentIds.set(fk.parentTable, await findPresentIds(target, fk.parentTable, referenced));
      }
      // Promoted parents already on the target: reuse the USABLE id sets
      // captured when that table was planned (self-FKs use this table's own).
      const unusableParentIds = new Map<string, ReadonlySet<string>>();
      for (const fk of spec.fks) {
        if (fk.kind !== "promoted" || targetParentIds.has(fk.parentTable)) {
          continue;
        }
        const isSelf = fk.parentTable === spec.table;
        targetParentIds.set(
          fk.parentTable,
          isSelf ? usableTargetIds : usableTargetIdSets.get(fk.parentTable) ?? new Set()
        );
        unusableParentIds.set(
          fk.parentTable,
          isSelf ? unusableTargetIds : unusableTargetIdSets.get(fk.parentTable) ?? new Set()
        );
      }

      const plan = planTableInserts({
        spec,
        sourceRows,
        targetIds,
        targetParentIds,
        plannedParentIds,
        unusableParentIds,
      });
      plans.set(spec.table, plan);
      plannedParentIds.set(spec.table, new Set(plan.insertIds));
      usableTargetIdSets.set(spec.table, usableTargetIds);
      unusableTargetIdSets.set(spec.table, unusableTargetIds);
    }

    const report: BasePromotionReport = {
      mode: apply ? "apply" : "dry_run",
      source: describeEndpoint(endpoints.source),
      target: describeEndpoint(endpoints.target),
      tables: Object.fromEntries(
        BASE_TABLES.map((spec) => {
          const plan = plans.get(spec.table)!;
          return [
            spec.table,
            {
              inserts: plan.insertIds.length,
              alreadyOnTarget: plan.alreadyOnTargetCount,
              targetOnly: plan.targetOnlyCount,
              skipped: plan.skipped.length,
              skippedRows: plan.skipped,
            },
          ];
        })
      ),
    };

    for (const spec of BASE_TABLES) {
      const skippedRows = plans.get(spec.table)!.skipped;
      for (const skip of skippedRows.slice(0, CONSOLE_SKIPPED_ROWS_LIMIT)) {
        console.warn(
          skip.reason === "deleted_or_merged_on_target"
            ? `WARNING: skipping ${spec.table} ${skip.id} — ${skip.column} references ${skip.parentId}, ` +
                "which exists on the target but is soft-deleted or merged away; a retired identity " +
                "must not gain new children. Resolve the divergence by hand."
            : `WARNING: skipping ${spec.table} ${skip.id} — ${skip.column} references ${skip.parentId}, ` +
                "which is absent on the target and not being inserted by this run."
        );
      }
      if (skippedRows.length > CONSOLE_SKIPPED_ROWS_LIMIT) {
        console.warn(
          `WARNING: ${spec.table}: ${skippedRows.length - CONSOLE_SKIPPED_ROWS_LIMIT} more skipped ` +
            "row(s) not shown here; pass --report-file to capture the complete list."
        );
      }
    }

    if (apply) {
      const client: PoolClient = await targetPool.connect();
      try {
        await client.query("BEGIN");
        const wrapped: PromotionClient = { query: (text, values) => client.query(text, values as unknown[]) };
        for (const spec of BASE_TABLES) {
          const plan = plans.get(spec.table)!;
          const written = await insertPlannedRows({
            source,
            target: wrapped,
            table: spec.table,
            insertIds: plan.insertIds,
          });
          if (written !== plan.insertIds.length) {
            throw new Error(
              `Refusing to commit: planned ${plan.insertIds.length} ${spec.table} insert(s) but the ` +
                `target accepted ${written}. The target changed since the plan was computed; re-run.`
            );
          }
          report.tables[spec.table]!.written = written;
        }
        await client.query("COMMIT");
      } catch (error) {
        // Best-effort rollback, same shape as promoteResearchData: a failed
        // ROLLBACK must not mask the error that explains the failure.
        await client.query("ROLLBACK").catch((rollbackError: unknown) => {
          console.error(
            `ROLLBACK failed after the error below; the transaction's state on the target is unknown: ${
              rollbackError instanceof Error ? rollbackError.message : String(rollbackError)
            }`
          );
        });
        throw error;
      } finally {
        client.release();
      }
    }

    const reportFile = readFlagValue(argv, "--report-file");
    if (reportFile) {
      // The file gets the FULL report — every skipped row. Only the console
      // copy is truncated.
      await writeFile(reportFile, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    }
    console.log(JSON.stringify(consoleReportView(report), null, 2));
    if (!apply) {
      console.log(
        "\nDry run only — nothing was written. Re-run with:\n" +
          `  npm run research:promote:base:apply -- --confirm-target ${confirmationTokenFor(endpoints.target)}`
      );
    }
  } finally {
    await Promise.allSettled([sourcePool.end(), targetPool.end()]);
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint && import.meta.url === entrypoint) {
  main().catch((error: unknown) => {
    console.error(`${SCRIPT_LABEL} failed: ${error instanceof Error ? error.message : String(error)}`);
    console.error(usage());
    process.exit(1);
  });
}
