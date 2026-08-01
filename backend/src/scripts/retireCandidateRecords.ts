import { readFile } from "node:fs/promises";

import { Pool } from "pg";

/**
 * Soft-retires EXISTING candidate_records rows whose CLAIM was judged wrong or
 * unsupportable — wrong attribution, an unsupported claim, a stale-by-design
 * aggregate. These are defects the repair scripts cannot fix: repairing a URL
 * or a date corrects WHERE or WHEN a fact is cited from, while retirement
 * withdraws the fact itself.
 *
 * Why soft (retired_at/retired_reason) and not DELETE: notification events and
 * area tags cascade on record deletion, so a hard delete erases the audit
 * trail of notifications already sent about the row. The retired row also
 * keeps its (candidate_id, record_identity_key) slot — a later sweep that
 * re-derives the same claim folds into the retired row (the store's upserts
 * never clear retired_at) and stays hidden instead of silently resurrecting a
 * withdrawn claim. Read paths exclude retired rows; migration 202 states the
 * full read rule.
 *
 * origin / origin_run_id are deliberately NOT touched: provenance identifies
 * the run that INTRODUCED the claim, and a retired bad claim is exactly the
 * kind of row a poisoned-cohort query (WHERE origin_run_id = ...) must still
 * find.
 *
 * Usage:
 *   npm run manual:records:retire -- --retirements-file <path>
 *   npm run manual:records:retire -- --retirements-file <path> --apply
 *
 * Dry run is the default; --apply performs the writes.
 */

type RetirementInput = {
  recordId: string;
  reason: string;
  note?: string;
};

type RetirementOutcome =
  | { recordId: string; status: "retired"; description: string; reason: string; note?: string }
  | { recordId: string; status: "would_retire"; description: string; reason: string; note?: string }
  | { recordId: string; status: "skipped"; reason: string };

type RecordRow = {
  id: string;
  description: string;
  source_url: string;
  event_date: string;
  retired_at: string | null;
};

function parseArgs(argv: readonly string[]): { retirementsFile: string; apply: boolean } {
  let retirementsFile = "";
  let apply = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--retirements-file") {
      retirementsFile = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (arg === "--apply") {
      apply = true;
      continue;
    }
    throw new Error(`unknown flag(s): ${arg}`);
  }
  if (!retirementsFile) {
    throw new Error("--retirements-file <path> is required");
  }
  return { retirementsFile, apply };
}

export function parseRetirementsFile(raw: string): RetirementInput[] {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("retirements file must contain a JSON array");
  }
  return parsed.map((entry, index) => {
    if (typeof entry !== "object" || entry === null) {
      throw new Error(`retirements[${index}] must be an object`);
    }
    const { recordId, reason, note } = entry as Record<string, unknown>;
    if (typeof recordId !== "string" || recordId.trim().length === 0) {
      throw new Error(`retirements[${index}].recordId must be a non-empty string`);
    }
    // The reason is stored to retired_reason and is what makes the withdrawal
    // reviewable later; a placeholder defeats the point.
    const trimmedReason = typeof reason === "string" ? reason.trim() : "";
    if (trimmedReason.length < 10) {
      throw new Error(
        `retirements[${index}].reason must state why the claim is withdrawn (at least 10 characters)`
      );
    }
    return {
      recordId: recordId.trim(),
      reason: trimmedReason,
      ...(typeof note === "string" ? { note } : {}),
    };
  });
}

/**
 * Injected the same way as repairCandidateRecordSourceUrls' RepairDeps: the
 * gates are the point of the script and earn direct coverage without a
 * database.
 */
export type RetireDeps = {
  loadRecord: (recordId: string) => Promise<RecordRow | null>;
  /**
   * Compare-and-swap: retires only if description, event_date and source_url
   * still hold the values the operator reviewed, and the row is not already
   * retired. Returns the number of rows changed.
   */
  applyRetirement: (input: {
    recordId: string;
    reason: string;
    expected: { description: string; eventDate: string; sourceUrl: string };
  }) => Promise<number>;
};

export async function retireOneRecord(
  retirement: RetirementInput,
  deps: RetireDeps,
  options: { apply: boolean }
): Promise<RetirementOutcome> {
  const row = await deps.loadRecord(retirement.recordId);
  if (!row) {
    return { recordId: retirement.recordId, status: "skipped", reason: "record not found" };
  }
  if (row.retired_at !== null) {
    return {
      recordId: retirement.recordId,
      status: "skipped",
      reason: `already retired at ${row.retired_at}`,
    };
  }

  if (!options.apply) {
    return {
      recordId: retirement.recordId,
      status: "would_retire",
      description: row.description,
      reason: retirement.reason,
      ...(retirement.note ? { note: retirement.note } : {}),
    };
  }

  // The operator's retire decision was made about the content they reviewed.
  // Read and update are separate statements, so a concurrent writer could
  // replace the claim in between — retiring the NEW content would withdraw a
  // claim nobody reviewed. Guarding the UPDATE means that race skips instead.
  const updated = await deps.applyRetirement({
    recordId: row.id,
    reason: retirement.reason,
    expected: {
      description: row.description,
      eventDate: row.event_date,
      sourceUrl: row.source_url,
    },
  });
  if (updated !== 1) {
    return {
      recordId: retirement.recordId,
      status: "skipped",
      reason:
        "record changed after it was read (concurrent write); nothing was modified — review the current content and re-run",
    };
  }

  return {
    recordId: retirement.recordId,
    status: "retired",
    description: row.description,
    reason: retirement.reason,
    ...(retirement.note ? { note: retirement.note } : {}),
  };
}

function buildPoolDeps(pool: Pool): RetireDeps {
  return {
    loadRecord: async (recordId) => {
      const result = await pool.query<RecordRow>(
        `SELECT id, description, source_url, event_date::text AS event_date, retired_at::text AS retired_at
           FROM public.candidate_records
          WHERE id = $1`,
        [recordId]
      );
      return result.rows[0] ?? null;
    },
    applyRetirement: async ({ recordId, reason, expected }) => {
      const result = await pool.query(
        `UPDATE public.candidate_records
            SET retired_at = now(),
                retired_reason = $2,
                updated_at = now()
          WHERE id = $1
            AND retired_at IS NULL
            AND description = $3
            AND event_date = $4::date
            AND source_url = $5`,
        [recordId, reason, expected.description, expected.eventDate, expected.sourceUrl]
      );
      return result.rowCount ?? 0;
    },
  };
}

async function main(): Promise<void> {
  const { retirementsFile, apply } = parseArgs(process.argv.slice(2));
  const retirements = parseRetirementsFile(await readFile(retirementsFile, "utf8"));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate record retirement");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  const deps = buildPoolDeps(pool);
  const outcomes: RetirementOutcome[] = [];

  try {
    for (const retirement of retirements) {
      try {
        outcomes.push(await retireOneRecord(retirement, deps, { apply }));
      } catch (error) {
        // One bad row must not abandon the batch: in --apply mode some rows
        // are already retired by this point, and losing the report would
        // leave nobody knowing which.
        outcomes.push({
          recordId: retirement.recordId,
          status: "skipped",
          reason: `retirement failed: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
    }
  } finally {
    await pool.end();
  }

  const counts = outcomes.reduce<Record<string, number>>((acc, outcome) => {
    acc[outcome.status] = (acc[outcome.status] ?? 0) + 1;
    return acc;
  }, {});

  console.log(JSON.stringify({ mode: apply ? "apply" : "dry-run", counts, outcomes }, null, 2));

  for (const outcome of outcomes) {
    if (outcome.status === "skipped") {
      process.exitCode = 1;
    }
  }
}

if (process.argv[1] && process.argv[1].endsWith("retireCandidateRecords.ts")) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
