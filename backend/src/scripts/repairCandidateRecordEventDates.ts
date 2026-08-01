import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { parseRecordEventDate } from "../contracts/recordEventDate.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";

/**
 * Rewrites the `event_date` of EXISTING candidate_records in place.
 *
 * Why this script has to exist: like a corrected URL, a corrected date never
 * matches the row it is meant to fix — `findSimilarExistingRecord` locates
 * rows by (candidate_id, event_date, source_url), so re-importing the repaired
 * record INSERTS a second row and leaves the bad one behind. The observed
 * defect class is research-date substitution: rows stamped with the date the
 * research happened instead of the date the action did (wave 18: four rows
 * carrying 2026-05-26 for a page published 2026-03-10).
 *
 * Why it is not a raw SQL UPDATE: `record_identity_key` hashes the normalized
 * URL, event date and description under a UNIQUE (candidate_id,
 * record_identity_key) constraint; an UPDATE that changed event_date alone
 * would leave the key describing content the row no longer has.
 *
 * The claim and its citation are deliberately immutable here: this script only
 * ever changes WHEN the recorded action happened, never what the record says
 * or where it is cited from. origin / origin_run_id are NOT re-stamped for the
 * same reason the URL repair script leaves them: the claim still came from the
 * run that introduced it, and re-attributing would rotate the row out of its
 * poisoned-cohort query.
 *
 * Usage:
 *   npm run manual:records:repair-event-dates -- --repairs-file <path>
 *   npm run manual:records:repair-event-dates -- --repairs-file <path> --apply
 *
 * Dry run is the default; --apply performs the writes.
 */

type RepairInput = {
  recordId: string;
  eventDate: string;
  note?: string;
};

type RepairOutcome =
  | { recordId: string; status: "repaired"; from: string; to: string; note?: string }
  | { recordId: string; status: "would_repair"; from: string; to: string; note?: string }
  | { recordId: string; status: "skipped"; reason: string };

// event_date is read as ::text, never as a pg Date — node-postgres parses a
// DATE into a JS Date at LOCAL midnight and the identity key derives its date
// through toISOString() (UTC), which shifts the date a day on hosts east of
// UTC. See the RecordRow note in repairCandidateRecordSourceUrls.ts; here the
// stored date feeds only the report, but the same lie in the type would let a
// future edit feed it into the key.
type RecordRow = {
  id: string;
  candidate_id: string;
  description: string;
  source_url: string;
  event_date: string;
  retired_at: string | null;
};

function parseArgs(argv: readonly string[]): { repairsFile: string; apply: boolean } {
  let repairsFile = "";
  let apply = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--repairs-file") {
      repairsFile = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (arg === "--apply") {
      apply = true;
      continue;
    }
    throw new Error(`unknown flag(s): ${arg}`);
  }
  if (!repairsFile) {
    throw new Error("--repairs-file <path> is required");
  }
  return { repairsFile, apply };
}

export function parseRepairsFile(raw: string): RepairInput[] {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("repairs file must contain a JSON array");
  }
  return parsed.map((entry, index) => {
    if (typeof entry !== "object" || entry === null) {
      throw new Error(`repairs[${index}] must be an object`);
    }
    const { recordId, eventDate, note } = entry as Record<string, unknown>;
    if (typeof recordId !== "string" || recordId.trim().length === 0) {
      throw new Error(`repairs[${index}].recordId must be a non-empty string`);
    }
    // The replacement clears the same date gate a fresh write would
    // (recordEventDate.ts is shared with the discovery and source-repair
    // contracts so this path cannot become an escape hatch): full calendar
    // date, real date, not in the future.
    const parsedDate = parseRecordEventDate(eventDate);
    if (!parsedDate.ok) {
      throw new Error(`repairs[${index}].${parsedDate.reason}`);
    }
    return {
      recordId: recordId.trim(),
      eventDate: parsedDate.eventDate,
      ...(typeof note === "string" ? { note } : {}),
    };
  });
}

/**
 * Injected the same way as repairCandidateRecordSourceUrls' RepairDeps: the
 * gates are the point of the script, and the pg-Date timezone defect proved
 * they can be silently wrong — they earn direct coverage.
 */
export type RepairDeps = {
  loadRecord: (recordId: string) => Promise<RecordRow | null>;
  findIdentityCollision: (input: {
    candidateId: string;
    identityKey: string;
    excludeRecordId: string;
  }) => Promise<string | null>;
  /**
   * Compare-and-swap: updates only if description, event_date and source_url
   * still hold the values the identity key was computed from. Returns the
   * number of rows changed.
   */
  applyRepair: (input: {
    recordId: string;
    eventDate: string;
    identityKey: string;
    expected: { description: string; eventDate: string; sourceUrl: string };
  }) => Promise<number>;
};

export async function repairOneEventDate(
  repair: RepairInput,
  deps: RepairDeps,
  options: { apply: boolean }
): Promise<RepairOutcome> {
  const row = await deps.loadRecord(repair.recordId);
  if (!row) {
    return { recordId: repair.recordId, status: "skipped", reason: "record not found" };
  }
  if (row.retired_at !== null) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `record is retired (${row.retired_at}); repairing a withdrawn claim is moot`,
    };
  }
  if (row.event_date === repair.eventDate) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: "event_date already matches (already repaired)",
    };
  }

  const identityKey = buildCandidateRecordIdentityKey({
    description: row.description,
    sourceUrl: row.source_url,
    eventDate: repair.eventDate,
  });

  // If the repaired identity already exists for this candidate, the fix would
  // collide with the UNIQUE constraint: the correctly-dated record is already
  // stored on another row, making this one a duplicate. Removing a duplicate
  // is a retire decision (manual:records:retire), never this script's.
  const collidingRecordId = await deps.findIdentityCollision({
    candidateId: row.candidate_id,
    identityKey,
    excludeRecordId: row.id,
  });
  if (collidingRecordId) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `repaired identity already exists on record ${collidingRecordId} — this row is a duplicate; retire it instead of re-dating it`,
    };
  }

  if (!options.apply) {
    return {
      recordId: repair.recordId,
      status: "would_repair",
      from: row.event_date,
      to: repair.eventDate,
      ...(repair.note ? { note: repair.note } : {}),
    };
  }

  // Compare-and-swap on exactly the columns the identity key was computed
  // from; a concurrent write between read and update changes nothing instead
  // of leaving the key describing content the row no longer has.
  const updated = await deps.applyRepair({
    recordId: row.id,
    eventDate: repair.eventDate,
    identityKey,
    expected: {
      description: row.description,
      eventDate: row.event_date,
      sourceUrl: row.source_url,
    },
  });
  if (updated !== 1) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason:
        "record changed after it was read (concurrent write); nothing was modified — re-run to pick up the current content",
    };
  }

  return {
    recordId: repair.recordId,
    status: "repaired",
    from: row.event_date,
    to: repair.eventDate,
    ...(repair.note ? { note: repair.note } : {}),
  };
}

function buildPoolDeps(pool: Pool): RepairDeps {
  return {
    loadRecord: async (recordId) => {
      const result = await pool.query<RecordRow>(
        `SELECT id, candidate_id, description, source_url, event_date::text AS event_date, retired_at::text AS retired_at
           FROM public.candidate_records
          WHERE id = $1`,
        [recordId]
      );
      const row = result.rows[0];
      if (!row) {
        return null;
      }
      // Same loud failure as the URL repair script: drop the ::text cast and
      // pg hands back a Date while the type keeps claiming string.
      if (typeof row.event_date !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(row.event_date)) {
        throw new Error(
          `record ${recordId}: event_date must be selected as text (got ${typeof row.event_date}: ${String(row.event_date)})`
        );
      }
      return row;
    },
    findIdentityCollision: async ({ candidateId, identityKey, excludeRecordId }) => {
      const result = await pool.query<{ id: string }>(
        `SELECT id
           FROM public.candidate_records
          WHERE candidate_id = $1 AND record_identity_key = $2 AND id <> $3`,
        [candidateId, identityKey, excludeRecordId]
      );
      return result.rows[0]?.id ?? null;
    },
    applyRepair: async ({ recordId, eventDate, identityKey, expected }) => {
      const result = await pool.query(
        `UPDATE public.candidate_records
            SET event_date = $2::date,
                record_identity_key = $3,
                updated_at = now()
          WHERE id = $1
            AND description = $4
            AND event_date = $5::date
            AND source_url = $6
            AND retired_at IS NULL`,
        [
          recordId,
          eventDate,
          identityKey,
          expected.description,
          expected.eventDate,
          expected.sourceUrl,
        ]
      );
      return result.rowCount ?? 0;
    },
  };
}

async function main(): Promise<void> {
  const { repairsFile, apply } = parseArgs(process.argv.slice(2));
  const repairs = parseRepairsFile(await readFile(repairsFile, "utf8"));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate record event-date repair");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  const deps = buildPoolDeps(pool);
  const outcomes: RepairOutcome[] = [];

  try {
    for (const repair of repairs) {
      try {
        outcomes.push(await repairOneEventDate(repair, deps, { apply }));
      } catch (error) {
        // One bad row must not abandon the batch: in --apply mode some rows
        // are already written by this point, and losing the report would
        // leave nobody knowing which.
        outcomes.push({
          recordId: repair.recordId,
          status: "skipped",
          reason: `repair failed: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
    }
  } finally {
    // The report prints BEFORE pool teardown: in --apply mode some rows are
    // already written by now, and a pool.end() rejection after the loop would
    // otherwise discard the only account of which.
    const counts = outcomes.reduce<Record<string, number>>((acc, outcome) => {
      acc[outcome.status] = (acc[outcome.status] ?? 0) + 1;
      return acc;
    }, {});
    console.log(JSON.stringify({ mode: apply ? "apply" : "dry-run", counts, outcomes }, null, 2));
    await pool.end();
  }

  for (const outcome of outcomes) {
    if (outcome.status === "skipped") {
      process.exitCode = 1;
    }
  }
}

if (process.argv[1] && process.argv[1].endsWith("repairCandidateRecordEventDates.ts")) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
