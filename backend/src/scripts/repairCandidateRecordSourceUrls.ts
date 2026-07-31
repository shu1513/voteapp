import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { verifyHttpUrlReachability } from "../ai/urlReachability.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";
import { evaluateCandidateRecordSourcePolicy } from "../pipeline/candidates/candidateRecordSourcePolicy.js";

/**
 * Rewrites the `source_url` of EXISTING candidate_records in place.
 *
 * Why this script has to exist: the manual/AI record writers can only add
 * records. `findSimilarExistingRecord` locates the row to update by
 * (candidate_id, event_date, source_url), so a corrected URL never matches the
 * row it is meant to fix — re-importing a repaired citation INSERTS a second
 * row and leaves the bad one behind. Repairing a citation is therefore not
 * expressible through the normal write path.
 *
 * Why it is not a raw SQL UPDATE: `record_identity_key` is a hash over the
 * normalized URL, event date and description, under a UNIQUE
 * (candidate_id, record_identity_key) constraint. An UPDATE that changed
 * source_url alone would leave every repaired row carrying a key that no
 * longer describes its own content, silently breaking dedupe and re-import.
 *
 * The claim itself is deliberately immutable here: this script only ever
 * changes WHERE a fact is cited from, never what the record says. A changed
 * description is a research question and belongs in a research pass.
 *
 * Usage:
 *   npm run manual:records:repair-source-urls -- --repairs-file <path>
 *   npm run manual:records:repair-source-urls -- --repairs-file <path> --apply
 *
 * Dry run is the default; --apply performs the writes.
 */

type RepairInput = {
  recordId: string;
  sourceUrl: string;
  note?: string;
};

type RepairOutcome =
  | { recordId: string; status: "repaired"; from: string; to: string }
  | { recordId: string; status: "would_repair"; from: string; to: string }
  | { recordId: string; status: "skipped"; reason: string };

/**
 * `event_date` is read as `::text`, never as a pg `Date`. node-postgres parses
 * a DATE into a JS Date at LOCAL midnight, and the identity key derives its
 * date through `toISOString()` — which is UTC. On any host east of UTC that
 * round trip moves the date back a day (verified: TZ=Europe/Berlin turns
 * 2025-10-21 into 2025-10-20 and changes the key). This script writes the key
 * but NOT event_date, so a shifted key would encode a date the row does not
 * have — the exact content/key divergence the script exists to prevent. Taking
 * the calendar date as text keeps the key pinned to what is actually stored.
 */
type RecordRow = {
  id: string;
  candidate_id: string;
  description: string;
  source_url: string;
  event_date: string;
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
    const { recordId, sourceUrl, note } = entry as Record<string, unknown>;
    if (typeof recordId !== "string" || recordId.trim().length === 0) {
      throw new Error(`repairs[${index}].recordId must be a non-empty string`);
    }
    // Trim BEFORE the shape check: these files are hand-authored, and a
    // leading space would otherwise fail as "not an http(s) URL".
    const trimmedSourceUrl = typeof sourceUrl === "string" ? sourceUrl.trim() : "";
    if (!/^https?:\/\//i.test(trimmedSourceUrl)) {
      throw new Error(`repairs[${index}].sourceUrl must be an http(s) URL`);
    }
    return {
      recordId: recordId.trim(),
      sourceUrl: trimmedSourceUrl,
      ...(typeof note === "string" ? { note } : {}),
    };
  });
}

/**
 * Everything the repair decision needs, injected so the safety logic is
 * testable without a database or a network. The gates here are the entire
 * point of the script, and the pg-Date timezone defect proved they can be
 * silently wrong — they earn direct coverage.
 */
export type RepairDeps = {
  loadRecord: (recordId: string) => Promise<RecordRow | null>;
  checkReachable: (sourceUrl: string) => Promise<{ ok: boolean; reason?: string }>;
  findIdentityCollision: (input: {
    candidateId: string;
    identityKey: string;
    excludeRecordId: string;
  }) => Promise<string | null>;
  applyRepair: (input: {
    recordId: string;
    sourceUrl: string;
    identityKey: string;
  }) => Promise<void>;
};

export async function repairOneSourceUrl(
  repair: RepairInput,
  deps: RepairDeps,
  options: { apply: boolean }
): Promise<RepairOutcome> {
  const row = await deps.loadRecord(repair.recordId);
  if (!row) {
    return { recordId: repair.recordId, status: "skipped", reason: "record not found" };
  }
  if (row.source_url === repair.sourceUrl) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: "source_url already matches (already repaired)",
    };
  }

  // The replacement has to clear the same gate a fresh write would. Without
  // this, a repair pass could swap one blocked domain for another.
  const policy = evaluateCandidateRecordSourcePolicy({
    description: row.description,
    sourceUrl: repair.sourceUrl,
  });
  if (!policy.ok) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `replacement rejected by source policy: ${policy.reason}`,
    };
  }

  const reachability = await deps.checkReachable(repair.sourceUrl);
  if (!reachability.ok) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `replacement not reachable: ${reachability.reason ?? "unknown reason"}`,
    };
  }

  const identityKey = buildCandidateRecordIdentityKey({
    description: row.description,
    sourceUrl: repair.sourceUrl,
    eventDate: row.event_date,
  });

  // If the repaired identity already exists for this candidate, the fix would
  // collide with the UNIQUE constraint: the corrected citation is already
  // stored on another row, making this one a duplicate. Deleting a canonical
  // row is an operator decision, never this script's.
  const collidingRecordId = await deps.findIdentityCollision({
    candidateId: row.candidate_id,
    identityKey,
    excludeRecordId: row.id,
  });
  if (collidingRecordId) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `repaired identity already exists on record ${collidingRecordId} — this row is a duplicate; needs an operator decision, not a rewrite`,
    };
  }

  if (!options.apply) {
    return {
      recordId: repair.recordId,
      status: "would_repair",
      from: row.source_url,
      to: repair.sourceUrl,
    };
  }

  await deps.applyRepair({ recordId: row.id, sourceUrl: repair.sourceUrl, identityKey });
  return {
    recordId: repair.recordId,
    status: "repaired",
    from: row.source_url,
    to: repair.sourceUrl,
  };
}

function buildPoolDeps(pool: Pool): RepairDeps {
  return {
    loadRecord: async (recordId) => {
      const result = await pool.query<RecordRow>(
        `SELECT id, candidate_id, description, source_url, event_date::text AS event_date
           FROM public.candidate_records
          WHERE id = $1`,
        [recordId]
      );
      const row = result.rows[0];
      if (!row) {
        return null;
      }
      // TypeScript believes event_date is a string, but that is only true
      // because of the ::text cast above. Drop the cast and pg hands back a
      // Date, the type stays a lie, and every repaired key silently encodes a
      // timezone-shifted date. Fail loudly instead.
      if (typeof row.event_date !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(row.event_date)) {
        throw new Error(
          `record ${recordId}: event_date must be selected as text (got ${typeof row.event_date}: ${String(row.event_date)})`
        );
      }
      return row;
    },
    checkReachable: (sourceUrl) => verifyHttpUrlReachability(sourceUrl),
    findIdentityCollision: async ({ candidateId, identityKey, excludeRecordId }) => {
      const result = await pool.query<{ id: string }>(
        `SELECT id
           FROM public.candidate_records
          WHERE candidate_id = $1 AND record_identity_key = $2 AND id <> $3`,
        [candidateId, identityKey, excludeRecordId]
      );
      return result.rows[0]?.id ?? null;
    },
    applyRepair: async ({ recordId, sourceUrl, identityKey }) => {
      await pool.query(
        `UPDATE public.candidate_records
            SET source_url = $2,
                record_identity_key = $3,
                updated_at = now()
          WHERE id = $1`,
        [recordId, sourceUrl, identityKey]
      );
    },
  };
}

async function main(): Promise<void> {
  const { repairsFile, apply } = parseArgs(process.argv.slice(2));
  const repairs = parseRepairsFile(await readFile(repairsFile, "utf8"));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate record source repair");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  const deps = buildPoolDeps(pool);
  const outcomes: RepairOutcome[] = [];

  try {
    for (const repair of repairs) {
      try {
        outcomes.push(await repairOneSourceUrl(repair, deps, { apply }));
      } catch (error) {
        // One bad row must not abandon the batch. In --apply mode some rows
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

if (process.argv[1] && process.argv[1].endsWith("repairCandidateRecordSourceUrls.ts")) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
