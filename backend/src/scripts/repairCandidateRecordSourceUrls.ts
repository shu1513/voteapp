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
  | { recordId: string; status: "repaired"; from: string; to: string; note?: string }
  | { recordId: string; status: "would_repair"; from: string; to: string; note?: string }
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
  checkReachable: (
    sourceUrl: string
  ) => Promise<{ ok: boolean; reason?: string; finalUrl?: string }>;
  findIdentityCollision: (input: {
    candidateId: string;
    identityKey: string;
    excludeRecordId: string;
  }) => Promise<string | null>;
  /**
   * Compare-and-swap: updates only if description, event_date and source_url
   * still hold the values the identity key was computed from. Returns the
   * number of rows changed. See the concurrency note on applyRepair below.
   */
  applyRepair: (input: {
    recordId: string;
    sourceUrl: string;
    identityKey: string;
    expected: { description: string; eventDate: string; sourceUrl: string };
  }) => Promise<number>;
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
  if (row.retired_at !== null) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `record is retired (${row.retired_at}); repairing a withdrawn claim is moot`,
    };
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

  // Store and judge the POST-REDIRECT url, exactly as ingestion does
  // (enrichCandidateRecords.ts): a shortener, tracking link, or open redirect
  // clears the pre-fetch policy check on its own hostname and then lands
  // wherever it likes. Checking only the submitted URL would let this script
  // launder a blocked source back into the corpus through the very repair
  // meant to remove one.
  //
  // A WAF-fronted source can also resolve to a bot-check interstitial, in
  // which case a legitimate page is skipped rather than stored. That is the
  // safe direction to fail: a skipped repair is reported and retryable, while
  // storing the interstitial would recreate the defect being repaired.
  // (Observed: sos.mn.gov redirects a browser user-agent into perfdrive, but
  // not the backend's HEAD-first fetcher, so these resolve to themselves.)
  const resolvedUrl = reachability.finalUrl ?? repair.sourceUrl;
  if (resolvedUrl !== repair.sourceUrl) {
    const resolvedPolicy = evaluateCandidateRecordSourcePolicy({
      description: row.description,
      sourceUrl: resolvedUrl,
    });
    if (!resolvedPolicy.ok) {
      return {
        recordId: repair.recordId,
        status: "skipped",
        reason: `replacement redirects to ${resolvedUrl}, rejected by source policy: ${resolvedPolicy.reason}`,
      };
    }
  }
  if (resolvedUrl === row.source_url) {
    return {
      recordId: repair.recordId,
      status: "skipped",
      reason: `replacement resolves to the stored URL (${resolvedUrl}); nothing to repair`,
    };
  }

  const identityKey = buildCandidateRecordIdentityKey({
    description: row.description,
    sourceUrl: resolvedUrl,
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
      to: resolvedUrl,
      ...(repair.note ? { note: repair.note } : {}),
    };
  }

  // Compare-and-swap on exactly the columns the identity key was computed
  // from. Read, collision check and update are separate statements, so a
  // concurrent writer could change the description between them and leave the
  // key describing content the row no longer has. Guarding the UPDATE means
  // that race changes nothing instead of corrupting the row.
  const updated = await deps.applyRepair({
    recordId: row.id,
    sourceUrl: resolvedUrl,
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
    from: row.source_url,
    to: resolvedUrl,
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
    // Compare-and-swap rather than a transaction with SELECT ... FOR UPDATE.
    // Both close the race, but a lock would have to be taken before the
    // reachability check and held across an HTTP fetch that can run for
    // seconds — a row lock on a canonical table for the duration of a network
    // call. Guarding the UPDATE on the three columns the identity key is
    // derived from gives the same guarantee with no lock: either the content
    // is unchanged and the key is correct, or zero rows match and the caller
    // reports it.
    //
    // origin / origin_run_id are deliberately NOT re-stamped. Migration 197
    // and candidateRecordStore both state the purpose plainly: provenance
    // identifies the run that INTRODUCED the content so a poisoned cohort is
    // one `WHERE origin_run_id = ...` away, and re-imports keep their
    // attribution precisely "so later reruns cannot rotate a poisoned cohort
    // out of that query". This script never alters a claim — only where that
    // claim is cited from. Re-attributing here would rotate a repaired row out
    // of its poisoned cohort while the claim it carries still came from that
    // run, which is the exact failure the columns exist to prevent. The repair
    // stays traceable through updated_at and the run report.
    applyRepair: async ({ recordId, sourceUrl, identityKey, expected }) => {
      const result = await pool.query(
        `UPDATE public.candidate_records
            SET source_url = $2,
                record_identity_key = $3,
                updated_at = now()
          WHERE id = $1
            AND description = $4
            AND event_date = $5::date
            AND source_url = $6`,
        [
          recordId,
          sourceUrl,
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
