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

function toEventDateString(value: string | Date): string {
  return value instanceof Date ? value.toISOString().slice(0, 10) : String(value).slice(0, 10);
}

async function main(): Promise<void> {
  const { repairsFile, apply } = parseArgs(process.argv.slice(2));
  const repairs = parseRepairsFile(await readFile(repairsFile, "utf8"));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate record source repair");
  }
  const pool = new Pool({ connectionString: databaseUrl });
  const outcomes: RepairOutcome[] = [];

  for (const repair of repairs) {
    const existing = await pool.query<RecordRow>(
      `SELECT id, candidate_id, description, source_url, event_date
         FROM public.candidate_records
        WHERE id = $1`,
      [repair.recordId]
    );
    const row = existing.rows[0];
    if (!row) {
      outcomes.push({ recordId: repair.recordId, status: "skipped", reason: "record not found" });
      continue;
    }
    if (row.source_url === repair.sourceUrl) {
      outcomes.push({
        recordId: repair.recordId,
        status: "skipped",
        reason: "source_url already matches (already repaired)",
      });
      continue;
    }

    // The replacement has to clear the same gate a fresh write would. Without
    // this, a repair pass could swap one blocked domain for another.
    const policy = evaluateCandidateRecordSourcePolicy({
      description: row.description,
      sourceUrl: repair.sourceUrl,
    });
    if (!policy.ok) {
      outcomes.push({
        recordId: repair.recordId,
        status: "skipped",
        reason: `replacement rejected by source policy: ${policy.reason}`,
      });
      continue;
    }

    const reachability = await verifyHttpUrlReachability(repair.sourceUrl);
    if (!reachability.ok) {
      outcomes.push({
        recordId: repair.recordId,
        status: "skipped",
        reason: `replacement not reachable: ${reachability.reason}`,
      });
      continue;
    }

    const eventDate = toEventDateString(row.event_date);
    const identityKey = buildCandidateRecordIdentityKey({
      description: row.description,
      sourceUrl: repair.sourceUrl,
      eventDate,
    });

    // If the repaired identity already exists for this candidate, the fix
    // would collide with the UNIQUE constraint: the corrected citation is
    // already stored on another row, making this one a duplicate. Deleting a
    // canonical row is an operator decision, never this script's.
    const collision = await pool.query<{ id: string }>(
      `SELECT id
         FROM public.candidate_records
        WHERE candidate_id = $1 AND record_identity_key = $2 AND id <> $3`,
      [row.candidate_id, identityKey, row.id]
    );
    if (collision.rows.length > 0) {
      outcomes.push({
        recordId: repair.recordId,
        status: "skipped",
        reason: `repaired identity already exists on record ${collision.rows[0]?.id} — this row is a duplicate; needs an operator decision, not a rewrite`,
      });
      continue;
    }

    if (!apply) {
      outcomes.push({
        recordId: repair.recordId,
        status: "would_repair",
        from: row.source_url,
        to: repair.sourceUrl,
      });
      continue;
    }

    await pool.query(
      `UPDATE public.candidate_records
          SET source_url = $2,
              record_identity_key = $3,
              updated_at = now()
        WHERE id = $1`,
      [row.id, repair.sourceUrl, identityKey]
    );
    outcomes.push({
      recordId: repair.recordId,
      status: "repaired",
      from: row.source_url,
      to: repair.sourceUrl,
    });
  }

  const counts = outcomes.reduce<Record<string, number>>((acc, outcome) => {
    acc[outcome.status] = (acc[outcome.status] ?? 0) + 1;
    return acc;
  }, {});

  await pool.end();

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
