import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

/**
 * Removes a research-area tag from EXISTING candidate_records rows when the
 * tag's stance claims more than the recorded act supports. The stance chip
 * renders as "Supports/Opposes <Area>", so a single no vote on one contract
 * tagged `public_safety_and_crime_control:against` reads as "Opposes Public
 * Safety and Crime Control" — a general position the record never states.
 * `ai:candidate-records:relabel` is additive-only; this is the removal side.
 *
 * The delete is hard (the tags table has no retirement columns), but
 * reversible for stanced areas: re-add the tag through
 * `ai:candidate-records:relabel --labels-file`. Null-stance tags (general,
 * integrity_and_ethics) have no re-add tool — the dry-run report is the
 * operator's chance to catch those before applying.
 *
 * Usage:
 *   npm run manual:records:untag -- --untags-file <path>
 *   npm run manual:records:untag -- --untags-file <path> --apply
 *
 * File format: JSON array of { recordId, researchAreaSlug, reason, note? }.
 * Dry run is the default; --apply performs the deletes.
 *
 * Production: research:promote deliberately never deletes target-only rows,
 * so a local untag does NOT propagate — the stale tag stays live in prod
 * (promote reports it in its target-only tag count). Promote untags by
 * KEEPING every applied untags file and re-running it against prod
 * (ALLOW_REMOTE_DB_WRITES=1, dry-run first): the untags file is the reviewed
 * deletion manifest, and the compare-and-swap re-verifies each tag against
 * prod's own stance and description before deleting.
 */

type UntagInput = {
  recordId: string;
  researchAreaSlug: string;
  reason: string;
  note?: string;
};

type UntagOutcome =
  | {
      recordId: string;
      researchAreaSlug: string;
      status: "untagged" | "would_untag";
      stance: string | null;
      description: string;
      reason: string;
      note?: string;
    }
  | { recordId: string; researchAreaSlug: string; status: "skipped"; reason: string };

type TagRow = {
  tag_id: string;
  stance: string | null;
  description: string;
};

function parseArgs(argv: readonly string[]): { untagsFile: string; apply: boolean } {
  let untagsFile = "";
  let apply = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--untags-file") {
      untagsFile = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (arg === "--apply") {
      apply = true;
      continue;
    }
    throw new Error(`unknown flag(s): ${arg}`);
  }
  if (!untagsFile) {
    throw new Error("--untags-file <path> is required");
  }
  return { untagsFile, apply };
}

export function parseUntagsFile(raw: string): UntagInput[] {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("untags file must contain a JSON array");
  }
  return parsed.map((entry, index) => {
    if (typeof entry !== "object" || entry === null) {
      throw new Error(`untags[${index}] must be an object`);
    }
    const { recordId, researchAreaSlug, reason, note } = entry as Record<string, unknown>;
    if (typeof recordId !== "string" || recordId.trim().length === 0) {
      throw new Error(`untags[${index}].recordId must be a non-empty string`);
    }
    if (typeof researchAreaSlug !== "string" || researchAreaSlug.trim().length === 0) {
      throw new Error(`untags[${index}].researchAreaSlug must be a non-empty string`);
    }
    // The reason lands in the run report, which is the only durable account of
    // why the tag was withdrawn (the delete leaves no row to carry it).
    const trimmedReason = typeof reason === "string" ? reason.trim() : "";
    if (trimmedReason.length < 10) {
      throw new Error(
        `untags[${index}].reason must state why the tag overclaims (at least 10 characters)`
      );
    }
    return {
      recordId: recordId.trim(),
      researchAreaSlug: researchAreaSlug.trim(),
      reason: trimmedReason,
      ...(typeof note === "string" ? { note } : {}),
    };
  });
}

export type UntagDeps = {
  loadTag: (recordId: string, researchAreaSlug: string) => Promise<TagRow | null>;
  /**
   * Compare-and-swap: deletes only if the tag still carries the stance the
   * operator reviewed and the record still holds the description the
   * overclaim was judged against. Returns the number of rows deleted.
   */
  applyUntag: (input: {
    tagId: string;
    expected: { stance: string | null; description: string };
  }) => Promise<number>;
};

export async function untagOneRecordArea(
  untag: UntagInput,
  deps: UntagDeps,
  options: { apply: boolean }
): Promise<UntagOutcome> {
  const row = await deps.loadTag(untag.recordId, untag.researchAreaSlug);
  if (!row) {
    return {
      recordId: untag.recordId,
      researchAreaSlug: untag.researchAreaSlug,
      status: "skipped",
      reason: "no live record carries this tag (mistyped id/slug, retired record, or already untagged)",
    };
  }

  if (!options.apply) {
    return {
      recordId: untag.recordId,
      researchAreaSlug: untag.researchAreaSlug,
      status: "would_untag",
      stance: row.stance,
      description: row.description,
      reason: untag.reason,
      ...(untag.note ? { note: untag.note } : {}),
    };
  }

  const deleted = await deps.applyUntag({
    tagId: row.tag_id,
    expected: { stance: row.stance, description: row.description },
  });
  if (deleted !== 1) {
    return {
      recordId: untag.recordId,
      researchAreaSlug: untag.researchAreaSlug,
      status: "skipped",
      reason:
        "tag or record changed after it was read (concurrent write); nothing was deleted — review the current content and re-run",
    };
  }

  return {
    recordId: untag.recordId,
    researchAreaSlug: untag.researchAreaSlug,
    status: "untagged",
    stance: row.stance,
    description: row.description,
    reason: untag.reason,
    ...(untag.note ? { note: untag.note } : {}),
  };
}

function buildPoolDeps(pool: Pool): UntagDeps {
  return {
    loadTag: async (recordId, researchAreaSlug) => {
      const result = await pool.query<TagRow>(
        `SELECT t.id AS tag_id, t.stance, cr.description
           FROM public.candidate_record_area_tags t
           JOIN public.research_areas ra ON ra.id = t.research_area_id
           JOIN public.candidate_records cr ON cr.id = t.candidate_record_id
          WHERE t.candidate_record_id = $1
            AND ra.slug = $2
            AND cr.retired_at IS NULL`,
        [recordId, researchAreaSlug]
      );
      return result.rows[0] ?? null;
    },
    applyUntag: async ({ tagId, expected }) => {
      const result = await pool.query(
        `DELETE FROM public.candidate_record_area_tags t
          USING public.candidate_records cr
          WHERE t.id = $1
            AND t.stance IS NOT DISTINCT FROM $2
            AND cr.id = t.candidate_record_id
            AND cr.description = $3`,
        [tagId, expected.stance, expected.description]
      );
      return result.rowCount ?? 0;
    },
  };
}

async function main(): Promise<void> {
  const { untagsFile, apply } = parseArgs(process.argv.slice(2));
  const untags = parseUntagsFile(await readFile(untagsFile, "utf8"));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate record untagging");
  }
  // Hard deletes stay local until the reviewed prod-promotion step.
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const deps = buildPoolDeps(pool);
  const outcomes: UntagOutcome[] = [];

  try {
    for (const untag of untags) {
      try {
        outcomes.push(await untagOneRecordArea(untag, deps, { apply }));
      } catch (error) {
        // One bad row must not abandon the batch: in --apply mode some tags
        // are already deleted by this point, and losing the report would
        // leave nobody knowing which.
        outcomes.push({
          recordId: untag.recordId,
          researchAreaSlug: untag.researchAreaSlug,
          status: "skipped",
          reason: `untag failed: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
    }
  } finally {
    // The report prints BEFORE pool teardown: in --apply mode some tags are
    // already deleted by now, and a pool.end() rejection after the loop would
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

if (process.argv[1] && process.argv[1].endsWith("untagCandidateRecordAreas.ts")) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
