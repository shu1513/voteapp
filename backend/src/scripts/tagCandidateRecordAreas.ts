import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import {
  loadAllowedResearchAreasForOfficeId,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
  type AllowedResearchArea,
  type CandidateRecordAreaStance,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

/**
 * Adds a research-area tag to EXISTING candidate_records rows from a
 * reviewed manifest — the manual counterpart of `ai:candidate-records:relabel`
 * and the add side of `manual:records:untag`. Built for a new research area
 * (ai_regulation, migration 276): records written before the area existed
 * can only carry it once an operator reads each one and decides the stance.
 *
 * Every tag goes through the same gate as a fresh write: the slug must be in
 * the allowed set of the office the candidate is running for (their latest
 * office race), and the stance is validated by
 * validateCandidateRecordAreaLabels. Never touches an existing tag on the
 * same area — a different stance is reported as a conflict to untag first.
 *
 * Usage:
 *   npm run manual:records:tag -- --tags-file <path>
 *   npm run manual:records:tag -- --tags-file <path> --apply
 *
 * File format: JSON array of
 *   { recordId, researchAreaSlug, stance, expectedDescription, reason, note? }.
 * expectedDescription pins what the operator reviewed (same staleness
 * discipline as the untag manifest): a row whose description moved since
 * review skips instead of tagging today's content. Dry run is the default;
 * --apply performs the upserts.
 *
 * Production: research:promote copies local tags to prod, so an applied tags
 * file needs no separate prod step — promote the candidates as usual.
 */

type TagInput = {
  recordId: string;
  researchAreaSlug: string;
  stance: CandidateRecordAreaStance;
  /** The record description the stance was judged against. */
  expectedDescription: string;
  reason: string;
  note?: string;
};

export type TagRecordRow = {
  candidate_id: string;
  description: string;
  /** Office of the candidate's latest office race; null when they have none. */
  office_id: string | null;
  office_name: string | null;
  /** Stance of the record's existing tag on this area; undefined when untagged. */
  existing_stance?: string | null;
};

type TagOutcome =
  | {
      recordId: string;
      researchAreaSlug: string;
      status: "tagged" | "would_tag";
      stance: CandidateRecordAreaStance;
      office: string;
      description: string;
      reason: string;
      note?: string;
    }
  | { recordId: string; researchAreaSlug: string; status: "skipped"; reason: string };

function parseArgs(argv: readonly string[]): { tagsFile: string; apply: boolean } {
  let tagsFile = "";
  let apply = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--tags-file") {
      tagsFile = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (arg === "--apply") {
      apply = true;
      continue;
    }
    throw new Error(`unknown flag(s): ${arg}`);
  }
  if (!tagsFile) {
    throw new Error("--tags-file <path> is required");
  }
  return { tagsFile, apply };
}

export function parseTagsFile(raw: string): TagInput[] {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("tags file must contain a JSON array");
  }
  return parsed.map((entry, index) => {
    if (typeof entry !== "object" || entry === null) {
      throw new Error(`tags[${index}] must be an object`);
    }
    const { recordId, researchAreaSlug, stance, expectedDescription, reason, note } = entry as Record<string, unknown>;
    if (typeof recordId !== "string" || recordId.trim().length === 0) {
      throw new Error(`tags[${index}].recordId must be a non-empty string`);
    }
    if (typeof researchAreaSlug !== "string" || researchAreaSlug.trim().length === 0) {
      throw new Error(`tags[${index}].researchAreaSlug must be a non-empty string`);
    }
    // Null-stance areas (general, integrity_and_ethics) are the writer's job;
    // this manifest only ever states a policy position.
    if (stance !== "for" && stance !== "against") {
      throw new Error(`tags[${index}].stance must be "for" or "against"`);
    }
    if (typeof expectedDescription !== "string" || expectedDescription.trim().length === 0) {
      throw new Error(`tags[${index}].expectedDescription must be the reviewed record description`);
    }
    const trimmedReason = typeof reason === "string" ? reason.trim() : "";
    if (trimmedReason.length < 10) {
      throw new Error(`tags[${index}].reason must state why the record takes this stance (at least 10 characters)`);
    }
    return {
      recordId: recordId.trim(),
      researchAreaSlug: researchAreaSlug.trim(),
      stance,
      expectedDescription,
      reason: trimmedReason,
      ...(typeof note === "string" ? { note } : {}),
    };
  });
}

export type TagDeps = {
  loadRecord: (recordId: string, researchAreaSlug: string) => Promise<TagRecordRow | null>;
  loadAllowedAreas: (officeId: string) => Promise<AllowedResearchArea[]>;
  applyTag: (input: {
    recordId: string;
    researchAreaSlug: string;
    stance: CandidateRecordAreaStance;
    researchAreaIdBySlug: ReadonlyMap<string, string>;
  }) => Promise<void>;
};

export async function tagOneRecordArea(
  tag: TagInput,
  deps: TagDeps,
  options: { apply: boolean }
): Promise<TagOutcome> {
  const skipped = (reason: string): TagOutcome => ({
    recordId: tag.recordId,
    researchAreaSlug: tag.researchAreaSlug,
    status: "skipped",
    reason,
  });
  const row = await deps.loadRecord(tag.recordId, tag.researchAreaSlug);
  if (!row) {
    return skipped("no live record with this id (mistyped id or retired record)");
  }
  // Reviewed-data drift guard, checked in dry-run too: the stance was decided
  // about the description in the manifest, so a row rewritten since review
  // must be re-read, not tagged on today's content.
  if (row.description !== tag.expectedDescription) {
    return skipped("record description no longer matches expectedDescription; the row changed since review — re-review before tagging");
  }
  if (row.existing_stance !== undefined) {
    return row.existing_stance === tag.stance
      ? skipped(`already tagged ${tag.researchAreaSlug}:${tag.stance}`)
      : skipped(`already tagged ${tag.researchAreaSlug}:${row.existing_stance ?? "null"}; untag it first`);
  }
  if (!row.office_id) {
    return skipped("candidate has no office race, so no allowed research-area set");
  }
  const allowed = await deps.loadAllowedAreas(row.office_id);
  const validation = validateCandidateRecordAreaLabels(
    [{ candidateRecordId: tag.recordId, researchAreaSlug: tag.researchAreaSlug, stance: tag.stance }],
    new Set(allowed.map((area) => area.slug))
  );
  if (!validation.ok) {
    return skipped(`${validation.failures.map((failure) => failure.reason).join("; ")} (office: ${row.office_name ?? row.office_id})`);
  }
  const office = row.office_name ?? row.office_id;
  if (!options.apply) {
    return {
      recordId: tag.recordId,
      researchAreaSlug: tag.researchAreaSlug,
      status: "would_tag",
      stance: tag.stance,
      office,
      description: row.description,
      reason: tag.reason,
      ...(tag.note ? { note: tag.note } : {}),
    };
  }
  await deps.applyTag({
    recordId: tag.recordId,
    researchAreaSlug: tag.researchAreaSlug,
    stance: tag.stance,
    researchAreaIdBySlug: new Map(allowed.map((area) => [area.slug, area.id])),
  });
  return {
    recordId: tag.recordId,
    researchAreaSlug: tag.researchAreaSlug,
    status: "tagged",
    stance: tag.stance,
    office,
    description: row.description,
    reason: tag.reason,
    ...(tag.note ? { note: tag.note } : {}),
  };
}

function buildPoolDeps(pool: Pool): TagDeps {
  return {
    loadRecord: async (recordId, researchAreaSlug) => {
      const result = await pool.query<TagRecordRow & { tag_present: boolean }>(
        `SELECT cr.candidate_id,
                cr.description,
                latest.office_id::text AS office_id,
                latest.office_name,
                (t.id IS NOT NULL) AS tag_present,
                t.stance AS existing_stance
           FROM public.candidate_records cr
           LEFT JOIN LATERAL (
             SELECT e.office_id, o.scope || '/' || o.canonical_name AS office_name
               FROM public.candidate_elections ce
               JOIN public.elections e ON e.id = ce.election_id
               JOIN public.offices o ON o.id = e.office_id
              WHERE (ce.candidate_id = cr.candidate_id OR ce.running_mate_candidate_id = cr.candidate_id)
                AND e.race_type = 'office'
              ORDER BY e.election_date DESC
              LIMIT 1
           ) latest ON true
           LEFT JOIN public.research_areas ra ON ra.slug = $2
           LEFT JOIN public.candidate_record_area_tags t
             ON t.candidate_record_id = cr.id AND t.research_area_id = ra.id
          WHERE cr.id = $1
            AND cr.retired_at IS NULL`,
        [recordId, researchAreaSlug]
      );
      const row = result.rows[0];
      if (!row) {
        return null;
      }
      const { tag_present, existing_stance, ...rest } = row;
      return tag_present ? { ...rest, existing_stance: existing_stance ?? null } : rest;
    },
    loadAllowedAreas: (officeId) => loadAllowedResearchAreasForOfficeId(pool, officeId),
    applyTag: async ({ recordId, researchAreaSlug, stance, researchAreaIdBySlug }) => {
      await upsertCandidateRecordAreaTags(pool, [{ candidateRecordId: recordId, researchAreaSlug, stance }], researchAreaIdBySlug);
    },
  };
}

async function main(): Promise<void> {
  const { tagsFile, apply } = parseArgs(process.argv.slice(2));
  const tags = parseTagsFile(await readFile(tagsFile, "utf8"));
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for candidate record tagging");
  }
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const deps = buildPoolDeps(pool);
  const outcomes: TagOutcome[] = [];

  try {
    for (const tag of tags) {
      try {
        outcomes.push(await tagOneRecordArea(tag, deps, { apply }));
      } catch (error) {
        // One bad row must not abandon the batch: in --apply mode some tags
        // are already written by this point, and the report is the only
        // account of which.
        outcomes.push({
          recordId: tag.recordId,
          researchAreaSlug: tag.researchAreaSlug,
          status: "skipped",
          reason: `tag failed: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
    }
  } finally {
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

if (process.argv[1] && process.argv[1].endsWith("tagCandidateRecordAreas.ts")) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
