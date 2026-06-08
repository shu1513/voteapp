import type { PoolClient } from "pg";

import {
  GENERAL_RESEARCH_AREA_SLUG,
  INTEGRITY_AND_ETHICS_RESEARCH_AREA_SLUG,
  isNonStanceResearchAreaSlug,
} from "./candidateRecordResearchAreaPolicy.js";

type Queryable = Pick<PoolClient, "query">;

export type CandidateRecordAreaStance = "for" | "against";

export type CandidateRecordAreaLabelInput = {
  candidateRecordId: string;
  researchAreaSlug: string;
  stance?: CandidateRecordAreaStance | null;
};

export type AllowedResearchArea = {
  id: string;
  slug: string;
};

export type CandidateRecordAreaLabelValidationFailure = {
  index: number;
  reason: string;
};

export type CandidateRecordAreaLabelValidationResult =
  | { ok: true; normalized: CandidateRecordAreaLabelInput[] }
  | { ok: false; failures: CandidateRecordAreaLabelValidationFailure[] };

function normalizeSlug(value: string): string {
  return value.trim().toLowerCase();
}

function normalizeStance(value: unknown): CandidateRecordAreaStance | null {
  if (value === "for" || value === "against") {
    return value;
  }
  return null;
}

export async function loadAllowedResearchAreasForOfficeId(
  client: Queryable,
  officeId: string
): Promise<AllowedResearchArea[]> {
  const result = await client.query<AllowedResearchArea>(
    `
      WITH office_bound AS (
        SELECT DISTINCT ra.id, ra.slug
        FROM public.office_research_areas ora
        JOIN public.research_areas ra
          ON ra.id = ora.research_area_id
        WHERE ora.office_id = $1::uuid
      ),
      universal_areas AS (
        SELECT ra.id, ra.slug
        FROM public.research_areas ra
        WHERE ra.slug = ANY($2::text[])
      )
      SELECT DISTINCT id, slug
      FROM (
        SELECT id, slug FROM office_bound
        UNION ALL
        SELECT id, slug FROM universal_areas
      ) merged
      ORDER BY slug ASC
    `,
    [officeId, [GENERAL_RESEARCH_AREA_SLUG, INTEGRITY_AND_ETHICS_RESEARCH_AREA_SLUG]]
  );

  return result.rows;
}

export async function loadAllResearchAreas(client: Queryable): Promise<AllowedResearchArea[]> {
  const result = await client.query<AllowedResearchArea>(
    `
      SELECT id, slug
      FROM public.research_areas
      ORDER BY slug ASC
    `
  );

  return result.rows;
}

export function validateCandidateRecordAreaLabels(
  labels: readonly CandidateRecordAreaLabelInput[],
  allowedResearchAreaSlugs: ReadonlySet<string>
): CandidateRecordAreaLabelValidationResult {
  const failures: CandidateRecordAreaLabelValidationFailure[] = [];
  const normalized: CandidateRecordAreaLabelInput[] = [];

  for (let i = 0; i < labels.length; i += 1) {
    const label = labels[i]!;
    const slug = normalizeSlug(label.researchAreaSlug);
    const rawStance = label.stance;
    const stance = normalizeStance(rawStance);
    const stanceWasProvided = rawStance !== undefined && rawStance !== null;

    if (!allowedResearchAreaSlugs.has(slug)) {
      failures.push({
        index: i,
        reason: `research_area_slug '${slug}' is not allowed for this candidate/election context`,
      });
      continue;
    }

    if (isNonStanceResearchAreaSlug(slug)) {
      if (stanceWasProvided) {
        failures.push({
          index: i,
          reason: `research_area_slug '${slug}' must not include stance`,
        });
        continue;
      }
      normalized.push({
        candidateRecordId: label.candidateRecordId,
        researchAreaSlug: slug,
        stance: null,
      });
      continue;
    }

    if (!stance) {
      failures.push({
        index: i,
        reason: `research_area_slug '${slug}' requires stance (for|against)`,
      });
      continue;
    }

    normalized.push({
      candidateRecordId: label.candidateRecordId,
      researchAreaSlug: slug,
      stance,
    });
  }

  if (failures.length > 0) {
    return { ok: false, failures };
  }
  return { ok: true, normalized };
}

export async function upsertCandidateRecordAreaTags(
  client: Queryable,
  labels: readonly CandidateRecordAreaLabelInput[],
  researchAreaIdBySlug: ReadonlyMap<string, string>
): Promise<{ processed: number }> {
  for (const label of labels) {
    const slug = normalizeSlug(label.researchAreaSlug);
    const researchAreaId = researchAreaIdBySlug.get(slug);
    if (!researchAreaId) {
      throw new Error(`Cannot upsert candidate_record_area_tag: missing research area id for slug '${slug}'`);
    }

    await client.query(
      `
        INSERT INTO public.candidate_record_area_tags (
          candidate_record_id,
          research_area_id,
          stance
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (candidate_record_id, research_area_id)
        DO UPDATE SET
          stance = EXCLUDED.stance,
          updated_at = now()
      `,
      [label.candidateRecordId, researchAreaId, label.stance ?? null]
    );
  }

  return { processed: labels.length };
}
