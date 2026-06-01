import type { PoolClient } from "pg";

type Queryable = Pick<PoolClient, "query">;

export type CandidateRecordAreaStance = "for" | "against" | "neutral";

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

function normalizeStance(value: CandidateRecordAreaStance | null | undefined): CandidateRecordAreaStance | null {
  if (value === null || value === undefined) {
    return null;
  }
  return value;
}

export async function loadAllowedResearchAreasForElection(
  client: Queryable,
  electionId: string
): Promise<AllowedResearchArea[]> {
  const result = await client.query<AllowedResearchArea>(
    `
      WITH election_office AS (
        SELECT office_id
        FROM public.elections
        WHERE id = $1
      ),
      office_bound AS (
        SELECT DISTINCT ra.id, ra.slug
        FROM election_office eo
        JOIN public.office_research_areas ora
          ON ora.office_id = eo.office_id
        JOIN public.research_areas ra
          ON ra.id = ora.research_area_id
      ),
      general_area AS (
        SELECT ra.id, ra.slug
        FROM public.research_areas ra
        WHERE ra.slug = 'general'
        LIMIT 1
      )
      SELECT DISTINCT id, slug
      FROM (
        SELECT id, slug FROM office_bound
        UNION ALL
        SELECT id, slug FROM general_area
      ) merged
      ORDER BY slug ASC
    `,
    [electionId]
  );

  return result.rows;
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
      general_area AS (
        SELECT ra.id, ra.slug
        FROM public.research_areas ra
        WHERE ra.slug = 'general'
        LIMIT 1
      )
      SELECT DISTINCT id, slug
      FROM (
        SELECT id, slug FROM office_bound
        UNION ALL
        SELECT id, slug FROM general_area
      ) merged
      ORDER BY slug ASC
    `,
    [officeId]
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
    const stance = normalizeStance(label.stance);

    if (!allowedResearchAreaSlugs.has(slug)) {
      failures.push({
        index: i,
        reason: `research_area_slug '${slug}' is not allowed for this office`,
      });
      continue;
    }

    if (slug === "general") {
      if (stance !== null) {
        failures.push({
          index: i,
          reason: "research_area_slug 'general' must not include stance",
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
        reason: `research_area_slug '${slug}' requires stance (for|against|neutral)`,
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
