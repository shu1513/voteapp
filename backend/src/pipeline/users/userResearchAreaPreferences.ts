import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type TransactionalDb = Pick<Pool, "connect">;
type TransactionClient = Pick<PoolClient, "query" | "release">;

export type ResearchAreaPreferenceDirection = "support" | "oppose";

export type UserResearchAreaPreferenceInput = {
  researchAreaId: string;
  rank?: number | null;
  /**
   * support/oppose the area's stated goal, and the "line in the sand" flag.
   * Omitted (undefined) = keep the value already stored for this area, or the
   * default (support / false) for a newly added area. Clients that only know
   * about ranks (the mobile editor today) can keep sending {id, rank} without
   * wiping settings made elsewhere.
   */
  direction?: ResearchAreaPreferenceDirection;
  hardVeto?: boolean;
};

export type UserResearchAreaPreference = {
  research_area_id: string;
  slug: string;
  name: string;
  description: string | null;
  rank: number | null;
  direction: ResearchAreaPreferenceDirection;
  hard_veto: boolean;
};

export type UserResearchAreaPreferencesResult = {
  preferences: UserResearchAreaPreference[];
};

export type ResearchAreaCatalogItem = {
  id: string;
  slug: string;
  name: string;
  description: string | null;
};

export type ResearchAreaCatalogResult = {
  research_areas: ResearchAreaCatalogItem[];
};

export type UserResearchAreaPreferencesErrorCode =
  | "invalid_user_id"
  | "user_not_found"
  | "invalid_preferences"
  | "unknown_research_area_ids"
  | "unselectable_research_area_ids";

export class UserResearchAreaPreferencesError extends Error {
  constructor(
    readonly code: UserResearchAreaPreferencesErrorCode,
    message: string,
    readonly details: {
      unknownResearchAreaIds?: string[];
      unselectableResearchAreaIds?: string[];
    } = {}
  ) {
    super(message);
    this.name = "UserResearchAreaPreferencesError";
  }
}

type PreferenceRow = {
  user_id: string;
  research_area_id: string | null;
  slug: string | null;
  name: string | null;
  description: string | null;
  rank: number | string | null;
  direction: string | null;
  hard_veto: boolean | null;
};

type StoredPreferenceSettingsRow = {
  research_area_id: string;
  direction: string;
  hard_veto: boolean;
};

type ResearchAreaValidationRow = {
  id: string;
  is_user_selectable: boolean;
};

type NormalizedPreferenceInput = {
  researchAreaId: string;
  rank: number | null;
  direction: ResearchAreaPreferenceDirection | undefined;
  hardVeto: boolean | undefined;
};

function parseDirection(value: string | null): ResearchAreaPreferenceDirection {
  return value === "oppose" ? "oppose" : "support";
}

function parseRank(value: number | string | null): number | null {
  if (value === null) {
    return null;
  }
  if (typeof value === "number") {
    return value;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new UserResearchAreaPreferencesError("invalid_user_id", "User ID must be a valid UUID");
  }
  return normalized;
}

function normalizePreferenceInputs(
  preferences: readonly UserResearchAreaPreferenceInput[]
): NormalizedPreferenceInput[] {
  const normalized: NormalizedPreferenceInput[] = [];
  const seenResearchAreaIds = new Set<string>();
  const seenRanks = new Set<number>();

  for (const preference of preferences) {
    const researchAreaId = preference.researchAreaId.trim();
    if (!isUuid(researchAreaId)) {
      throw new UserResearchAreaPreferencesError(
        "invalid_preferences",
        `Research area ID must be a valid UUID: ${researchAreaId}`
      );
    }

    const researchAreaDedupeKey = researchAreaId.toLowerCase();
    if (seenResearchAreaIds.has(researchAreaDedupeKey)) {
      throw new UserResearchAreaPreferencesError(
        "invalid_preferences",
        `Duplicate research area preference: ${researchAreaId}`
      );
    }
    seenResearchAreaIds.add(researchAreaDedupeKey);

    // Rank is a position in the submitted list, so 1..length is the whole
    // valid range (uniqueness per user is the DB index plus the check below).
    // The bound also keeps ranks inside Postgres integer and keeps every
    // weight positive (0.75^(rank-1) would underflow for absurd ranks).
    const rank = preference.rank ?? null;
    if (rank !== null && (!Number.isInteger(rank) || rank < 1 || rank > preferences.length)) {
      throw new UserResearchAreaPreferencesError(
        "invalid_preferences",
        `Preference rank must be an integer from 1 to ${preferences.length}`
      );
    }
    if (rank !== null) {
      if (seenRanks.has(rank)) {
        throw new UserResearchAreaPreferencesError("invalid_preferences", `Duplicate preference rank: ${rank}`);
      }
      seenRanks.add(rank);
    }

    const direction = preference.direction;
    if (direction !== undefined && direction !== "support" && direction !== "oppose") {
      throw new UserResearchAreaPreferencesError(
        "invalid_preferences",
        "Preference direction must be 'support' or 'oppose'"
      );
    }
    const hardVeto = preference.hardVeto;
    if (hardVeto !== undefined && typeof hardVeto !== "boolean") {
      throw new UserResearchAreaPreferencesError("invalid_preferences", "Preference hard_veto must be a boolean");
    }

    normalized.push({ researchAreaId, rank, direction, hardVeto });
  }

  return normalized;
}

async function rollbackQuietly(client: TransactionClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original failure.
  }
}

function rowsToPreferences(rows: readonly PreferenceRow[]): UserResearchAreaPreference[] {
  return rows.flatMap((row) => {
    if (!row.research_area_id || !row.slug || !row.name) {
      return [];
    }
    return [
      {
        research_area_id: row.research_area_id,
        slug: row.slug,
        name: row.name,
        description: row.description,
        rank: parseRank(row.rank),
        direction: parseDirection(row.direction),
        hard_veto: row.hard_veto === true,
      },
    ];
  });
}

async function queryPreferences(db: Queryable, normalizedUserId: string): Promise<UserResearchAreaPreferencesResult> {
  const result = await db.query<PreferenceRow>(
    `
      SELECT
        u.id::text AS user_id,
        preference.research_area_id::text AS research_area_id,
        area.slug,
        area.name,
        area.description,
        preference.rank,
        preference.direction,
        preference.hard_veto
      FROM public.users AS u
      LEFT JOIN public.user_research_area_preferences AS preference
        ON preference.user_id = u.id
      LEFT JOIN public.research_areas AS area
        ON area.id = preference.research_area_id
      WHERE u.id = $1::uuid
        AND u.deleted_at IS NULL
      ORDER BY preference.rank ASC NULLS LAST, preference.created_at ASC NULLS LAST, preference.id ASC NULLS LAST
    `,
    [normalizedUserId]
  );
  if (result.rows.length === 0) {
    throw new UserResearchAreaPreferencesError("user_not_found", "User not found");
  }

  return { preferences: rowsToPreferences(result.rows) };
}

async function validateSelectableResearchAreas(
  db: Queryable,
  preferences: readonly NormalizedPreferenceInput[]
): Promise<void> {
  if (preferences.length === 0) {
    return;
  }

  const requestedResearchAreaIds = preferences.map((preference) => preference.researchAreaId);
  const result = await db.query<ResearchAreaValidationRow>(
    `
      SELECT id::text, is_user_selectable
      FROM public.research_areas
      WHERE id = ANY($1::uuid[])
    `,
    [requestedResearchAreaIds]
  );

  const foundById = new Map(result.rows.map((row) => [row.id.toLowerCase(), row]));
  const unknownResearchAreaIds = requestedResearchAreaIds.filter(
    (researchAreaId) => !foundById.has(researchAreaId.toLowerCase())
  );
  if (unknownResearchAreaIds.length > 0) {
    throw new UserResearchAreaPreferencesError(
      "unknown_research_area_ids",
      `Unknown research area IDs: ${unknownResearchAreaIds.join(", ")}`,
      { unknownResearchAreaIds }
    );
  }

  const unselectableResearchAreaIds = requestedResearchAreaIds.filter((researchAreaId) => {
    const row = foundById.get(researchAreaId.toLowerCase());
    return row ? !row.is_user_selectable : false;
  });
  if (unselectableResearchAreaIds.length > 0) {
    throw new UserResearchAreaPreferencesError(
      "unselectable_research_area_ids",
      `Research areas cannot be selected as user preferences: ${unselectableResearchAreaIds.join(", ")}`,
      { unselectableResearchAreaIds }
    );
  }
}

export async function listUserResearchAreaPreferences(
  db: Queryable,
  userId: string
): Promise<UserResearchAreaPreferencesResult> {
  return queryPreferences(db, normalizeUserId(userId));
}

export async function listSelectableResearchAreas(db: Queryable): Promise<ResearchAreaCatalogResult> {
  const result = await db.query<ResearchAreaCatalogItem>(
    `
      SELECT id::text, slug, name, description
      FROM public.research_areas
      WHERE is_user_selectable = true
      ORDER BY name ASC, slug ASC
    `
  );

  return { research_areas: result.rows };
}

export async function replaceUserResearchAreaPreferences(
  db: TransactionalDb,
  userId: string,
  preferences: readonly UserResearchAreaPreferenceInput[]
): Promise<UserResearchAreaPreferencesResult> {
  const normalizedUserId = normalizeUserId(userId);
  const normalizedPreferences = normalizePreferenceInputs(preferences);
  const client = await db.connect();

  try {
    await client.query("BEGIN");

    const user = await client.query<{ id: string }>(
      `
        SELECT id
        FROM public.users
        WHERE id = $1::uuid
          AND deleted_at IS NULL
        FOR UPDATE
      `,
      [normalizedUserId]
    );
    if (user.rows.length === 0) {
      throw new UserResearchAreaPreferencesError("user_not_found", "User not found");
    }

    await validateSelectableResearchAreas(client, normalizedPreferences);

    // Full-list replace, but direction/hard_veto survive for areas the caller
    // re-sent without those fields: RETURNING hands back what the old rows
    // held so the insert below can carry it over. (An upsert would be the
    // obvious alternative, but the unique (user_id, rank) index makes a
    // multi-row upsert fail on any rank swap.)
    const previous = await client.query<StoredPreferenceSettingsRow>(
      `
        DELETE FROM public.user_research_area_preferences
        WHERE user_id = $1::uuid
        RETURNING research_area_id::text AS research_area_id, direction, hard_veto
      `,
      [normalizedUserId]
    );
    const previousByAreaId = new Map(previous.rows.map((row) => [row.research_area_id.toLowerCase(), row] as const));

    if (normalizedPreferences.length > 0) {
      const rows = normalizedPreferences.map((preference) => {
        const stored = previousByAreaId.get(preference.researchAreaId.toLowerCase());
        return {
          researchAreaId: preference.researchAreaId,
          rank: preference.rank,
          direction: preference.direction ?? (stored ? parseDirection(stored.direction) : "support"),
          hardVeto: preference.hardVeto ?? (stored ? stored.hard_veto === true : false),
        };
      });
      await client.query(
        `
          INSERT INTO public.user_research_area_preferences (user_id, research_area_id, rank, direction, hard_veto)
          SELECT $1::uuid, input.research_area_id, input.rank, input.direction, input.hard_veto
          FROM unnest($2::uuid[], $3::integer[], $4::text[], $5::boolean[])
            AS input(research_area_id, rank, direction, hard_veto)
        `,
        [
          normalizedUserId,
          rows.map((row) => row.researchAreaId),
          rows.map((row) => row.rank),
          rows.map((row) => row.direction),
          rows.map((row) => row.hardVeto),
        ]
      );
    }

    const saved = await queryPreferences(client, normalizedUserId);
    await client.query("COMMIT");
    return saved;
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}
