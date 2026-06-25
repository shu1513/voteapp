import type { Pool, PoolClient } from "pg";

import { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../../constants/userResearchAreaPreferences.js";
import { isUuid } from "../../utils/uuid.js";

export { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../../constants/userResearchAreaPreferences.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type TransactionalDb = Pick<Pool, "connect">;
type TransactionClient = Pick<PoolClient, "query" | "release">;

export type UserResearchAreaPreferenceInput = {
  researchAreaId: string;
  rank?: number | null;
};

export type UserResearchAreaPreference = {
  research_area_id: string;
  slug: string;
  name: string;
  description: string | null;
  rank: number | null;
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
};

type ResearchAreaValidationRow = {
  id: string;
  is_user_selectable: boolean;
};

type NormalizedPreferenceInput = {
  researchAreaId: string;
  rank: number | null;
};

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
  if (preferences.length > MAX_USER_RESEARCH_AREA_PREFERENCES) {
    throw new UserResearchAreaPreferencesError(
      "invalid_preferences",
      `At most ${MAX_USER_RESEARCH_AREA_PREFERENCES} research areas can be selected`
    );
  }

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

    const rank = preference.rank ?? null;
    if (rank !== null && (!Number.isInteger(rank) || rank < 1 || rank > MAX_USER_RESEARCH_AREA_PREFERENCES)) {
      throw new UserResearchAreaPreferencesError(
        "invalid_preferences",
        `Preference rank must be an integer from 1 to ${MAX_USER_RESEARCH_AREA_PREFERENCES}`
      );
    }
    if (rank !== null) {
      if (seenRanks.has(rank)) {
        throw new UserResearchAreaPreferencesError("invalid_preferences", `Duplicate preference rank: ${rank}`);
      }
      seenRanks.add(rank);
    }

    normalized.push({ researchAreaId, rank });
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
        preference.rank
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

    await client.query(
      `
        DELETE FROM public.user_research_area_preferences
        WHERE user_id = $1::uuid
      `,
      [normalizedUserId]
    );

    if (normalizedPreferences.length > 0) {
      await client.query(
        `
          INSERT INTO public.user_research_area_preferences (user_id, research_area_id, rank)
          SELECT $1::uuid, input.research_area_id, input.rank
          FROM unnest($2::uuid[], $3::integer[]) AS input(research_area_id, rank)
        `,
        [
          normalizedUserId,
          normalizedPreferences.map((preference) => preference.researchAreaId),
          normalizedPreferences.map((preference) => preference.rank),
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
