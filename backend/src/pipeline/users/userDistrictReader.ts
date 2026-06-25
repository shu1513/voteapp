import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type UserDistrictReaderErrorCode = "invalid_user_id" | "user_not_found";

export class UserDistrictReaderError extends Error {
  constructor(readonly code: UserDistrictReaderErrorCode, message: string) {
    super(message);
    this.name = "UserDistrictReaderError";
  }
}

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new UserDistrictReaderError("invalid_user_id", "User ID must be a valid UUID");
  }
  return normalized;
}

export async function listUserDistrictIds(db: Queryable, userId: string): Promise<string[]> {
  const normalizedUserId = normalizeUserId(userId);
  const districts = await db.query<{ user_id: string; district_id: string | null }>(
    `
      SELECT
        u.id::text AS user_id,
        ud.district_id::text AS district_id
      FROM public.users AS u
      LEFT JOIN public.user_districts AS ud
        ON ud.user_id = u.id
      WHERE u.id = $1::uuid
        AND u.deleted_at IS NULL
      ORDER BY ud.created_at ASC NULLS LAST, ud.id ASC NULLS LAST
    `,
    [normalizedUserId]
  );
  if (districts.rows.length === 0) {
    throw new UserDistrictReaderError("user_not_found", "User not found");
  }

  return districts.rows.flatMap((row) => (row.district_id ? [row.district_id] : []));
}
