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
  const user = await db.query<{ id: string }>(
    `
      SELECT id
      FROM public.users
      WHERE id = $1::uuid
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [normalizedUserId]
  );
  if (user.rows.length === 0) {
    throw new UserDistrictReaderError("user_not_found", "User not found");
  }

  const districts = await db.query<{ district_id: string }>(
    `
      SELECT ud.district_id::text AS district_id
      FROM public.user_districts AS ud
      JOIN public.districts AS d
        ON d.id = ud.district_id
       AND d.district_type = ud.district_type
      WHERE ud.user_id = $1::uuid
      ORDER BY ud.created_at ASC, ud.id ASC
    `,
    [normalizedUserId]
  );

  return districts.rows.map((row) => row.district_id);
}
