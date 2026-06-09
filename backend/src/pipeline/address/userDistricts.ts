import type { Pool, PoolClient } from "pg";

import type { AddressResolvedDistrict } from "./addressDistrictLookup.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type SaveUserDistrictsResult = {
  user_id: string;
  district_count: number;
};

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (normalized.length === 0) {
    throw new Error("userId must not be empty");
  }
  return normalized;
}

export async function saveUserDistricts(
  db: Queryable,
  userId: string,
  districts: readonly AddressResolvedDistrict[]
): Promise<SaveUserDistrictsResult> {
  const normalizedUserId = normalizeUserId(userId);
  if (districts.length === 0) {
    return { user_id: normalizedUserId, district_count: 0 };
  }

  const uniqueDistricts = [...new Map(districts.map((district) => [district.id, district])).values()];
  const districtIds = uniqueDistricts.map((district) => district.id);
  const districtTypes = uniqueDistricts.map((district) => district.district_type);

  const result = await db.query(
    `
      INSERT INTO public.user_districts (user_id, district_id, district_type)
      SELECT $1::uuid, input.district_id, input.district_type
      FROM unnest($2::uuid[], $3::text[]) AS input(district_id, district_type)
      ON CONFLICT (user_id, district_id)
      DO UPDATE SET
        district_type = EXCLUDED.district_type,
        updated_at = now()
    `,
    [normalizedUserId, districtIds, districtTypes]
  );

  return {
    user_id: normalizedUserId,
    district_count: result.rowCount ?? uniqueDistricts.length,
  };
}
