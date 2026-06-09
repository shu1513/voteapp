import type { Pool, PoolClient } from "pg";

import type { AddressDistrictKey, AddressDistrictType } from "./addressDistrictResolver.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type AddressDistrictLookupKey = {
  district_type: AddressDistrictType;
  geoid_compact: string;
};

export type AddressResolvedDistrict = {
  id: string;
  district_type: AddressDistrictType;
  geoid_compact: string;
  name: string;
  state: string;
  state_fips: string;
  population: number;
  vote_power_score: number | null;
};

export type AddressDistrictLookupResult = {
  districts: AddressResolvedDistrict[];
  missing_district_keys: AddressDistrictLookupKey[];
};

type DistrictRow = {
  id: string;
  district_type: AddressDistrictType;
  geoid_compact: string;
  name: string;
  state: string;
  state_fips: string;
  population: number;
  vote_power_score: string | number | null;
};

function normalizeLookupKeys(keys: readonly (AddressDistrictKey | AddressDistrictLookupKey)[]): AddressDistrictLookupKey[] {
  const normalized: AddressDistrictLookupKey[] = [];
  const seen = new Set<string>();

  for (const key of keys) {
    const districtType = key.district_type;
    const geoidCompact = key.geoid_compact.trim();
    if (geoidCompact.length === 0) {
      continue;
    }

    const dedupeKey = `${districtType}::${geoidCompact}`;
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    normalized.push({
      district_type: districtType,
      geoid_compact: geoidCompact,
    });
  }

  return normalized;
}

function parseVotePowerScore(value: string | number | null): number | null {
  if (value === null) {
    return null;
  }
  const parsed = typeof value === "number" ? value : Number.parseFloat(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed;
}

function toResolvedDistrict(row: DistrictRow): AddressResolvedDistrict {
  return {
    id: row.id,
    district_type: row.district_type,
    geoid_compact: row.geoid_compact,
    name: row.name,
    state: row.state,
    state_fips: row.state_fips,
    population: row.population,
    vote_power_score: parseVotePowerScore(row.vote_power_score),
  };
}

export async function lookupAddressDistricts(
  db: Queryable,
  keys: readonly (AddressDistrictKey | AddressDistrictLookupKey)[]
): Promise<AddressDistrictLookupResult> {
  const normalizedKeys = normalizeLookupKeys(keys);
  if (normalizedKeys.length === 0) {
    return {
      districts: [],
      missing_district_keys: [],
    };
  }

  const districtTypes = normalizedKeys.map((key) => key.district_type);
  const geoidCompacts = normalizedKeys.map((key) => key.geoid_compact);

  const result = await db.query<DistrictRow>(
    `
      WITH requested AS (
        SELECT district_type, geoid_compact, ord
        FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS keys(district_type, geoid_compact, ord)
      )
      SELECT
        d.id,
        d.district_type,
        d.geoid_compact,
        d.name,
        d.state,
        d.state_fips,
        d.population,
        d.vote_power_score
      FROM requested
      JOIN public.districts AS d
        ON d.district_type = requested.district_type
       AND d.geoid_compact = requested.geoid_compact
      ORDER BY requested.ord ASC
    `,
    [districtTypes, geoidCompacts]
  );

  const districts = result.rows.map(toResolvedDistrict);
  const foundKeys = new Set(districts.map((district) => `${district.district_type}::${district.geoid_compact}`));
  const missingDistrictKeys = normalizedKeys.filter(
    (key) => !foundKeys.has(`${key.district_type}::${key.geoid_compact}`)
  );

  return {
    districts,
    missing_district_keys: missingDistrictKeys,
  };
}
