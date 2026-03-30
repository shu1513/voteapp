import { Pool } from "pg";

import { loadProjectEnv } from "../../config/env.js";
import { STATE_ABBR_BY_FIPS, getStateAbbreviationByFips, normalizeFips } from "../../constants/usStates.js";

export const DISTRICTS_ACS_YEAR = 2024;
export const CENSUS_STATES_DISTRICTS_URL = `https://api.census.gov/data/${DISTRICTS_ACS_YEAR}/acs/acs5?get=NAME,B01001_001E&for=state:*`;

export type DistrictLoadType = "state";

export type DistrictLoadOptions = {
  type: DistrictLoadType;
  dryRun?: boolean;
};

type StateDistrictRow = {
  geoid_compact: string;
  name: string;
  state: string;
  state_fips: string;
  district_type: "us_senate";
  population: number;
};

type DistrictCodeColumnName = "geoid_compact" | "fips_code";

type ExistingDistrictRecord = {
  name: string;
  state: string;
  state_fips: string;
  population: number;
};

type DistrictLoadSummary = {
  type: DistrictLoadType;
  sourceUrl: string;
  totalCandidates: number;
  inserted: number;
  updated: number;
  skipped: number;
  dryRun: boolean;
};

function parsePopulation(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid population value from Census: ${value}`);
  }
  return parsed;
}

/**
 * Parses the Census state endpoint into districts rows for the us_senate district_type.
 */
export function parseStateDistrictRows(data: unknown): StateDistrictRow[] {
  if (!Array.isArray(data) || data.length < 2) {
    throw new Error("Unexpected Census response format: expected array with header and rows");
  }

  const rows = data.slice(1);
  const result: StateDistrictRow[] = [];

  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 3) {
      continue;
    }

    const [nameRaw, populationRaw, stateRaw] = row;
    if (typeof nameRaw !== "string" || typeof populationRaw !== "string" || typeof stateRaw !== "string") {
      continue;
    }

    const stateFips = normalizeFips(stateRaw.trim());
    if (!Object.hasOwn(STATE_ABBR_BY_FIPS, stateFips)) {
      // Excludes territories and keeps 50 states + DC.
      continue;
    }

    const stateAbbreviation = getStateAbbreviationByFips(stateFips);
    result.push({
      geoid_compact: stateFips,
      name: nameRaw.trim(),
      state: stateAbbreviation,
      state_fips: stateFips,
      district_type: "us_senate",
      population: parsePopulation(populationRaw.trim()),
    });
  }

  const expected = Object.keys(STATE_ABBR_BY_FIPS).length;
  const distinctFips = new Set(result.map((item) => item.state_fips));
  if (distinctFips.size !== expected) {
    const missing = Object.keys(STATE_ABBR_BY_FIPS)
      .sort()
      .filter((fips) => !distinctFips.has(fips));
    throw new Error(`Expected ${expected} state rows (50 + DC), got ${distinctFips.size}. Missing: ${missing.join(", ")}`);
  }

  return result.sort((a, b) => a.state_fips.localeCompare(b.state_fips));
}

async function fetchStateDistrictRows(): Promise<StateDistrictRow[]> {
  const response = await fetch(CENSUS_STATES_DISTRICTS_URL);
  if (!response.ok) {
    throw new Error(`Census API request failed: ${response.status} ${response.statusText}`);
  }

  const data: unknown = await response.json();
  return parseStateDistrictRows(data);
}

async function detectDistrictCodeColumn(pool: Pool): Promise<DistrictCodeColumnName> {
  const result = await pool.query<{ column_name: string }>(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'districts'
        AND column_name IN ('geoid_compact', 'fips_code')
    `
  );

  const columns = new Set(result.rows.map((row) => row.column_name));
  if (columns.has("geoid_compact")) {
    return "geoid_compact";
  }
  if (columns.has("fips_code")) {
    return "fips_code";
  }

  throw new Error("districts table is missing both geoid_compact and fips_code columns");
}

async function loadExistingDistrict(
  pool: Pool,
  codeColumn: DistrictCodeColumnName,
  districtType: string,
  geoidCompact: string
): Promise<ExistingDistrictRecord | null> {
  const result = await pool.query<ExistingDistrictRecord>(
    `
      SELECT name, state, state_fips, population
      FROM districts
      WHERE district_type = $1
        AND ${codeColumn} = $2
      LIMIT 1
    `,
    [districtType, geoidCompact]
  );

  return result.rows[0] ?? null;
}

function isSameDistrict(existing: ExistingDistrictRecord, next: StateDistrictRow): boolean {
  return (
    existing.name === next.name &&
    existing.state === next.state &&
    existing.state_fips === next.state_fips &&
    existing.population === next.population
  );
}

async function insertDistrict(pool: Pool, codeColumn: DistrictCodeColumnName, row: StateDistrictRow): Promise<void> {
  await pool.query(
    `
      INSERT INTO districts
        (${codeColumn}, name, state, state_fips, district_type, population, last_researched)
      VALUES
        ($1, $2, $3, $4, $5, $6, now())
    `,
    [row.geoid_compact, row.name, row.state, row.state_fips, row.district_type, row.population]
  );
}

async function updateDistrict(pool: Pool, codeColumn: DistrictCodeColumnName, row: StateDistrictRow): Promise<void> {
  await pool.query(
    `
      UPDATE districts
      SET name = $3,
          state = $4,
          state_fips = $5,
          population = $6,
          last_researched = now()
      WHERE district_type = $1
        AND ${codeColumn} = $2
    `,
    [row.district_type, row.geoid_compact, row.name, row.state, row.state_fips, row.population]
  );
}

/**
 * Loads districts from Census into the districts table.
 * Phase 1 supports only statewide us_senate rows (2024 state:* endpoint).
 */
export async function runDistrictsLoader(options: DistrictLoadOptions): Promise<DistrictLoadSummary> {
  const dryRun = Boolean(options.dryRun);
  if (options.type !== "state") {
    throw new Error(`Unsupported districts load type: ${options.type}`);
  }

  const rows = await fetchStateDistrictRows();

  if (dryRun) {
    return {
      type: options.type,
      sourceUrl: CENSUS_STATES_DISTRICTS_URL,
      totalCandidates: rows.length,
      inserted: 0,
      updated: 0,
      skipped: rows.length,
      dryRun: true,
    };
  }

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp";
  const pool = new Pool({ connectionString: databaseUrl });

  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  try {
    const codeColumn = await detectDistrictCodeColumn(pool);

    await pool.query("BEGIN");
    for (const row of rows) {
      const existing = await loadExistingDistrict(pool, codeColumn, row.district_type, row.geoid_compact);
      if (!existing) {
        await insertDistrict(pool, codeColumn, row);
        inserted += 1;
        continue;
      }

      if (isSameDistrict(existing, row)) {
        skipped += 1;
        continue;
      }

      await updateDistrict(pool, codeColumn, row);
      updated += 1;
    }
    await pool.query("COMMIT");
  } catch (error) {
    await pool.query("ROLLBACK");
    throw error;
  } finally {
    await pool.end();
  }

  return {
    type: options.type,
    sourceUrl: CENSUS_STATES_DISTRICTS_URL,
    totalCandidates: rows.length,
    inserted,
    updated,
    skipped,
    dryRun: false,
  };
}
