import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../../config/env.js";
import { STATE_ABBR_BY_FIPS, getStateAbbreviationByFips, normalizeFips } from "../../constants/usStates.js";

export const DISTRICTS_ACS_YEAR = 2024;
export const CENSUS_STATES_DISTRICTS_URL = `https://api.census.gov/data/${DISTRICTS_ACS_YEAR}/acs/acs5?get=NAME,B01001_001E&for=state:*`;
export const CENSUS_US_HOUSE_DISTRICTS_URL = `https://api.census.gov/data/${DISTRICTS_ACS_YEAR}/acs/acs5?get=NAME,B01001_001E&for=congressional+district:*`;
export const CENSUS_COUNTY_DISTRICTS_URL = `https://api.census.gov/data/${DISTRICTS_ACS_YEAR}/acs/acs5?get=NAME,B01001_001E&for=county:*`;
const CENSUS_FETCH_TIMEOUT_MS = 30_000;

export type DistrictLoadType = "state" | "us_house" | "county";

export type DistrictLoadOptions = {
  type: DistrictLoadType;
  dryRun?: boolean;
};

type DistrictRow = {
  geoid_compact: string;
  name: string;
  state: string;
  state_fips: string;
  district_type: "us_senate" | "us_house" | "county";
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
  const trimmed = value.trim();
  if (!/^\d+$/.test(trimmed)) {
    throw new Error(`Invalid population value from Census: ${value}`);
  }

  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid population value from Census: ${value}`);
  }
  return parsed;
}

/**
 * Parses the Census state endpoint into districts rows for the us_senate district_type.
 */
export function parseStateDistrictRows(data: unknown): DistrictRow[] {
  if (!Array.isArray(data) || data.length < 2) {
    throw new Error("Unexpected Census response format: expected array with header and rows");
  }

  const rows = data.slice(1);
  const result: DistrictRow[] = [];

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
  const allFips = result.map((item) => item.state_fips);
  const distinctFips = new Set(allFips);
  if (allFips.length !== distinctFips.size) {
    const duplicates = [...new Set(allFips.filter((fips, index, all) => all.indexOf(fips) !== index))].sort();
    throw new Error(`Duplicate state rows returned by Census: ${duplicates.join(", ")}`);
  }
  if (distinctFips.size !== expected) {
    const missing = Object.keys(STATE_ABBR_BY_FIPS)
      .sort()
      .filter((fips) => !distinctFips.has(fips));
    throw new Error(`Expected ${expected} state rows (50 + DC), got ${distinctFips.size}. Missing: ${missing.join(", ")}`);
  }

  return result.sort((a, b) => a.state_fips.localeCompare(b.state_fips));
}

/**
 * Parses Census congressional district rows into us_house districts.
 * - Keeps 50 states + DC.
 * - Excludes "ZZ" not-defined aggregate rows.
 * - Uses compact geoid format: {state_fips}{district_code}, e.g. "0601", "0200", "1198".
 */
export function parseUsHouseDistrictRows(data: unknown): DistrictRow[] {
  if (!Array.isArray(data) || data.length < 2) {
    throw new Error("Unexpected Census response format: expected array with header and rows");
  }

  const rows = data.slice(1);
  const result: DistrictRow[] = [];

  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 4) {
      continue;
    }

    const [nameRaw, populationRaw, stateRaw, districtRaw] = row;
    if (
      typeof nameRaw !== "string" ||
      typeof populationRaw !== "string" ||
      typeof stateRaw !== "string" ||
      typeof districtRaw !== "string"
    ) {
      continue;
    }

    const stateFips = normalizeFips(stateRaw.trim());
    if (!Object.hasOwn(STATE_ABBR_BY_FIPS, stateFips)) {
      // Excludes territories and keeps 50 states + DC.
      continue;
    }

    const districtCode = districtRaw.trim().toUpperCase();
    if (districtCode === "ZZ") {
      // Census aggregate: "Congressional Districts not defined".
      continue;
    }
    if (!/^\d{2}$/.test(districtCode)) {
      throw new Error(`Unexpected congressional district code from Census: ${districtRaw}`);
    }

    const population = parsePopulation(populationRaw.trim());
    if (population <= 0) {
      throw new Error(`Invalid congressional district population from Census: ${populationRaw}`);
    }

    const stateAbbreviation = getStateAbbreviationByFips(stateFips);
    result.push({
      geoid_compact: `${stateFips}${districtCode}`,
      name: nameRaw.trim(),
      state: stateAbbreviation,
      state_fips: stateFips,
      district_type: "us_house",
      population,
    });
  }

  const expectedStates = Object.keys(STATE_ABBR_BY_FIPS).length;
  const distinctFips = new Set(result.map((item) => item.state_fips));
  if (distinctFips.size !== expectedStates) {
    const missing = Object.keys(STATE_ABBR_BY_FIPS)
      .sort()
      .filter((fips) => !distinctFips.has(fips));
    throw new Error(
      `Expected congressional district rows for ${expectedStates} states (50 + DC), got ${distinctFips.size}. Missing: ${missing.join(", ")}`
    );
  }

  const geoids = result.map((item) => item.geoid_compact);
  const distinctGeoids = new Set(geoids);
  if (geoids.length !== distinctGeoids.size) {
    const duplicates = [...new Set(geoids.filter((geoid, index, all) => all.indexOf(geoid) !== index))].sort();
    throw new Error(`Duplicate congressional district rows returned by Census: ${duplicates.join(", ")}`);
  }

  return result.sort((a, b) => a.geoid_compact.localeCompare(b.geoid_compact));
}

/**
 * Parses Census county rows into county districts.
 * - Keeps 50 states + DC.
 * - Excludes territories (e.g., Puerto Rico).
 * - Uses compact geoid format: {state_fips}{county_code}, e.g. "06037", "11001".
 */
export function parseCountyDistrictRows(data: unknown): DistrictRow[] {
  if (!Array.isArray(data) || data.length < 2) {
    throw new Error("Unexpected Census response format: expected array with header and rows");
  }

  const rows = data.slice(1);
  const result: DistrictRow[] = [];

  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 4) {
      continue;
    }

    const [nameRaw, populationRaw, stateRaw, countyRaw] = row;
    if (
      typeof nameRaw !== "string" ||
      typeof populationRaw !== "string" ||
      typeof stateRaw !== "string" ||
      typeof countyRaw !== "string"
    ) {
      continue;
    }

    const stateFips = normalizeFips(stateRaw.trim());
    if (!Object.hasOwn(STATE_ABBR_BY_FIPS, stateFips)) {
      // Excludes territories and keeps 50 states + DC.
      continue;
    }

    const countyCode = countyRaw.trim();
    if (!/^\d{3}$/.test(countyCode)) {
      throw new Error(`Unexpected county code from Census: ${countyRaw}`);
    }

    const population = parsePopulation(populationRaw.trim());
    if (population <= 0) {
      throw new Error(`Invalid county population from Census: ${populationRaw}`);
    }

    result.push({
      geoid_compact: `${stateFips}${countyCode}`,
      name: nameRaw.trim(),
      state: getStateAbbreviationByFips(stateFips),
      state_fips: stateFips,
      district_type: "county",
      population,
    });
  }

  const expectedStates = Object.keys(STATE_ABBR_BY_FIPS).length;
  const distinctFips = new Set(result.map((item) => item.state_fips));
  if (distinctFips.size !== expectedStates) {
    const missing = Object.keys(STATE_ABBR_BY_FIPS)
      .sort()
      .filter((fips) => !distinctFips.has(fips));
    throw new Error(
      `Expected county rows for ${expectedStates} states (50 + DC), got ${distinctFips.size}. Missing: ${missing.join(", ")}`
    );
  }

  const geoids = result.map((item) => item.geoid_compact);
  const distinctGeoids = new Set(geoids);
  if (geoids.length !== distinctGeoids.size) {
    const duplicates = [...new Set(geoids.filter((geoid, index, all) => all.indexOf(geoid) !== index))].sort();
    throw new Error(`Duplicate county rows returned by Census: ${duplicates.join(", ")}`);
  }

  return result.sort((a, b) => a.geoid_compact.localeCompare(b.geoid_compact));
}

async function fetchCensusRows(url: string): Promise<unknown> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CENSUS_FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Census API request failed: ${response.status} ${response.statusText}`);
    }

    return (await response.json()) as unknown;
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Census API request timed out after ${CENSUS_FETCH_TIMEOUT_MS}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchStateDistrictRows(): Promise<DistrictRow[]> {
  const data = await fetchCensusRows(CENSUS_STATES_DISTRICTS_URL);
  return parseStateDistrictRows(data);
}

async function fetchUsHouseDistrictRows(): Promise<DistrictRow[]> {
  const data = await fetchCensusRows(CENSUS_US_HOUSE_DISTRICTS_URL);
  return parseUsHouseDistrictRows(data);
}

async function fetchCountyDistrictRows(): Promise<DistrictRow[]> {
  const data = await fetchCensusRows(CENSUS_COUNTY_DISTRICTS_URL);
  return parseCountyDistrictRows(data);
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
  client: PoolClient,
  codeColumn: DistrictCodeColumnName,
  districtType: string,
  geoidCompact: string
): Promise<ExistingDistrictRecord | null> {
  const result = await client.query<ExistingDistrictRecord>(
    `
      SELECT name, state, state_fips, population
      FROM public.districts
      WHERE district_type = $1
        AND ${codeColumn} = $2
      LIMIT 1
    `,
    [districtType, geoidCompact]
  );

  return result.rows[0] ?? null;
}

function isSameDistrict(existing: ExistingDistrictRecord, next: DistrictRow): boolean {
  return (
    existing.name === next.name &&
    existing.state === next.state &&
    existing.state_fips === next.state_fips &&
    existing.population === next.population
  );
}

async function insertDistrict(client: PoolClient, codeColumn: DistrictCodeColumnName, row: DistrictRow): Promise<void> {
  await client.query(
    `
      INSERT INTO public.districts
        (${codeColumn}, name, state, state_fips, district_type, population, last_researched)
      VALUES
        ($1, $2, $3, $4, $5, $6, now())
    `,
    [row.geoid_compact, row.name, row.state, row.state_fips, row.district_type, row.population]
  );
}

async function updateDistrict(client: PoolClient, codeColumn: DistrictCodeColumnName, row: DistrictRow): Promise<void> {
  await client.query(
    `
      UPDATE public.districts
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
 * Recomputes vote_power_score for every district row using a log-scaled inverse-population model:
 *   score_i = 100 * ln(max_scope_pop / pop_i) / ln(max_scope_pop / min_scope_pop)
 *
 * Why this model:
 * - Population-based structural measure that works across district types.
 * - Log scaling keeps scores interpretable (avoids extreme swings from raw inverse population).
 * - Bounded output (0..100) with deterministic behavior for edge cases.
 *
 * Scope rules:
 * - us_senate/us_house: national scope per district_type.
 * - all other district types: state-level scope (district_type + state_fips).
 */
async function recomputeVotePowerScores(client: PoolClient): Promise<void> {
  await client.query(
    `
      WITH scoped AS (
        SELECT
          id,
          district_type,
          state_fips,
          population::numeric AS population,
          CASE
            -- Federal congressional types compare nationally.
            WHEN district_type IN ('us_senate', 'us_house') THEN district_type
            -- Other types compare within the same state.
            ELSE district_type || ':' || COALESCE(state_fips, '')
          END AS scope_key
        FROM public.districts
      ),
      scope_stats AS (
        SELECT
          scope_key,
          MIN(population) AS min_population,
          MAX(population) AS max_population
        FROM scoped
        WHERE population IS NOT NULL
          AND population > 0
        GROUP BY scope_key
      ),
      scored AS (
        SELECT
          scoped.id,
          CASE
            WHEN scoped.population IS NULL OR scoped.population <= 0 THEN NULL::numeric
            WHEN scope_stats.scope_key IS NULL THEN NULL::numeric
            WHEN scope_stats.max_population = scope_stats.min_population THEN 50::numeric
            ELSE ROUND(
              LEAST(
                100::numeric,
                GREATEST(
                  0::numeric,
                  100::numeric
                    * (LN(scope_stats.max_population) - LN(scoped.population))
                    / NULLIF(LN(scope_stats.max_population) - LN(scope_stats.min_population), 0::numeric)
                )
              ),
              2
            )
          END AS vote_power_score
        FROM scoped
        LEFT JOIN scope_stats ON scope_stats.scope_key = scoped.scope_key
      )
      UPDATE public.districts
      SET vote_power_score = scored.vote_power_score
      FROM scored
      WHERE public.districts.id = scored.id
        AND public.districts.vote_power_score IS DISTINCT FROM scored.vote_power_score
    `
  );
}

/**
 * Loads districts from Census into the districts table.
 * Supported types:
 * - state -> us_senate rows (2024 state:* endpoint)
 * - us_house -> congressional district rows (2024 congressional+district:* endpoint)
 * - county -> county rows (2024 county:* endpoint)
 */
export async function runDistrictsLoader(options: DistrictLoadOptions): Promise<DistrictLoadSummary> {
  const dryRun = Boolean(options.dryRun);
  let sourceUrl: string;
  let rows: DistrictRow[];
  if (options.type === "state") {
    sourceUrl = CENSUS_STATES_DISTRICTS_URL;
    rows = await fetchStateDistrictRows();
  } else if (options.type === "us_house") {
    sourceUrl = CENSUS_US_HOUSE_DISTRICTS_URL;
    rows = await fetchUsHouseDistrictRows();
  } else if (options.type === "county") {
    sourceUrl = CENSUS_COUNTY_DISTRICTS_URL;
    rows = await fetchCountyDistrictRows();
  } else {
    throw new Error(`Unsupported districts load type: ${options.type}`);
  }

  if (dryRun) {
    return {
      type: options.type,
      sourceUrl,
      totalCandidates: rows.length,
      inserted: 0,
      updated: 0,
      skipped: rows.length,
      dryRun: true,
    };
  }

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl || databaseUrl.trim().length === 0) {
    throw new Error("DATABASE_URL is required for districts loader");
  }
  const pool = new Pool({ connectionString: databaseUrl });

  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  try {
    const codeColumn = await detectDistrictCodeColumn(pool);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", [`districts_loader:${options.type}`]);
      for (const row of rows) {
        const existing = await loadExistingDistrict(client, codeColumn, row.district_type, row.geoid_compact);
        if (!existing) {
          await insertDistrict(client, codeColumn, row);
          inserted += 1;
          continue;
        }

        if (isSameDistrict(existing, row)) {
          skipped += 1;
          continue;
        }

        await updateDistrict(client, codeColumn, row);
        updated += 1;
      }

      await recomputeVotePowerScores(client);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }

  return {
    type: options.type,
    sourceUrl,
    totalCandidates: rows.length,
    inserted,
    updated,
    skipped,
    dryRun: false,
  };
}
