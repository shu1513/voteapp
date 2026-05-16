import { Pool } from "pg";

import { fetchCensusJsonWithKeyRotation, readCensusApiKeysFromEnv } from "./src/config/censusApi.js";
import { loadProjectEnv } from "./src/config/env.js";
import {
  CENSUS_COUNTY_DISTRICTS_URL,
  CENSUS_PLACE_DISTRICTS_URL,
  CENSUS_SCHOOL_ELEMENTARY_DISTRICTS_URL,
  CENSUS_SCHOOL_SECONDARY_DISTRICTS_URL,
  CENSUS_SCHOOL_UNIFIED_DISTRICTS_URL,
  CENSUS_STATES_DISTRICTS_URL,
  CENSUS_STATE_LOWER_DISTRICTS_URL_PATTERN,
  CENSUS_STATE_UPPER_DISTRICTS_URL_PATTERN,
  CENSUS_US_HOUSE_DISTRICTS_URL,
  parseCountyDistrictRows,
  parsePlaceDistrictRows,
  parseSchoolElementaryDistrictRows,
  parseSchoolSecondaryDistrictRows,
  parseSchoolUnifiedDistrictRows,
  parseStateDistrictRows,
  parseStateLowerDistrictRows,
  parseStateUpperDistrictRows,
  parseUsHouseDistrictRows,
} from "./src/pipeline/loaders/districtsLoader.js";
import { STATE_LOWER_STATE_FIPS_2024 } from "./src/constants/stateLowerGeoids2024.js";
import { STATE_UPPER_STATE_FIPS_2024 } from "./src/constants/stateUpperGeoids2024.js";

type DistrictType =
  | "statewide"
  | "state_upper"
  | "state_lower"
  | "us_house"
  | "county"
  | "place"
  | "school_unified"
  | "school_secondary"
  | "school_elementary";

type DistrictLike = {
  geoid_compact: string;
  name: string;
  state_fips: string;
  population: number;
};

type ComparisonSummary = {
  districtType: DistrictType;
  censusCount: number;
  dbCount: number;
  missingInDb: number;
  extraInDb: number;
  fieldMismatches: number;
  sampleMissing: string[];
  sampleExtra: string[];
  sampleFieldMismatch: Array<{
    geoid: string;
    census: Pick<DistrictLike, "name" | "state_fips" | "population">;
    db: Pick<DistrictLike, "name" | "state_fips" | "population">;
  }>;
};

function withStateFips(patternUrl: string, stateFips: string): string {
  return patternUrl.replace("{state_fips}", stateFips);
}

function toRecordMap(rows: DistrictLike[], source: string): Map<string, DistrictLike> {
  const map = new Map<string, DistrictLike>();
  for (const row of rows) {
    if (map.has(row.geoid_compact)) {
      throw new Error(`Duplicate geoid_compact in ${source}: ${row.geoid_compact}`);
    }
    map.set(row.geoid_compact, row);
  }
  return map;
}

function sameDistrict(a: DistrictLike, b: DistrictLike): boolean {
  return a.name === b.name && a.state_fips === b.state_fips && a.population === b.population;
}

async function fetchAllDistrictsFromCensus(censusApiKeys: readonly string[]): Promise<Record<DistrictType, DistrictLike[]>> {
  const stateData = await fetchCensusJsonWithKeyRotation(CENSUS_STATES_DISTRICTS_URL, censusApiKeys);
  const usHouseData = await fetchCensusJsonWithKeyRotation(CENSUS_US_HOUSE_DISTRICTS_URL, censusApiKeys);
  const countyData = await fetchCensusJsonWithKeyRotation(CENSUS_COUNTY_DISTRICTS_URL, censusApiKeys);
  const placeData = await fetchCensusJsonWithKeyRotation(CENSUS_PLACE_DISTRICTS_URL, censusApiKeys);
  const schoolUnifiedData = await fetchCensusJsonWithKeyRotation(CENSUS_SCHOOL_UNIFIED_DISTRICTS_URL, censusApiKeys);
  const schoolSecondaryData = await fetchCensusJsonWithKeyRotation(CENSUS_SCHOOL_SECONDARY_DISTRICTS_URL, censusApiKeys);
  const schoolElementaryData = await fetchCensusJsonWithKeyRotation(CENSUS_SCHOOL_ELEMENTARY_DISTRICTS_URL, censusApiKeys);

  const upperCombined: unknown[] = [["NAME", "B01001_001E", "state", "state legislative district (upper chamber)"]];
  for (const stateFips of STATE_UPPER_STATE_FIPS_2024) {
    const upperData = await fetchCensusJsonWithKeyRotation(
      withStateFips(CENSUS_STATE_UPPER_DISTRICTS_URL_PATTERN, stateFips),
      censusApiKeys
    );
    if (!Array.isArray(upperData) || upperData.length < 2) {
      throw new Error(`Unexpected upper-chamber Census format for state ${stateFips}`);
    }
    upperCombined.push(...upperData.slice(1));
  }

  const lowerCombined: unknown[] = [["NAME", "B01001_001E", "state", "state legislative district (lower chamber)"]];
  for (const stateFips of STATE_LOWER_STATE_FIPS_2024) {
    const lowerData = await fetchCensusJsonWithKeyRotation(
      withStateFips(CENSUS_STATE_LOWER_DISTRICTS_URL_PATTERN, stateFips),
      censusApiKeys
    );
    if (!Array.isArray(lowerData) || lowerData.length < 2) {
      throw new Error(`Unexpected lower-chamber Census format for state ${stateFips}`);
    }
    lowerCombined.push(...lowerData.slice(1));
  }

  // Match loader behavior: zero-pop rows are deleted/skipped, so compare only > 0 population rows.
  const keepPositivePopulation = (rows: DistrictLike[]) => rows.filter((row) => row.population > 0);

  return {
    statewide: keepPositivePopulation(parseStateDistrictRows(stateData) as DistrictLike[]),
    state_upper: keepPositivePopulation(parseStateUpperDistrictRows(upperCombined) as DistrictLike[]),
    state_lower: keepPositivePopulation(parseStateLowerDistrictRows(lowerCombined) as DistrictLike[]),
    us_house: keepPositivePopulation(parseUsHouseDistrictRows(usHouseData) as DistrictLike[]),
    county: keepPositivePopulation(parseCountyDistrictRows(countyData) as DistrictLike[]),
    place: keepPositivePopulation(parsePlaceDistrictRows(placeData) as DistrictLike[]),
    school_unified: keepPositivePopulation(parseSchoolUnifiedDistrictRows(schoolUnifiedData) as DistrictLike[]),
    school_secondary: keepPositivePopulation(parseSchoolSecondaryDistrictRows(schoolSecondaryData) as DistrictLike[]),
    school_elementary: keepPositivePopulation(parseSchoolElementaryDistrictRows(schoolElementaryData) as DistrictLike[]),
  };
}

async function detectDistrictCodeColumn(pool: Pool): Promise<"geoid_compact" | "fips_code"> {
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
  throw new Error("districts table missing both geoid_compact and fips_code");
}

async function loadDistrictsFromDb(pool: Pool, districtType: DistrictType, codeColumn: "geoid_compact" | "fips_code"): Promise<DistrictLike[]> {
  const result = await pool.query<{
    geoid_compact: string;
    name: string;
    state_fips: string;
    population: number;
  }>(
    `
      SELECT ${codeColumn} AS geoid_compact, name, state_fips, population
      FROM public.districts
      WHERE district_type = $1
        AND population > 0
    `,
    [districtType]
  );

  return result.rows;
}

function compareType(districtType: DistrictType, censusRows: DistrictLike[], dbRows: DistrictLike[]): ComparisonSummary {
  const censusMap = toRecordMap(censusRows, `${districtType}:census`);
  const dbMap = toRecordMap(dbRows, `${districtType}:db`);

  const missingInDb: string[] = [];
  const fieldMismatches: Array<{
    geoid: string;
    census: Pick<DistrictLike, "name" | "state_fips" | "population">;
    db: Pick<DistrictLike, "name" | "state_fips" | "population">;
  }> = [];

  for (const [geoid, censusRow] of censusMap) {
    const dbRow = dbMap.get(geoid);
    if (!dbRow) {
      missingInDb.push(geoid);
      continue;
    }
    if (!sameDistrict(censusRow, dbRow)) {
      fieldMismatches.push({
        geoid,
        census: {
          name: censusRow.name,
          state_fips: censusRow.state_fips,
          population: censusRow.population,
        },
        db: {
          name: dbRow.name,
          state_fips: dbRow.state_fips,
          population: dbRow.population,
        },
      });
    }
  }

  const extraInDb = [...dbMap.keys()].filter((geoid) => !censusMap.has(geoid));

  return {
    districtType,
    censusCount: censusRows.length,
    dbCount: dbRows.length,
    missingInDb: missingInDb.length,
    extraInDb: extraInDb.length,
    fieldMismatches: fieldMismatches.length,
    sampleMissing: missingInDb.slice(0, 5),
    sampleExtra: extraInDb.slice(0, 5),
    sampleFieldMismatch: fieldMismatches.slice(0, 3),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const censusApiKeys = readCensusApiKeysFromEnv(process.env);
  if (censusApiKeys.length === 0) {
    throw new Error("No Census API keys configured in environment.");
  }

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl || databaseUrl.trim().length === 0) {
    throw new Error("DATABASE_URL is required.");
  }

  const pool = new Pool({ connectionString: databaseUrl });

  try {
    const censusByType = await fetchAllDistrictsFromCensus(censusApiKeys);
    const codeColumn = await detectDistrictCodeColumn(pool);

    const districtTypes: DistrictType[] = [
      "statewide",
      "state_upper",
      "state_lower",
      "us_house",
      "county",
      "place",
      "school_unified",
      "school_secondary",
      "school_elementary",
    ];

    const summaries: ComparisonSummary[] = [];
    for (const districtType of districtTypes) {
      const dbRows = await loadDistrictsFromDb(pool, districtType, codeColumn);
      summaries.push(compareType(districtType, censusByType[districtType], dbRows));
    }

    console.log(JSON.stringify({ ok: true, summaries }, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exit(1);
});
