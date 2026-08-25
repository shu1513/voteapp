import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";

// One-time-per-decade load of the Census 2020 ZCTA/county relationship file
// into address_zcta_county (docs/plans/partial-address-scope.md). The ZIP
// partial-ballot path reads this table instead of calling any geocoder.
//
// Usage:
//   npm run import:zcta-county-crosswalk            # download from Census
//   npm run import:zcta-county-crosswalk -- --file /path/to/tab20_...natl.txt

export const ZCTA_COUNTY_CROSSWALK_URL =
  "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt";

const DOWNLOAD_TIMEOUT_MS = 120_000;

// Verified against the 2026-08-25 download: 47,863 data rows of which 46,960
// carry a ZCTA (903 are county territory with no ZCTA). The guard band
// refuses to replace existing data with an implausible parse — a truncated
// download or a changed layout must fail loudly, not load quietly.
const MIN_PLAUSIBLE_ROWS = 40_000;
const MAX_PLAUSIBLE_ROWS = 60_000;

// The columns the import reads, validated against the header row so a layout
// change fails with a clear message instead of loading wrong columns.
const REQUIRED_COLUMNS = ["GEOID_ZCTA5_20", "GEOID_COUNTY_20"] as const;

const FIVE_DIGITS = /^[0-9]{5}$/;

export type ZctaCountyRow = {
  zcta5: string;
  county_geoid: string;
};

export type ZctaCountyParseResult = {
  rows: ZctaCountyRow[];
  skipped_blank_zcta: number;
};

export function parseZctaCountyRelationshipFile(text: string): ZctaCountyParseResult {
  // The file ships with a UTF-8 BOM before the first header name.
  const withoutBom = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const lines = withoutBom.split(/\r?\n/).filter((line) => line.length > 0);
  if (lines.length === 0) {
    throw new Error("relationship file is empty");
  }

  const header = lines[0].split("|");
  const columnIndex = new Map(header.map((name, index) => [name, index]));
  for (const column of REQUIRED_COLUMNS) {
    if (!columnIndex.has(column)) {
      throw new Error(`relationship file header is missing column ${column}; got: ${header.join("|")}`);
    }
  }
  const zctaIndex = columnIndex.get("GEOID_ZCTA5_20")!;
  const countyIndex = columnIndex.get("GEOID_COUNTY_20")!;

  const rows: ZctaCountyRow[] = [];
  const seen = new Set<string>();
  let skippedBlankZcta = 0;
  for (const [lineNumber, line] of lines.slice(1).entries()) {
    const fields = line.split("|");
    if (fields.length !== header.length) {
      throw new Error(`line ${lineNumber + 2}: expected ${header.length} fields, got ${fields.length}`);
    }
    const zcta5 = fields[zctaIndex].trim();
    const countyGeoid = fields[countyIndex].trim();
    // Blank-ZCTA records are county territory without a ZCTA — expected, and
    // useless to the ZIP path.
    if (zcta5.length === 0) {
      skippedBlankZcta += 1;
      continue;
    }
    if (!FIVE_DIGITS.test(zcta5) || !FIVE_DIGITS.test(countyGeoid)) {
      throw new Error(`line ${lineNumber + 2}: invalid ZCTA "${zcta5}" or county GEOID "${countyGeoid}"`);
    }
    const key = `${zcta5}|${countyGeoid}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    rows.push({ zcta5, county_geoid: countyGeoid });
  }

  return { rows, skipped_blank_zcta: skippedBlankZcta };
}

async function downloadRelationshipFile(): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DOWNLOAD_TIMEOUT_MS);
  try {
    const response = await fetch(ZCTA_COUNTY_CROSSWALK_URL, { signal: controller.signal });
    if (!response.ok) {
      throw new Error(`download failed: status=${response.status} ${response.statusText}`);
    }
    return await response.text();
  } finally {
    clearTimeout(timeout);
  }
}

async function main(): Promise<void> {
  const fileFlagIndex = process.argv.indexOf("--file");
  const localPath = fileFlagIndex >= 0 ? process.argv[fileFlagIndex + 1] : null;
  if (fileFlagIndex >= 0 && !localPath) {
    throw new Error("--file requires a path argument");
  }

  console.log(localPath ? `Reading ${localPath}` : `Downloading ${ZCTA_COUNTY_CROSSWALK_URL}`);
  const text = localPath ? await readFile(localPath, "utf8") : await downloadRelationshipFile();

  const parsed = parseZctaCountyRelationshipFile(text);
  console.log(
    `Parsed ${parsed.rows.length} ZCTA/county relationships (skipped ${parsed.skipped_blank_zcta} blank-ZCTA rows)`
  );
  if (parsed.rows.length < MIN_PLAUSIBLE_ROWS || parsed.rows.length > MAX_PLAUSIBLE_ROWS) {
    throw new Error(
      `parsed ${parsed.rows.length} rows, outside the plausible band [${MIN_PLAUSIBLE_ROWS}, ${MAX_PLAUSIBLE_ROWS}]; refusing to replace existing data`
    );
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const client = await pool.connect();
  try {
    // Truncate-and-reload in one transaction: readers see the old data until
    // commit, and any failure leaves the previous load untouched.
    await client.query("BEGIN");
    await client.query("TRUNCATE public.address_zcta_county");
    const BATCH = 5_000;
    for (let offset = 0; offset < parsed.rows.length; offset += BATCH) {
      const batch = parsed.rows.slice(offset, offset + BATCH);
      await client.query(
        `
          INSERT INTO public.address_zcta_county (zcta5, county_geoid)
          SELECT * FROM unnest($1::text[], $2::text[])
        `,
        [batch.map((row) => row.zcta5), batch.map((row) => row.county_geoid)]
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }

  console.log(`Loaded ${parsed.rows.length} rows into address_zcta_county`);
}

// Only run as a CLI; tests import parseZctaCountyRelationshipFile directly.
if (process.argv[1]?.endsWith("importZctaCountyCrosswalk.ts")) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
