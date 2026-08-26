import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";

// One-time-per-decade load of the Census 2020 ZCTA/place relationship file
// into address_zcta_place (docs/plans/partial-address-scope.md). Unlike the
// county crosswalk this stores a DECISION, not raw relationships: only ZCTAs
// wholly inside a single legally incorporated place get a row, so the ZIP
// partial-ballot path can offer that place's races with no geometry math at
// request time.
//
// Containment rule (all three must hold for a ZCTA):
// - exactly one place-overlap record (any class — a second place of ANY kind,
//   CDP included, means part of the ZCTA is outside the candidate place);
// - no land area outside every place (the file's blank-place record carries
//   the remainder; water-only remainders are fine — nobody votes offshore);
// - the one place is legally incorporated: CLASSFP C* (C1 city/town, C5, C7
//   independent city, C8 consolidated-government balance, even inactive C9 —
//   its district simply has no races). U*/M* (CDPs, military) are not
//   governments and never qualify.
//
// Usage:
//   npm run import:zcta-place-crosswalk            # download from Census
//   npm run import:zcta-place-crosswalk -- --file /path/to/tab20_...natl.txt

export const ZCTA_PLACE_CROSSWALK_URL =
  "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_place20_natl.txt";

const DOWNLOAD_TIMEOUT_MS = 120_000;

// Verified against the 2026-08-25 download: 82,880 data rows reduce to 3,087
// wholly-contained ZCTAs. The guard band refuses to replace existing data
// with an implausible parse — a truncated download or a changed layout must
// fail loudly, not load quietly.
const MIN_PLAUSIBLE_ROWS = 2_000;
const MAX_PLAUSIBLE_ROWS = 6_000;

// The columns the import reads, validated against the header row so a layout
// change fails with a clear message instead of loading wrong columns.
const REQUIRED_COLUMNS = ["GEOID_ZCTA5_20", "GEOID_PLACE_20", "CLASSFP_PLACE_20", "AREALAND_PART"] as const;

const FIVE_DIGITS = /^[0-9]{5}$/;
const SEVEN_DIGITS = /^[0-9]{7}$/;

export type ZctaPlaceRow = {
  zcta5: string;
  place_geoid: string;
};

export type ZctaPlaceParseResult = {
  rows: ZctaPlaceRow[];
  zctas_seen: number;
};

export function parseZctaPlaceRelationshipFile(text: string): ZctaPlaceParseResult {
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
  const placeIndex = columnIndex.get("GEOID_PLACE_20")!;
  const classIndex = columnIndex.get("CLASSFP_PLACE_20")!;
  const landIndex = columnIndex.get("AREALAND_PART")!;

  type ZctaState = {
    place_geoids: Set<string>;
    incorporated_place: string | null;
    outside_land: number;
  };
  const byZcta = new Map<string, ZctaState>();
  for (const [lineNumber, line] of lines.slice(1).entries()) {
    const fields = line.split("|");
    if (fields.length !== header.length) {
      throw new Error(`line ${lineNumber + 2}: expected ${header.length} fields, got ${fields.length}`);
    }
    const zcta5 = fields[zctaIndex].trim();
    // Blank-ZCTA records are place territory without a ZCTA — expected, and
    // useless to the ZIP path.
    if (zcta5.length === 0) {
      continue;
    }
    if (!FIVE_DIGITS.test(zcta5)) {
      throw new Error(`line ${lineNumber + 2}: invalid ZCTA "${zcta5}"`);
    }
    const placeGeoid = fields[placeIndex].trim();
    const landPartRaw = fields[landIndex].trim();
    const landPart = landPartRaw.length === 0 ? 0 : Number(landPartRaw);
    if (!Number.isFinite(landPart) || landPart < 0) {
      throw new Error(`line ${lineNumber + 2}: invalid AREALAND_PART "${landPartRaw}"`);
    }

    let state = byZcta.get(zcta5);
    if (!state) {
      state = { place_geoids: new Set(), incorporated_place: null, outside_land: 0 };
      byZcta.set(zcta5, state);
    }
    if (placeGeoid.length === 0) {
      // The blank-place record: ZCTA territory outside every place.
      state.outside_land += landPart;
      continue;
    }
    if (!SEVEN_DIGITS.test(placeGeoid)) {
      throw new Error(`line ${lineNumber + 2}: invalid place GEOID "${placeGeoid}"`);
    }
    state.place_geoids.add(placeGeoid);
    if (fields[classIndex].trim().startsWith("C")) {
      state.incorporated_place = placeGeoid;
    }
  }

  const rows: ZctaPlaceRow[] = [];
  for (const [zcta5, state] of byZcta) {
    if (state.place_geoids.size === 1 && state.outside_land === 0 && state.incorporated_place !== null) {
      rows.push({ zcta5, place_geoid: state.incorporated_place });
    }
  }
  rows.sort((a, b) => (a.zcta5 < b.zcta5 ? -1 : 1));

  return { rows, zctas_seen: byZcta.size };
}

async function downloadRelationshipFile(): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DOWNLOAD_TIMEOUT_MS);
  try {
    const response = await fetch(ZCTA_PLACE_CROSSWALK_URL, { signal: controller.signal });
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

  console.log(localPath ? `Reading ${localPath}` : `Downloading ${ZCTA_PLACE_CROSSWALK_URL}`);
  const text = localPath ? await readFile(localPath, "utf8") : await downloadRelationshipFile();

  const parsed = parseZctaPlaceRelationshipFile(text);
  console.log(`${parsed.zctas_seen} ZCTAs in the file; ${parsed.rows.length} wholly inside one incorporated place`);
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
    await client.query("TRUNCATE public.address_zcta_place");
    const BATCH = 5_000;
    for (let offset = 0; offset < parsed.rows.length; offset += BATCH) {
      const batch = parsed.rows.slice(offset, offset + BATCH);
      await client.query(
        `
          INSERT INTO public.address_zcta_place (zcta5, place_geoid)
          SELECT * FROM unnest($1::text[], $2::text[])
        `,
        [batch.map((row) => row.zcta5), batch.map((row) => row.place_geoid)]
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

  console.log(`Loaded ${parsed.rows.length} rows into address_zcta_place`);
}

// Only run as a CLI; tests import parseZctaPlaceRelationshipFile directly.
if (process.argv[1]?.endsWith("importZctaPlaceCrosswalk.ts")) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
