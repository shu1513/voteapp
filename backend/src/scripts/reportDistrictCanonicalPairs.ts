import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

/**
 * Reports `districts` rows where Census describes one place twice.
 *
 * Two different shapes share that description, and only one of them is a fault:
 *
 *   1. A CDP coextensive with, and named like, a real county-equivalent
 *      (Arlington CDP / Arlington County). A Census Designated Place has no
 *      government, so the CDP row is not a district at all. These are marked
 *      via districts.canonical_district_id by migration 226; an UNMARKED one is
 *      a fault this script exits non-zero on.
 *
 *   2. One real government holding both a county-equivalent FIPS and a place
 *      FIPS (Virginia's 38 independent cities, consolidated city-counties).
 *      Both rows are legitimate and both stay researchable: offices are scoped
 *      and county/place scopes do not overlap, so `Mayor` can only ever be
 *      written under the place row and `Sheriff` only under the county row.
 *      These are listed for orientation, NOT flagged.
 *
 * Read-only. Run: npm run districts:canonical-pairs
 */

type PairRow = {
  state: string;
  county_geoid: string;
  county_name: string;
  county_elections: number;
  place_geoid: string;
  place_name: string;
  place_elections: number;
  place_is_cdp: boolean;
  marked: boolean;
};

// Coextensive rows share a population exactly, so that is the primary signal; a
// name check removes the coincidences it produces among small counties (King
// County, Texas has 211 residents, and so does Blackwell city).
const COEXTENSIVE_PAIRS_SQL = `
  WITH normalized AS (
    SELECT
      id, state, geoid_compact, district_type, population, name, canonical_district_id,
      lower(btrim(regexp_replace(
        regexp_replace(name, ',\\s*[A-Za-z ]+$', ''),
        '\\s+(County|Parish|Borough|Census Area|City and Borough|city and borough|city|City|town|village|CDP|municipality|Municipality|consolidated government|unified government|urban county|metropolitan government)$',
        ''
      ))) AS base
    FROM public.districts
    WHERE district_type IN ('county', 'place')
  )
  SELECT c.id AS county_id, p.id AS place_id
  FROM normalized AS c
  JOIN normalized AS p
    ON c.district_type = 'county'
   AND p.district_type = 'place'
   AND c.state = p.state
   AND c.population = p.population
   AND c.population > 0
  WHERE c.base = p.base
     OR position(c.base in p.base) > 0
     OR position(p.base in c.base) > 0
     -- Two consolidations whose names share nothing with their county.
     OR (c.geoid_compact = '11001' AND p.geoid_compact = '1150000')
     OR (c.geoid_compact = '13215' AND p.geoid_compact = '1319000')
`;

// Partially consolidated governments: the place row is the county minus the
// satellite cities that kept their own governments, so populations differ while
// the metro-wide offices are still elected by the whole county. Census marks
// these "(balance)" but the marker is not reliable on its own (Milford city
// (balance), Connecticut has no county government behind it, and Jacksonville
// carries no marker at all), so the set is enumerated.
const BALANCE_PAIR_GEOIDS: ReadonlyArray<readonly [string, string]> = [
  ["12031", "1235000"], // Duval County / Jacksonville city, Florida
  ["13059", "1303440"], // Clarke County / Athens-Clarke County unified government, Georgia
  ["13245", "1304204"], // Richmond County / Augusta-Richmond County consolidated government, Georgia
  ["18097", "1836003"], // Marion County / Indianapolis city, Indiana
  ["20071", "2028412"], // Greeley County / Greeley County unified government, Kansas
  ["21111", "2148006"], // Jefferson County / Louisville-Jefferson County metro government, Kentucky
  ["30093", "3011397"], // Silver Bow County / Butte-Silver Bow, Montana
  ["47037", "4752006"], // Davidson County / Nashville-Davidson metropolitan government, Tennessee
];

async function loadPairs(pool: Pool): Promise<PairRow[]> {
  const result = await pool.query<PairRow>(
    `
      WITH pairs AS (
        ${COEXTENSIVE_PAIRS_SQL}
        UNION
        SELECT c.id, p.id
        FROM unnest($1::text[], $2::text[]) AS b(county_geoid, place_geoid)
        JOIN public.districts AS c ON c.district_type = 'county' AND c.geoid_compact = b.county_geoid
        JOIN public.districts AS p ON p.district_type = 'place' AND p.geoid_compact = b.place_geoid
      )
      SELECT
        c.state,
        c.geoid_compact AS county_geoid,
        c.name AS county_name,
        (SELECT count(*)::int FROM public.elections e WHERE e.district_id = c.id) AS county_elections,
        p.geoid_compact AS place_geoid,
        p.name AS place_name,
        (SELECT count(*)::int FROM public.elections e WHERE e.district_id = p.id) AS place_elections,
        (p.name LIKE '%CDP%') AS place_is_cdp,
        (c.canonical_district_id IS NOT NULL OR p.canonical_district_id IS NOT NULL) AS marked
      FROM pairs
      JOIN public.districts AS c ON c.id = pairs.county_id
      JOIN public.districts AS p ON p.id = pairs.place_id
      ORDER BY c.state, c.population DESC
    `,
    [BALANCE_PAIR_GEOIDS.map((pair) => pair[0]), BALANCE_PAIR_GEOIDS.map((pair) => pair[1])]
  );
  return result.rows;
}

async function main(): Promise<void> {
  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl || databaseUrl.trim().length === 0) {
    throw new Error("Missing required env var: DATABASE_URL");
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const pairs = await loadPairs(pool);

    // A CDP has no government. One that impersonates a county-equivalent this
    // closely will be claimed and researched as if it were one, so it must be
    // marked. A district reload could introduce a new one.
    const unmarkedNonGovernmentRows = pairs.filter((pair) => pair.place_is_cdp && !pair.marked);

    // Both rows are real governments with non-overlapping office scopes. Listed
    // so the shape is visible; nothing to do about them.
    const twoRowGovernments = pairs
      .filter((pair) => !pair.place_is_cdp)
      .map((pair) => ({
        state: pair.state,
        county: `${pair.county_geoid} ${pair.county_name} (${pair.county_elections} elections)`,
        place: `${pair.place_geoid} ${pair.place_name} (${pair.place_elections} elections)`,
      }));

    // Marking a row that holds contests would hide them from every reader.
    // Migration 226 asserts this is empty; a later hand-edit could break it.
    const strandedResult = await pool.query<{
      district_id: string;
      name: string;
      district_type: string;
      elections: number;
    }>(
      `
        SELECT d.id AS district_id, d.name, d.district_type, count(e.id)::int AS elections
        FROM public.districts AS d
        JOIN public.elections AS e ON e.district_id = d.id
        WHERE d.canonical_district_id IS NOT NULL
        GROUP BY d.id, d.name, d.district_type
        ORDER BY count(e.id) DESC
      `
    );

    // Research finished against a row before it was marked. The work is done for
    // that government, but the owner still looks untouched, so the queue will
    // offer it again and an agent will redo the pass. Transferring a stamp or a
    // deferral is a data change, so it is reported, not applied.
    const strandedStateResult = await pool.query<{
      alias: string;
      owner: string;
      stranded: string;
      detail: string;
    }>(
      `
        SELECT
          a.district_type || ' ' || a.geoid_compact || ' ' || a.name AS alias,
          o.district_type || ' ' || o.geoid_compact || ' ' || o.name AS owner,
          'elections_searched_stamp' AS stranded,
          a.last_elections_searched_at::text AS detail
        FROM public.districts AS a
        JOIN public.districts AS o ON o.id = a.canonical_district_id
        WHERE a.last_elections_searched_at IS NOT NULL
          AND o.last_elections_searched_at IS NULL

        UNION ALL

        SELECT
          a.district_type || ' ' || a.geoid_compact || ' ' || a.name AS alias,
          o.district_type || ' ' || o.geoid_compact || ' ' || o.name AS owner,
          'active_deferral' AS stranded,
          md.stage || ' until ' || md.blocked_until::text AS detail
        FROM public.manual_research_deferrals AS md
        JOIN public.districts AS a ON a.id = md.district_id
        JOIN public.districts AS o ON o.id = a.canonical_district_id
        WHERE md.status = 'deferred'
          AND md.blocked_until > CURRENT_DATE
          AND NOT EXISTS (
            SELECT 1
            FROM public.manual_research_deferrals AS m2
            WHERE m2.district_id = o.id
              AND m2.status = 'deferred'
              AND m2.blocked_until > CURRENT_DATE
          )
        ORDER BY 1, 3
      `
    );

    const openQueueResult = await pool.query<{ count: number }>(
      `
        SELECT count(*)::int AS count
        FROM public.manual_district_research_requests AS r
        JOIN public.districts AS d ON d.id = r.district_id
        WHERE d.canonical_district_id IS NOT NULL
          AND r.status IN ('queued', 'claimed', 'running')
      `
    );

    console.log(
      JSON.stringify(
        {
          unmarked_non_government_rows: unmarkedNonGovernmentRows,
          suppressed_districts_holding_elections: strandedResult.rows,
          research_state_stranded_on_suppressed_rows: strandedStateResult.rows,
          open_queue_requests_on_suppressed_districts: openQueueResult.rows[0]?.count ?? 0,
          two_row_governments_not_a_fault: {
            count: twoRowGovernments.length,
            note:
              "One government, two Census rows. Both stay researchable: county and place office " +
              "scopes do not overlap, so each row can only hold its own half of the ballot.",
            pairs: twoRowGovernments,
          },
        },
        null,
        2
      )
    );

    if (unmarkedNonGovernmentRows.length > 0 || strandedResult.rows.length > 0) {
      process.exitCode = 1;
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
