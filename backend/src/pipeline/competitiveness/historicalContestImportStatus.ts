import type { Pool, PoolClient, QueryResultRow } from "pg";

import {
  VERIFIED_HISTORICAL_CONTEST_SOURCES,
  type HistoricalContestSourceDefinition,
} from "./historicalContestSources.js";
import type { HistoricalContestOfficeType } from "./historicalContestKeys.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type HistoricalContestImportStatusRow = QueryResultRow & {
  source: string;
  election_year: number;
  office_type: HistoricalContestOfficeType;
  district_type: string;
  stale_after_redistricting: boolean;
  row_count: string;
  latest_imported_at: Date | string | null;
  source_urls: string[] | null;
};

export type HistoricalContestImportStatusGroup = {
  source: string;
  election_year: number;
  office_type: HistoricalContestOfficeType;
  district_type: string;
  stale_after_redistricting: boolean;
  row_count: number;
  latest_imported_at: string | null;
  source_urls: string[];
};

export type VerifiedHistoricalContestSourceStatus = {
  preset: string;
  source: string;
  source_url: string;
  format: HistoricalContestSourceDefinition["format"];
  election_year: number;
  office_type: HistoricalContestOfficeType;
  imported: boolean;
  row_count: number;
  latest_imported_at: string | null;
};

export type HistoricalContestImportStatus = {
  total_records: number;
  groups: HistoricalContestImportStatusGroup[];
  verified_sources: VerifiedHistoricalContestSourceStatus[];
};

function toIsoString(value: Date | string | null): string | null {
  if (value === null) {
    return null;
  }
  return value instanceof Date ? value.toISOString() : value;
}

function parseRowCount(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid historical contest import status row count: ${value}`);
  }
  return parsed;
}

function groupKey(input: {
  source: string;
  election_year: number;
  office_type: HistoricalContestOfficeType;
}): string {
  return `${input.source}\0${input.election_year}\0${input.office_type}`;
}

function mapStatusRow(row: HistoricalContestImportStatusRow): HistoricalContestImportStatusGroup {
  return {
    source: row.source,
    election_year: row.election_year,
    office_type: row.office_type,
    district_type: row.district_type,
    stale_after_redistricting: row.stale_after_redistricting,
    row_count: parseRowCount(row.row_count),
    latest_imported_at: toIsoString(row.latest_imported_at),
    source_urls: row.source_urls ?? [],
  };
}

function buildVerifiedSourceStatuses(input: {
  groups: readonly HistoricalContestImportStatusGroup[];
  sources?: readonly HistoricalContestSourceDefinition[];
}): VerifiedHistoricalContestSourceStatus[] {
  const groupsByKey = new Map<string, HistoricalContestImportStatusGroup[]>();
  for (const group of input.groups) {
    const key = groupKey(group);
    groupsByKey.set(key, [...(groupsByKey.get(key) ?? []), group]);
  }

  return (input.sources ?? VERIFIED_HISTORICAL_CONTEST_SOURCES).flatMap((source) =>
    source.officeTypes.map((officeType) => {
      const matchingGroups = groupsByKey.get(
        groupKey({
          source: source.source,
          election_year: source.electionYear,
          office_type: officeType,
        })
      ) ?? [];
      const rowCount = matchingGroups.reduce((sum, group) => sum + group.row_count, 0);
      const [latestImportedAt] = matchingGroups
        .map((group) => group.latest_imported_at)
        .filter((value): value is string => value !== null)
        .sort((left, right) => right.localeCompare(left));

      return {
        preset: source.preset,
        source: source.source,
        source_url: source.sourceUrl,
        format: source.format,
        election_year: source.electionYear,
        office_type: officeType,
        imported: rowCount > 0,
        row_count: rowCount,
        latest_imported_at: latestImportedAt ?? null,
      };
    })
  );
}

export async function loadHistoricalContestImportStatus(
  db: Queryable
): Promise<HistoricalContestImportStatus> {
  const result = await db.query<HistoricalContestImportStatusRow>(
    `
      SELECT
        source,
        election_year,
        office_type,
        district_type,
        stale_after_redistricting,
        COUNT(*)::text AS row_count,
        MAX(imported_at) AS latest_imported_at,
        array_remove(array_agg(DISTINCT source_url ORDER BY source_url), NULL) AS source_urls
      FROM public.historical_contest_margins
      GROUP BY
        source,
        election_year,
        office_type,
        district_type,
        stale_after_redistricting
      ORDER BY
        election_year DESC,
        source ASC,
        office_type ASC,
        district_type ASC,
        stale_after_redistricting ASC
    `
  );

  const groups = result.rows.map(mapStatusRow);

  return {
    total_records: groups.reduce((sum, group) => sum + group.row_count, 0),
    groups,
    verified_sources: buildVerifiedSourceStatuses({ groups }),
  };
}
