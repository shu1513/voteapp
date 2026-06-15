import type { Pool, PoolClient } from "pg";

import {
  buildCsvHeaderIndex,
  csvCell,
  parseCsvRows,
  requireAnyCsvColumn,
  requireCsvColumn,
} from "./historicalContestCsv.js";
import {
  normalizeMedslHistoricalContestMargins,
  type HistoricalContestMarginRecord,
  type HistoricalContestNormalizationSkippedRow,
  type MedslHistoricalContestCandidateRow,
} from "./historicalContestNormalizer.js";
import {
  upsertHistoricalContestMargins,
  type HistoricalContestMarginWriteResult,
} from "./historicalContestMarginWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type HistoricalContestCsvImportResult = {
  parsedRows: number;
  normalizedRecords: number;
  skippedRows: HistoricalContestNormalizationSkippedRow[];
  writeResult: HistoricalContestMarginWriteResult | null;
};

const REQUIRED_MEDSL_COLUMNS = [
  "year",
  "state_po",
  "state_fips",
  "office",
  "totalvotes",
] as const;

function isStatewideMitOffice(value: string): boolean {
  const office = value.trim().replace(/\s+/g, " ").toUpperCase();
  return office === "US PRESIDENT" || office === "US SENATE" || office === "GOVERNOR";
}

export function parseMedslHistoricalContestCsv(csv: string): MedslHistoricalContestCandidateRow[] {
  const rows = parseCsvRows(csv);
  const header = rows[0];
  if (!header) {
    return [];
  }

  const headerIndex = buildCsvHeaderIndex(header);

  const indexes = Object.fromEntries(
    REQUIRED_MEDSL_COLUMNS.map((column) => [column, requireCsvColumn(headerIndex, column)])
  ) as Record<(typeof REQUIRED_MEDSL_COLUMNS)[number], number>;
  const candidateVotesIndex = requireAnyCsvColumn(headerIndex, ["candidatevotes", "votes"]);
  const districtIndex = headerIndex.get("district");
  const partySimplifiedIndex = headerIndex.get("party_simplified");
  const partyDetailedIndex = headerIndex.get("party_detailed");
  const candidateIndex = headerIndex.get("candidate");
  const stageIndex = headerIndex.get("stage");

  return rows.slice(1).map((cells) => {
    const office = csvCell(cells, indexes.office);
    return {
      year: csvCell(cells, indexes.year),
      state_po: csvCell(cells, indexes.state_po),
      state_fips: csvCell(cells, indexes.state_fips),
      office,
      district:
        districtIndex === undefined && isStatewideMitOffice(office) ? "STATEWIDE" : csvCell(cells, districtIndex ?? -1),
      candidate: candidateIndex === undefined ? null : csvCell(cells, candidateIndex),
      candidatevotes: csvCell(cells, candidateVotesIndex),
      totalvotes: csvCell(cells, indexes.totalvotes),
      party_simplified: partySimplifiedIndex === undefined ? null : csvCell(cells, partySimplifiedIndex),
      party_detailed: partyDetailedIndex === undefined ? null : csvCell(cells, partyDetailedIndex),
      stage: stageIndex === undefined ? null : csvCell(cells, stageIndex),
    };
  });
}

export async function importHistoricalContestMarginsFromCsv(
  db: Queryable,
  input: {
    csv: string;
    source: string;
    sourceUrl?: string | null;
    staleAfterRedistricting?: boolean;
    dryRun?: boolean;
    importedAt?: Date;
  }
): Promise<HistoricalContestCsvImportResult> {
  const parsedRows = parseMedslHistoricalContestCsv(input.csv);
  const normalized = normalizeMedslHistoricalContestMargins({
    source: input.source,
    sourceUrl: input.sourceUrl,
    rows: parsedRows,
    staleAfterRedistricting: input.staleAfterRedistricting,
  });

  const records: HistoricalContestMarginRecord[] = normalized.records;
  const writeResult = input.dryRun
    ? null
    : await upsertHistoricalContestMargins(db, records, { importedAt: input.importedAt });

  return {
    parsedRows: parsedRows.length,
    normalizedRecords: records.length,
    skippedRows: normalized.skippedRows,
    writeResult,
  };
}
