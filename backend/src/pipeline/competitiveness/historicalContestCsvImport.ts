import type { Pool, PoolClient } from "pg";

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
  "district",
  "candidatevotes",
  "totalvotes",
] as const;

function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    const next = csv[index + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    if (char === "\r") {
      continue;
    }

    field += char;
  }

  if (inQuotes) {
    throw new Error("CSV has an unterminated quoted field");
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter((cells) => cells.some((cell) => cell.trim().length > 0));
}

function normalizeHeader(value: string): string {
  return value.trim().toLowerCase();
}

function requireColumn(headerIndex: Map<string, number>, column: string): number {
  const index = headerIndex.get(column);
  if (index === undefined) {
    throw new Error(`Missing required MEDSL CSV column: ${column}`);
  }
  return index;
}

function cell(cells: readonly string[], index: number): string {
  return cells[index]?.trim() ?? "";
}

export function parseMedslHistoricalContestCsv(csv: string): MedslHistoricalContestCandidateRow[] {
  const rows = parseCsvRows(csv);
  const header = rows[0];
  if (!header) {
    return [];
  }

  const headerIndex = new Map<string, number>();
  header.forEach((name, index) => {
    const normalized = normalizeHeader(name);
    if (normalized) {
      headerIndex.set(normalized, index);
    }
  });

  const indexes = Object.fromEntries(
    REQUIRED_MEDSL_COLUMNS.map((column) => [column, requireColumn(headerIndex, column)])
  ) as Record<(typeof REQUIRED_MEDSL_COLUMNS)[number], number>;
  const partySimplifiedIndex = headerIndex.get("party_simplified");
  const partyDetailedIndex = headerIndex.get("party_detailed");
  const candidateIndex = headerIndex.get("candidate");
  const stageIndex = headerIndex.get("stage");

  return rows.slice(1).map((cells) => ({
    year: cell(cells, indexes.year),
    state_po: cell(cells, indexes.state_po),
    state_fips: cell(cells, indexes.state_fips),
    office: cell(cells, indexes.office),
    district: cell(cells, indexes.district),
    candidate: candidateIndex === undefined ? null : cell(cells, candidateIndex),
    candidatevotes: cell(cells, indexes.candidatevotes),
    totalvotes: cell(cells, indexes.totalvotes),
    party_simplified: partySimplifiedIndex === undefined ? null : cell(cells, partySimplifiedIndex),
    party_detailed: partyDetailedIndex === undefined ? null : cell(cells, partyDetailedIndex),
    stage: stageIndex === undefined ? null : cell(cells, stageIndex),
  }));
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
