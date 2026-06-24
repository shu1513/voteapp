import { createReadStream } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { StringDecoder } from "node:string_decoder";

import { normalizeMichiganMitnLegacyArchiveYear } from "./michiganMitnLegacyArtifactCache.js";

export const MICHIGAN_MITN_LEGACY_RECEIPTS_CSV_FILE_NAME_PATTERN = /^(\d{4})_mi_cfr_receipts\.csv$/i;
export const MICHIGAN_MITN_LEGACY_EXPENDITURES_CSV_FILE_NAME_PATTERN = /^(\d{4})_mi_cfr_expenditures\.csv$/i;
export const MICHIGAN_MITN_LEGACY_CONTRIBUTIONS_CSV_FILE_NAME_PATTERN =
  /^(\d{4})_mi_cfr_contributions(?: \d+ of \d+)?\.csv$/i;

export const MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS = [
  "doc_seq_no",
  "page_no",
  "contribution_id",
  "cont_detail_id",
  "doc_stmnt_year",
  "doc_type_desc",
  "com_legal_name",
  "common_name",
  "cfr_com_id",
  "com_type",
  "can_first_name",
  "can_last_name",
  "contribtype",
  "f_name",
  "l_name_or_org",
  "address",
  "city",
  "state",
  "zip",
  "occupation",
  "employer",
  "received_date",
  "amount",
  "aggregate",
  "extra_desc",
] as const;

export const MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS = [
  "doc_seq_no",
  "doc_stmnt_year",
  "doc_type_desc",
  "com_legal_name",
  "common_name",
  "cfr_com_id",
  "com_type",
  "schedule_desc",
  "supp_opp",
  "can_or_ballot",
  "_column_29",
  "amount",
] as const;

export type MichiganMitnLegacyCsvRow = Record<string, string>;
export type MichiganMitnLegacyCsvRowPredicate = (row: MichiganMitnLegacyCsvRow) => boolean;
export type MichiganMitnLegacyContributionRow = Record<
  (typeof MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS)[number],
  string
>;
export type MichiganMitnLegacyExpenditureRow = Record<
  (typeof MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS)[number],
  string
>;

function normalizePositiveInteger(value: number | undefined, fieldName: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid Michigan MiTN legacy ${fieldName}: ${value}`);
  }
  return value;
}

export function normalizeMichiganMitnLegacyCsvHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

function buildHeader(cells: readonly string[]): string[] {
  const normalizedCells = cells.map(normalizeMichiganMitnLegacyCsvHeader);
  if (!normalizedCells.some(Boolean)) {
    throw new Error("Michigan MiTN legacy CSV header row is empty");
  }
  const header = normalizedCells.map((name, index) => name || `_column_${index + 1}`);
  const seen = new Set<string>();
  for (const name of header) {
    if (seen.has(name)) {
      throw new Error(`Duplicate Michigan MiTN legacy CSV header: ${name}`);
    }
    seen.add(name);
  }
  return header;
}

function requireColumns(headers: readonly string[], requiredColumns: readonly string[] | undefined, tableLabel: string): void {
  if (!requiredColumns) {
    return;
  }
  const headerSet = new Set(headers);
  const missing = requiredColumns.filter((column) => !headerSet.has(column));
  if (missing.length > 0) {
    throw new Error(`Missing required Michigan MiTN legacy ${tableLabel} CSV column: ${missing[0]}`);
  }
}

function rowObjectFromCells(headers: readonly string[], cells: readonly string[]): MichiganMitnLegacyCsvRow {
  const row: MichiganMitnLegacyCsvRow = {};
  for (let index = 0; index < headers.length; index += 1) {
    const name = headers[index];
    row[name] = cells[index]?.trim() ?? "";
  }
  return row;
}

export function parseMichiganMitnLegacyCsvRows(input: {
  csv: string;
  requiredColumns?: readonly string[];
  tableLabel?: string;
}): MichiganMitnLegacyCsvRow[] {
  const parsedRows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < input.csv.length; index += 1) {
    const char = input.csv[index];
    const next = input.csv[index + 1];

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
      parsedRows.push(row);
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
    throw new Error("Michigan MiTN legacy CSV has an unterminated quoted field");
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    parsedRows.push(row);
  }

  const nonEmptyRows = parsedRows.filter((cells) => cells.some((value) => value.trim().length > 0));
  const headerCells = nonEmptyRows[0];
  if (!headerCells) {
    return [];
  }
  const headers = buildHeader(headerCells);
  requireColumns(headers, input.requiredColumns, input.tableLabel ?? "table");
  return nonEmptyRows.slice(1).map((cells) => rowObjectFromCells(headers, cells));
}

async function assertReadableFile(path: string, tableLabel: string): Promise<void> {
  const fileStat = await stat(path);
  if (!fileStat.isFile()) {
    throw new Error(`Michigan MiTN legacy ${tableLabel} CSV path is not a file: ${path}`);
  }
}

async function streamMichiganMitnLegacyCsvRows(input: {
  filePath: string;
  predicate?: MichiganMitnLegacyCsvRowPredicate;
  maxRows?: number;
  requiredColumns?: readonly string[];
  tableLabel: string;
}): Promise<MichiganMitnLegacyCsvRow[]> {
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  await assertReadableFile(input.filePath, input.tableLabel);
  const source = createReadStream(input.filePath);
  const decoder = new StringDecoder("utf8");

  return await new Promise((resolvePromise, reject) => {
    const rows: MichiganMitnLegacyCsvRow[] = [];
    let headers: string[] | null = null;
    let row: string[] = [];
    let field = "";
    let inQuotes = false;
    let pendingQuoteInQuotedField = false;
    let settled = false;

    const rejectOnce = (error: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      reject(error);
    };

    const resolveOnce = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      resolvePromise(rows);
    };

    const consumeCompletedRow = (cells: string[]): void => {
      if (!cells.some((value) => value.trim().length > 0)) {
        return;
      }
      if (!headers) {
        headers = buildHeader(cells);
        requireColumns(headers, input.requiredColumns, input.tableLabel);
        return;
      }
      if (maxRows !== undefined && rows.length >= maxRows) {
        return;
      }

      const parsedRow = rowObjectFromCells(headers, cells);
      if (!input.predicate || input.predicate(parsedRow)) {
        rows.push(parsedRow);
        if (maxRows !== undefined && rows.length >= maxRows) {
          source.destroy();
          resolveOnce();
        }
      }
    };

    const finishCurrentRow = (): void => {
      row.push(field);
      consumeCompletedRow(row);
      row = [];
      field = "";
    };

    const processText = (text: string, isFinal = false): void => {
      let index = 0;
      if (pendingQuoteInQuotedField) {
        pendingQuoteInQuotedField = false;
        if (text[0] === '"') {
          field += '"';
          index = 1;
        } else {
          inQuotes = false;
        }
      }

      for (; index < text.length && !settled; index += 1) {
        const char = text[index];
        const next = text[index + 1];

        if (inQuotes) {
          if (char === '"' && next === '"') {
            field += '"';
            index += 1;
          } else if (char === '"' && next === undefined && !isFinal) {
            pendingQuoteInQuotedField = true;
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
          finishCurrentRow();
          continue;
        }

        if (char === "\r") {
          continue;
        }

        field += char;
      }

      if (isFinal && pendingQuoteInQuotedField) {
        pendingQuoteInQuotedField = false;
        inQuotes = false;
      }
    };

    source.on("data", (chunk: string | Buffer) => {
      try {
        processText(typeof chunk === "string" ? chunk : decoder.write(chunk));
      } catch (error) {
        rejectOnce(error instanceof Error ? error : new Error(String(error)));
        source.destroy();
      }
    });
    source.on("end", () => {
      if (settled) {
        return;
      }
      try {
        processText(decoder.end(), true);
        if (inQuotes) {
          throw new Error(`Michigan MiTN legacy CSV has an unterminated quoted field: ${input.filePath}`);
        }
        if (field.length > 0 || row.length > 0) {
          finishCurrentRow();
        }
        resolveOnce();
      } catch (error) {
        rejectOnce(error instanceof Error ? error : new Error(String(error)));
      }
    });
    source.on("error", (error: Error) => {
      if (settled && error.message.includes("Premature close")) {
        return;
      }
      rejectOnce(error);
    });
  });
}

export async function listMichiganMitnLegacyExtractedFileNames(extractedDir: string): Promise<string[]> {
  const dir = resolve(extractedDir);
  const entries = await readdir(dir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .sort(compareMichiganMitnLegacyFileNames);
}

function contributionShardSortKey(fileName: string): { prefix: string; shard: number } | undefined {
  const match = /^(.*?\bcontributions)\s+(\d+)\s+of\s+\d+\.csv$/i.exec(fileName);
  return match?.[1] && match[2] ? { prefix: match[1], shard: Number.parseInt(match[2], 10) } : undefined;
}

function compareMichiganMitnLegacyFileNames(left: string, right: string): number {
  const lexical = left.localeCompare(right, undefined, { numeric: true });
  const leftShard = contributionShardSortKey(left);
  const rightShard = contributionShardSortKey(right);
  if (!leftShard || !rightShard) {
    return lexical;
  }
  const prefixCompare = leftShard.prefix.localeCompare(rightShard.prefix, undefined, { numeric: true });
  if (prefixCompare !== 0) {
    return prefixCompare;
  }
  return leftShard.shard - rightShard.shard || lexical;
}

export function michiganMitnLegacyReceiptsCsvFileName(year: number): string {
  return `${normalizeMichiganMitnLegacyArchiveYear(year)}_mi_cfr_receipts.csv`;
}

export function michiganMitnLegacyExpendituresCsvFileName(year: number): string {
  return `${normalizeMichiganMitnLegacyArchiveYear(year)}_mi_cfr_expenditures.csv`;
}

export async function listMichiganMitnLegacyContributionCsvFileNames(input: {
  extractedDir: string;
  year: number;
}): Promise<string[]> {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  return (await listMichiganMitnLegacyExtractedFileNames(input.extractedDir)).filter((fileName) => {
    const match = MICHIGAN_MITN_LEGACY_CONTRIBUTIONS_CSV_FILE_NAME_PATTERN.exec(fileName);
    return match?.[1] === String(year);
  });
}

export async function readMichiganMitnLegacyCsvTableRows(input: {
  extractedDir: string;
  fileName: string;
  predicate?: MichiganMitnLegacyCsvRowPredicate;
  maxRows?: number;
  requiredColumns?: readonly string[];
  tableLabel?: string;
}): Promise<MichiganMitnLegacyCsvRow[]> {
  const filePath = resolve(input.extractedDir, basename(input.fileName));
  return await streamMichiganMitnLegacyCsvRows({
    filePath,
    predicate: input.predicate,
    maxRows: input.maxRows,
    requiredColumns: input.requiredColumns,
    tableLabel: input.tableLabel ?? input.fileName,
  });
}

export async function readMichiganMitnLegacyContributionRows(input: {
  extractedDir: string;
  year: number;
  predicate?: (row: MichiganMitnLegacyContributionRow) => boolean;
  maxRows?: number;
}): Promise<MichiganMitnLegacyContributionRow[]> {
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  const fileNames = await listMichiganMitnLegacyContributionCsvFileNames(input);
  if (fileNames.length === 0) {
    throw new Error(`Michigan MiTN legacy contribution CSV files not found for ${input.year}`);
  }

  const rows: MichiganMitnLegacyContributionRow[] = [];
  for (const fileName of fileNames) {
    if (maxRows !== undefined && rows.length >= maxRows) {
      break;
    }
    const remainingRows = maxRows === undefined ? undefined : maxRows - rows.length;
    const fileRows = (await readMichiganMitnLegacyCsvTableRows({
      extractedDir: input.extractedDir,
      fileName,
      predicate: input.predicate as MichiganMitnLegacyCsvRowPredicate | undefined,
      maxRows: remainingRows,
      requiredColumns: MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS,
      tableLabel: fileName,
    })) as MichiganMitnLegacyContributionRow[];
    for (const row of fileRows) {
      rows.push(row);
    }
  }
  return rows;
}

export async function readMichiganMitnLegacyExpenditureRows(input: {
  extractedDir: string;
  year: number;
  predicate?: (row: MichiganMitnLegacyExpenditureRow) => boolean;
  maxRows?: number;
}): Promise<MichiganMitnLegacyExpenditureRow[]> {
  return (await readMichiganMitnLegacyCsvTableRows({
    extractedDir: input.extractedDir,
    fileName: michiganMitnLegacyExpendituresCsvFileName(input.year),
    predicate: input.predicate as MichiganMitnLegacyCsvRowPredicate | undefined,
    maxRows: input.maxRows,
    requiredColumns: MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS,
    tableLabel: "expenditures",
  })) as MichiganMitnLegacyExpenditureRow[];
}
