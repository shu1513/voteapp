import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

import {
  NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS,
  NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS,
  countNewHampshireCsvRecordColumns,
  parseNewHampshireExpenditureCsvRecord,
  parseNewHampshireReceiptCsvRecord,
  validateNewHampshireExpenditureCsvHeader,
  validateNewHampshireReceiptCsvHeader,
  type NewHampshireExpenditureCsvRow,
  type NewHampshireReceiptCsvRow,
} from "./newHampshireCfsCsv.js";

const RECORD_START = /^\d+,/;

function normalizeMaxRows(value: number | undefined): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid New Hampshire CFS maxRows: ${value}`);
  }
  return value;
}

async function scanRows<TRow>(input: {
  filePath: string;
  label: string;
  validateHeader: (header: string) => void;
  parseRecord: (record: string, rowNumber: number) => TRow;
  expectedColumnCount: number;
  collectRows: boolean;
  predicate?: (row: TRow) => boolean;
  maxRows?: number;
}): Promise<{ rows: TRow[]; rowCount: number }> {
  const maxRows = normalizeMaxRows(input.maxRows);
  const source = createReadStream(input.filePath);
  const lines = createInterface({ input: source, crlfDelay: Infinity });
  const rows: TRow[] = [];
  let rowCount = 0;
  let physicalLine = 0;
  let recordStartLine = 0;
  let currentRecord: string | null = null;

  const consumeRecord = (record: string, startLine: number): boolean => {
    const row = input.parseRecord(record, startLine);
    rowCount += 1;
    if (input.collectRows && (!input.predicate || input.predicate(row))) rows.push(row);
    return input.collectRows && maxRows !== undefined && rows.length >= maxRows;
  };

  try {
    for await (const line of lines) {
      physicalLine += 1;
      if (physicalLine === 1) {
        input.validateHeader(line);
        continue;
      }

      if (RECORD_START.test(line) && currentRecord === null) {
        currentRecord = line;
        recordStartLine = physicalLine;
        continue;
      }

      if (currentRecord === null) {
        throw new Error(
          `New Hampshire CFS ${input.label} CSV line ${physicalLine} does not start with a numeric Filing Entity ID`
        );
      }

      if (
        RECORD_START.test(line) &&
        countNewHampshireCsvRecordColumns(currentRecord) >= input.expectedColumnCount
      ) {
        // Numeric comma-prefixed content is legal inside a quoted multiline
        // field. Only split after the accumulated row is structurally complete.
        if (consumeRecord(currentRecord, recordStartLine)) {
          lines.close();
          source.destroy();
          return { rows, rowCount };
        }
        currentRecord = line;
        recordStartLine = physicalLine;
        continue;
      }
      currentRecord += `\n${line}`;
    }

    if (physicalLine === 0) {
      throw new Error(`New Hampshire CFS ${input.label} CSV is empty`);
    }
    if (currentRecord !== null) consumeRecord(currentRecord, recordStartLine);
    return { rows, rowCount };
  } finally {
    lines.close();
    source.destroy();
  }
}

export function readNewHampshireReceiptCsvArtifact(input: {
  filePath: string;
  predicate?: (row: NewHampshireReceiptCsvRow) => boolean;
  maxRows?: number;
}): Promise<NewHampshireReceiptCsvRow[]> {
  return scanRows({
    ...input,
    label: "receipt",
    collectRows: true,
    expectedColumnCount: NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS.length,
    validateHeader: validateNewHampshireReceiptCsvHeader,
    parseRecord: parseNewHampshireReceiptCsvRecord,
  }).then(({ rows }) => rows);
}

export function readNewHampshireExpenditureCsvArtifact(input: {
  filePath: string;
  predicate?: (row: NewHampshireExpenditureCsvRow) => boolean;
  maxRows?: number;
}): Promise<NewHampshireExpenditureCsvRow[]> {
  return scanRows({
    ...input,
    label: "expenditure",
    collectRows: true,
    expectedColumnCount: NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS.length,
    validateHeader: validateNewHampshireExpenditureCsvHeader,
    parseRecord: parseNewHampshireExpenditureCsvRecord,
  }).then(({ rows }) => rows);
}

export async function validateNewHampshireReceiptCsvArtifact(input: {
  filePath: string;
}): Promise<{ rowCount: number }> {
  const { rowCount } = await scanRows({
    ...input,
    label: "receipt",
    collectRows: false,
    expectedColumnCount: NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS.length,
    validateHeader: validateNewHampshireReceiptCsvHeader,
    parseRecord: parseNewHampshireReceiptCsvRecord,
  });
  return { rowCount };
}

export async function validateNewHampshireExpenditureCsvArtifact(input: {
  filePath: string;
}): Promise<{ rowCount: number }> {
  const { rowCount } = await scanRows({
    ...input,
    label: "expenditure",
    collectRows: false,
    expectedColumnCount: NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS.length,
    validateHeader: validateNewHampshireExpenditureCsvHeader,
    parseRecord: parseNewHampshireExpenditureCsvRecord,
  });
  return { rowCount };
}
