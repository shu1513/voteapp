import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

import {
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

async function readRows<TRow>(input: {
  filePath: string;
  label: string;
  validateHeader: (header: string) => void;
  parseRecord: (record: string, rowNumber: number) => TRow;
  predicate?: (row: TRow) => boolean;
  maxRows?: number;
}): Promise<TRow[]> {
  const maxRows = normalizeMaxRows(input.maxRows);
  const source = createReadStream(input.filePath);
  const lines = createInterface({ input: source, crlfDelay: Infinity });
  const rows: TRow[] = [];
  let physicalLine = 0;
  let recordStartLine = 0;
  let currentRecord: string | null = null;

  const consumeRecord = (record: string, startLine: number): boolean => {
    const row = input.parseRecord(record, startLine);
    if (!input.predicate || input.predicate(row)) rows.push(row);
    return maxRows !== undefined && rows.length >= maxRows;
  };

  try {
    for await (const line of lines) {
      physicalLine += 1;
      if (physicalLine === 1) {
        input.validateHeader(line);
        continue;
      }

      if (RECORD_START.test(line)) {
        if (currentRecord !== null && consumeRecord(currentRecord, recordStartLine)) {
          lines.close();
          source.destroy();
          return rows;
        }
        currentRecord = line;
        recordStartLine = physicalLine;
        continue;
      }

      if (currentRecord === null) {
        throw new Error(
          `New Hampshire CFS ${input.label} CSV line ${physicalLine} does not start with a numeric Filing Entity ID`
        );
      }
      currentRecord += `\n${line}`;
    }

    if (physicalLine === 0) {
      throw new Error(`New Hampshire CFS ${input.label} CSV is empty`);
    }
    if (currentRecord !== null) consumeRecord(currentRecord, recordStartLine);
    return rows;
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
  return readRows({
    ...input,
    label: "receipt",
    validateHeader: validateNewHampshireReceiptCsvHeader,
    parseRecord: parseNewHampshireReceiptCsvRecord,
  });
}

export function readNewHampshireExpenditureCsvArtifact(input: {
  filePath: string;
  predicate?: (row: NewHampshireExpenditureCsvRow) => boolean;
  maxRows?: number;
}): Promise<NewHampshireExpenditureCsvRow[]> {
  return readRows({
    ...input,
    label: "expenditure",
    validateHeader: validateNewHampshireExpenditureCsvHeader,
    parseRecord: parseNewHampshireExpenditureCsvRecord,
  });
}
