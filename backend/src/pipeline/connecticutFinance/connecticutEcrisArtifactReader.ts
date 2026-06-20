import { createReadStream } from "node:fs";
import { StringDecoder } from "node:string_decoder";

import type { ConnecticutEcrisArtifactFormat } from "./connecticutEcrisArtifactCache.js";

export type ConnecticutEcrisArtifactRow = Record<string, string>;

export type ConnecticutEcrisArtifactRowPredicate = (row: ConnecticutEcrisArtifactRow) => boolean;

export function normalizeConnecticutEcrisHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

function normalizePositiveInteger(value: number | undefined, fieldName: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid Connecticut eCRIS ${fieldName}: ${value}`);
  }
  return value;
}

function cell(cells: readonly string[], index: number): string {
  return cells[index]?.trim() ?? "";
}

function validateCsvFormat(format: ConnecticutEcrisArtifactFormat): void {
  if (format !== "csv") {
    throw new Error(
      `Connecticut eCRIS ${format.toUpperCase()} artifacts are not supported by the CSV reader; use a CSV artifact or add spreadsheet parsing explicitly`
    );
  }
}

function buildHeader(cells: readonly string[]): string[] {
  const header = cells.map(normalizeConnecticutEcrisHeader);
  const seen = new Set<string>();
  for (const name of header) {
    if (!name) {
      continue;
    }
    if (seen.has(name)) {
      throw new Error(`Duplicate Connecticut eCRIS CSV header: ${name}`);
    }
    seen.add(name);
  }
  if (seen.size === 0) {
    throw new Error("Connecticut eCRIS CSV header row is empty");
  }
  return header;
}

function rowObjectFromCells(cells: readonly string[], header: readonly string[]): ConnecticutEcrisArtifactRow {
  const row: ConnecticutEcrisArtifactRow = {};
  for (let index = 0; index < header.length; index += 1) {
    const name = header[index];
    if (!name) {
      continue;
    }
    row[name] = cell(cells, index);
  }
  return row;
}

export function parseConnecticutEcrisCsvRows(csv: string): ConnecticutEcrisArtifactRow[] {
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
    throw new Error("Connecticut eCRIS CSV has an unterminated quoted field");
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  const nonEmptyRows = rows.filter((cells) => cells.some((value) => value.trim().length > 0));
  const headerCells = nonEmptyRows[0];
  if (!headerCells) {
    return [];
  }
  const header = buildHeader(headerCells);
  return nonEmptyRows.slice(1).map((cells) => rowObjectFromCells(cells, header));
}

export async function readConnecticutEcrisArtifactRows(input: {
  filePath: string;
  format: ConnecticutEcrisArtifactFormat;
  predicate?: ConnecticutEcrisArtifactRowPredicate;
  maxRows?: number;
}): Promise<ConnecticutEcrisArtifactRow[]> {
  validateCsvFormat(input.format);
  const maxRows = normalizePositiveInteger(input.maxRows, "maxRows");
  const source = createReadStream(input.filePath);
  const decoder = new StringDecoder("utf8");

  return await new Promise((resolve, reject) => {
    const rows: ConnecticutEcrisArtifactRow[] = [];
    let header: string[] | null = null;
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
      resolve(rows);
    };

    const consumeCompletedRow = (cells: string[]): void => {
      if (!cells.some((value) => value.trim().length > 0)) {
        return;
      }
      if (!header) {
        header = buildHeader(cells);
        return;
      }
      if (maxRows !== undefined && rows.length >= maxRows) {
        return;
      }

      const parsedRow = rowObjectFromCells(cells, header);
      if (!input.predicate || input.predicate(parsedRow)) {
        rows.push(parsedRow);
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

      for (; index < text.length; index += 1) {
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
    };

    source.on("data", (chunk: string | Buffer) => {
      if (settled) {
        return;
      }
      try {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        processText(decoder.write(buffer));
      } catch (error) {
        rejectOnce(error as Error);
        source.destroy(error as Error);
      }
    });

    source.on("end", () => {
      if (settled) {
        return;
      }
      try {
        processText(decoder.end(), true);
        if (inQuotes || pendingQuoteInQuotedField) {
          rejectOnce(new Error("Connecticut eCRIS CSV has an unterminated quoted field"));
          return;
        }
        if (field.length > 0 || row.length > 0) {
          finishCurrentRow();
        }
        resolveOnce();
      } catch (error) {
        rejectOnce(error as Error);
      }
    });

    source.on("error", (error: Error) => rejectOnce(error));
  });
}
