import { createReadStream } from "node:fs";
import { StringDecoder } from "node:string_decoder";

export type CsvObject = Record<string, string>;

type CsvParserState = {
  row: string[];
  field: string;
  inQuotes: boolean;
  pendingQuote: boolean;
};

function consumeCsvText(
  state: CsvParserState,
  text: string,
  final: boolean,
  onRow: (row: string[]) => void
): void {
  let index = 0;
  if (state.pendingQuote) {
    state.pendingQuote = false;
    if (text[0] === '"') {
      state.field += '"';
      index = 1;
    } else {
      state.inQuotes = false;
    }
  }

  for (; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (state.inQuotes) {
      if (char === '"' && next === '"') {
        state.field += '"';
        index += 1;
      } else if (char === '"' && next === undefined && !final) {
        state.pendingQuote = true;
      } else if (char === '"') {
        state.inQuotes = false;
      } else {
        state.field += char;
      }
      continue;
    }
    if (char === '"') {
      state.inQuotes = true;
    } else if (char === ",") {
      state.row.push(state.field);
      state.field = "";
    } else if (char === "\n") {
      state.row.push(state.field);
      onRow(state.row);
      state.row = [];
      state.field = "";
    } else if (char !== "\r") {
      state.field += char;
    }
  }
}
function normalizeHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim();
}

function validateHeader(cells: readonly string[], requiredHeaders: readonly string[]): string[] {
  const header = cells.map(normalizeHeader);
  const nonEmpty = header.filter(Boolean);
  if (nonEmpty.length === 0) {
    throw new Error("CSV header row is empty");
  }
  if (new Set(nonEmpty).size !== nonEmpty.length) {
    throw new Error("CSV contains duplicate headers");
  }
  const names = new Set(header);
  const missing = requiredHeaders.filter((name) => !names.has(name));
  if (missing.length > 0) {
    throw new Error(`CSV missing required headers: ${missing.join(", ")}`);
  }
  return header;
}

function toObject(cells: readonly string[], header: readonly string[]): CsvObject {
  const row: CsvObject = {};
  for (let index = 0; index < header.length; index += 1) {
    const name = header[index];
    if (name) {
      row[name] = cells[index]?.trim() ?? "";
    }
  }
  return row;
}

export function parseCsvRecord(text: string): string[] {
  const state: CsvParserState = { row: [], field: "", inQuotes: false, pendingQuote: false };
  const rows: string[][] = [];
  consumeCsvText(state, text, true, (row) => rows.push(row));
  if (state.inQuotes || state.pendingQuote) throw new Error("CSV has an unterminated quoted field");
  if (state.field.length > 0 || state.row.length > 0) {
    state.row.push(state.field);
    rows.push(state.row);
  }
  if (rows.length !== 1) throw new Error(`Expected one CSV record, received ${rows.length}`);
  return rows[0] ?? [];
}

export async function readCsvObjects(input: {
  filePath: string;
  requiredHeaders?: readonly string[];
  onRow: (row: CsvObject) => void;
}): Promise<{ rowCount: number; headers: string[]; malformedRowCount: number }> {
  const decoder = new StringDecoder("utf8");
  const state: CsvParserState = { row: [], field: "", inQuotes: false, pendingQuote: false };
  let header: string[] | null = null;
  let rowCount = 0;
  let malformedRowCount = 0;

  const consumeRow = (cells: string[]): void => {
    if (!cells.some((value) => value.trim().length > 0)) {
      return;
    }
    if (!header) {
      header = validateHeader(cells, input.requiredHeaders ?? []);
      return;
    }
    if (cells.length !== header.length) {
      malformedRowCount += 1;
      return;
    }
    input.onRow(toObject(cells, header));
    rowCount += 1;
  };

  for await (const chunk of createReadStream(input.filePath)) {
    consumeCsvText(state, decoder.write(chunk as Buffer), false, consumeRow);
  }
  consumeCsvText(state, decoder.end(), true, consumeRow);
  if (state.inQuotes || state.pendingQuote) {
    throw new Error("CSV has an unterminated quoted field");
  }
  if (state.field.length > 0 || state.row.length > 0) {
    state.row.push(state.field);
    consumeRow(state.row);
  }
  if (!header) {
    throw new Error("CSV is empty");
  }
  return { rowCount, headers: header, malformedRowCount };
}
