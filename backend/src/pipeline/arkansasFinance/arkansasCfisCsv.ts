// Arkansas CFIS bulk-export CSV parsing. The export machinery is the same
// Civix product as New Hampshire's (newHampshireCfsCsv.ts) and carries the
// same quirks: quoted multiline fields, invalid quote escaping inside
// otherwise complete rows, and a numeric first column that anchors record
// boundaries. The column sets are Arkansas-specific (pinned 2026-08-26).

export const ARKANSAS_RECEIPT_CSV_COLUMNS = [
  "Filing Entity ID",
  "Entity Name",
  "FilerType",
  "Transaction Type",
  "Transaction Sub Type",
  "Funding Source / Loan Source Type",
  "Source Name",
  "Source Address",
  "Employer Name",
  "Occupation",
  "Occupation Other",
  "Transaction Date",
  "Transaction Amount",
  "Transaction Description",
  "Transaction ID",
  "Election Type",
  "Election Year",
  "Guarantor Name",
  "Guarantor Address",
  "Report Filed Date",
  "Report Name",
  "Amended",
] as const;

export const ARKANSAS_EXPENDITURE_CSV_COLUMNS = [
  "Filing Entity ID",
  "Entity Name",
  "FilerType",
  "Transaction Type",
  "Transaction Sub Type",
  "Payee Type",
  "Payee Name",
  "Payee Address",
  "Transaction Date",
  "Transaction Amount",
  "Transaction Description",
  "Transaction ID",
  "Transaction Category",
  "Transaction Category Others",
  "Election Type",
  "Election Year",
  "Report Filed Date",
  "Report Name",
  "Amended",
] as const;

export type ArkansasReceiptCsvRow = Record<(typeof ARKANSAS_RECEIPT_CSV_COLUMNS)[number], string>;
export type ArkansasExpenditureCsvRow = Record<
  (typeof ARKANSAS_EXPENDITURE_CSV_COLUMNS)[number],
  string
>;

function isQuoteBoundary(record: string, quoteIndex: number): boolean {
  let index = quoteIndex + 1;
  while (record[index] === " " || record[index] === "\t") index += 1;
  const boundary = record[index];
  return boundary === "," || boundary === "\r" || boundary === "\n" || boundary === undefined;
}

function hasClosingQuote(record: string, openingIndex: number): boolean {
  for (let index = openingIndex + 1; index < record.length; index += 1) {
    if (record[index] !== '"') continue;
    if (record[index + 1] === '"') {
      if (isQuoteBoundary(record, index + 1)) return true;
      index += 1;
      continue;
    }
    if (isQuoteBoundary(record, index)) return true;
  }
  return false;
}

// Deliberately NOT RFC 4180: Civix never escapes quotes as `""`; it writes
// raw `"` inside values, so `""` followed by a field boundary means "literal
// quote, then the field ends" (live: `"Lead, Encourage, Elect PAC "LEE PAC"",`).
// On the 2022-2026 corpus this rule leaves 4 unparseable records vs 24 under
// RFC semantics, and every `"",` occurrence in the corpus is this shape.
function parseCsvRecord(record: string): string[] {
  const cells: string[] = [];
  let field = "";
  let inQuotes = false;
  for (let index = 0; index < record.length; index += 1) {
    const char = record[index];
    const next = record[index + 1];
    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
        if (isQuoteBoundary(record, index)) inQuotes = false;
      } else if (char === '"' && isQuoteBoundary(record, index)) {
        inQuotes = false;
      } else {
        field += char;
      }
    } else if (char === '"' && field.length === 0 && hasClosingQuote(record, index)) {
      inQuotes = true;
    } else if (char === ",") {
      cells.push(field);
      field = "";
    } else if (char !== "\r") {
      field += char;
    }
  }
  cells.push(field);
  return cells;
}

export function countArkansasCsvRecordColumns(record: string): number {
  return parseCsvRecord(record).length;
}

function hasOpenQuotedField(record: string): boolean {
  let fieldLength = 0;
  let inQuotes = false;
  for (let index = 0; index < record.length; index += 1) {
    const char = record[index];
    const next = record[index + 1];
    if (inQuotes) {
      if (char === '"' && next === '"') {
        fieldLength += 1;
        index += 1;
        if (isQuoteBoundary(record, index)) inQuotes = false;
      } else if (char === '"' && isQuoteBoundary(record, index)) {
        inQuotes = false;
      } else {
        fieldLength += 1;
      }
    } else if (char === '"' && fieldLength === 0) {
      inQuotes = true;
    } else if (char === ",") {
      fieldLength = 0;
    } else if (char !== "\r") {
      fieldLength += 1;
    }
  }
  return inQuotes;
}

export function isArkansasCsvRecordBoundary(
  currentRecord: string,
  nextLine: string,
  expectedColumnCount: number
): boolean {
  if (countArkansasCsvRecordColumns(currentRecord) < expectedColumnCount) return false;
  if (!hasOpenQuotedField(currentRecord)) return true;
  return (
    !hasOpenQuotedField(nextLine) && countArkansasCsvRecordColumns(nextLine) === expectedColumnCount
  );
}

function splitCsvRecords(csv: string, expectedColumnCount: number): { header: string; records: string[] } {
  const normalized = csv.replace(/^\uFEFF/, "");
  const firstNewline = normalized.indexOf("\n");
  if (firstNewline < 0) {
    return { header: normalized.replace(/\r$/, ""), records: [] };
  }
  const header = normalized.slice(0, firstNewline).replace(/\r$/, "");
  const data = normalized.slice(firstNewline + 1).replace(/\r?\n$/, "");
  const records: string[] = [];
  let currentRecord: string | null = null;
  for (const line of data.length > 0 ? data.split(/\r?\n/) : []) {
    if (currentRecord === null) {
      currentRecord = line;
      continue;
    }
    if (/^\d+,/.test(line) && isArkansasCsvRecordBoundary(currentRecord, line, expectedColumnCount)) {
      records.push(currentRecord);
      currentRecord = line;
    } else {
      currentRecord += `\n${line}`;
    }
  }
  if (currentRecord !== null) records.push(currentRecord);
  return { header, records: records.filter((record) => record.trim()) };
}

function validateHeaderCells<const TColumns extends readonly string[]>(
  header: readonly string[],
  columns: TColumns,
  label: string
): void {
  const normalized = header.map((value, index) => (index === 0 ? value.replace(/^\uFEFF/, "") : value));
  if (normalized.length !== columns.length || normalized.some((value, index) => value !== columns[index])) {
    throw new Error(`Arkansas CFIS ${label} CSV header changed: ${JSON.stringify(normalized)}`);
  }
}

// A small number of live rows are unrepairably mis-quoted at the source:
// Civix's export breaks RFC quoting when a field VALUE contains a literal
// double-quote (observed 2026-08-26, e.g. an address entered as
// `"1904 Lee Creek Drive<tab>"`). Those records cannot be split into columns
// deterministically, so callers either quarantine them (onMalformed) or the
// parse fails closed.
export type ArkansasMalformedCsvRecord = {
  rowNumber: number;
  columnCount: number;
  recordPreview: string;
};

function forEachRecord<const TColumns extends readonly string[]>(
  csv: string,
  columns: TColumns,
  label: string,
  callback: (row: Record<TColumns[number], string>, rowNumber: number) => void,
  onMalformed?: (malformed: ArkansasMalformedCsvRecord) => void
): number {
  const { header, records } = splitCsvRecords(csv, columns.length);
  if (!header.trim()) {
    throw new Error(`Arkansas CFIS ${label} CSV is empty`);
  }
  validateHeaderCells(parseCsvRecord(header), columns, label);
  records.forEach((record, index) => {
    const rowNumber = index + 2;
    const cells = parseCsvRecord(record);
    if (cells.length !== columns.length) {
      if (onMalformed) {
        onMalformed({ rowNumber, columnCount: cells.length, recordPreview: record.slice(0, 240) });
        return;
      }
      throw new Error(
        `Arkansas CFIS ${label} CSV row ${rowNumber} has ${cells.length} columns; expected ${columns.length}`
      );
    }
    callback(
      Object.fromEntries(columns.map((column, cellIndex) => [column, cells[cellIndex] ?? ""])) as Record<
        TColumns[number],
        string
      >,
      rowNumber
    );
  });
  return records.length;
}

export function validateArkansasReceiptCsvHeader(headerRecord: string): void {
  validateHeaderCells(parseCsvRecord(headerRecord), ARKANSAS_RECEIPT_CSV_COLUMNS, "receipt");
}

export function validateArkansasExpenditureCsvHeader(headerRecord: string): void {
  validateHeaderCells(parseCsvRecord(headerRecord), ARKANSAS_EXPENDITURE_CSV_COLUMNS, "expenditure");
}

// Bounded in-memory parsing, not true streaming: the whole artifact string
// (69-119 MB for 2023-2026 receipts) and its record strings are held at once,
// but rows are handed to the callback one at a time so callers never
// materialize 300k+ row objects. Revisit with a byte-stream parser only if
// exports outgrow the Node heap.
export function forEachArkansasReceiptCsvRow(
  csv: string,
  callback: (row: ArkansasReceiptCsvRow, rowNumber: number) => void,
  onMalformed?: (malformed: ArkansasMalformedCsvRecord) => void
): number {
  return forEachRecord(csv, ARKANSAS_RECEIPT_CSV_COLUMNS, "receipt", callback, onMalformed);
}

export function forEachArkansasExpenditureCsvRow(
  csv: string,
  callback: (row: ArkansasExpenditureCsvRow, rowNumber: number) => void,
  onMalformed?: (malformed: ArkansasMalformedCsvRecord) => void
): number {
  return forEachRecord(csv, ARKANSAS_EXPENDITURE_CSV_COLUMNS, "expenditure", callback, onMalformed);
}

export function parseArkansasReceiptCsv(csv: string): ArkansasReceiptCsvRow[] {
  const rows: ArkansasReceiptCsvRow[] = [];
  forEachArkansasReceiptCsvRow(csv, (row) => rows.push(row));
  return rows;
}

export function parseArkansasExpenditureCsv(csv: string): ArkansasExpenditureCsvRow[] {
  const rows: ArkansasExpenditureCsvRow[] = [];
  forEachArkansasExpenditureCsvRow(csv, (row) => rows.push(row));
  return rows;
}

// Amounts arrive as "$5,000.00"; a parenthesized amount is negative.
export function parseArkansasCurrencyCents(value: string): number {
  const trimmed = value.trim();
  const parenthesized = /^\(.*\)$/.test(trimmed);
  const normalized = (parenthesized ? trimmed.slice(1, -1) : trimmed)
    .replace(/^(-?)\$/, "$1")
    .replace(/,/g, "");
  if (!/^-?\d+(?:\.\d{1,2})?$/.test(normalized)) {
    throw new Error(`Invalid Arkansas CFIS currency amount: ${JSON.stringify(value)}`);
  }
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    throw new Error(`Invalid Arkansas CFIS currency amount: ${JSON.stringify(value)}`);
  }
  const cents = Math.round(amount * 100);
  return parenthesized ? -cents : cents;
}

export type ArkansasOccupationSource = "occupation" | "occupation_other" | "none";

export type ArkansasMergedOccupation = {
  value: string | null;
  source: ArkansasOccupationSource;
};

// The export splits occupation across a dropdown column ("Occupation") and a
// free-text column ("Occupation Other"); dropdown values can also arrive
// wrapped as "Other(<text>)". Prefer the dropdown, fall back to free text,
// and unwrap the Other(...) wrapper in either position.
export function mergeArkansasOccupation(
  occupation: string,
  occupationOther: string
): ArkansasMergedOccupation {
  const unwrap = (raw: string): string => {
    const trimmed = raw.trim();
    const match = /^Other\s*\((.*)\)$/is.exec(trimmed);
    const inner = match?.[1]?.trim();
    return inner !== undefined && inner.length > 0 ? inner : trimmed;
  };
  const primary = unwrap(occupation);
  if (primary && !/^other$/i.test(primary)) {
    return { value: primary, source: "occupation" };
  }
  const secondary = unwrap(occupationOther);
  if (secondary) {
    return { value: secondary, source: "occupation_other" };
  }
  return { value: null, source: "none" };
}
