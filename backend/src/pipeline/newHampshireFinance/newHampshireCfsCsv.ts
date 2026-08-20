export const NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS = [
  "Filing Entity ID",
  "Candidate Name",
  "Committee Name",
  "Committee Subtype",
  "Transaction Type",
  "Transaction Sub Type",
  "Election Period",
  "Election year",
  "Date of Receipt",
  "Amount of receipt",
  "Contributor Type",
  "Contributor Name",
  "Contributor Address Line 1",
  "Contributor Address Line 2",
  "Contributor City",
  "Contributor State",
  "Contributor Zip Code",
  "Contributor occupation",
  "Contributor Employer",
  "Contributor Principle place of Business",
  "Description",
  "Timed Report",
] as const;

export const NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS = [
  "Filing Entity ID",
  "Filing Entity Name",
  "Filing Entity Type",
  "Transaction Type",
  "Transaction Sub Type",
  "Payee/ Worker /Creditor/ Loan source type",
  "Payee /Worker/Creditor/ Loan Source Name",
  "Payee/Worker/Creditor/Loan source Address",
  "Transaction Amount",
  "TransactionDate",
  "Election Type",
  "Transaction Description",
  "Timed Report",
] as const;

export type NewHampshireReceiptCsvRow = Record<(typeof NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS)[number], string>;
export type NewHampshireExpenditureCsvRow = Record<
  (typeof NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS)[number],
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
      // Record boundaries are established before field parsing, so a newline
      // here is a genuine multiline field and remains part of its value.
      field += char;
    }
  }
  cells.push(field);
  return cells;
}

export function countNewHampshireCsvRecordColumns(record: string): number {
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

export function isNewHampshireCsvRecordBoundary(
  currentRecord: string,
  nextLine: string,
  expectedColumnCount: number
): boolean {
  if (countNewHampshireCsvRecordColumns(currentRecord) < expectedColumnCount) return false;
  if (!hasOpenQuotedField(currentRecord)) return true;

  // Civix also emits otherwise complete single-line rows with an unmatched
  // quote. In that ambiguous case, split only when the candidate line is
  // independently quote-balanced and has the exact expected shape.
  return (
    !hasOpenQuotedField(nextLine) &&
    countNewHampshireCsvRecordColumns(nextLine) === expectedColumnCount
  );
}

function parseCsvRows(csv: string, expectedColumnCount: number): string[][] {
  const normalized = csv.replace(/^\uFEFF/, "");
  const firstNewline = normalized.indexOf("\n");
  if (firstNewline < 0) return normalized.trim() ? [parseCsvRecord(normalized)] : [];

  const header = normalized.slice(0, firstNewline).replace(/\r$/, "");
  const data = normalized.slice(firstNewline + 1).replace(/\r?\n$/, "");
  const records: string[] = [];
  let currentRecord: string | null = null;
  for (const line of data.length > 0 ? data.split(/\r?\n/) : []) {
    if (currentRecord === null) {
      currentRecord = line;
      continue;
    }

    // Filing Entity ID is numeric, but quoted multiline content can also
    // begin with digits and a comma. It is a boundary only after the prior
    // record has all expected columns. This remains tolerant of Civix's
    // invalid quote escaping inside otherwise complete rows.
    if (
      /^\d+,/.test(line) &&
      isNewHampshireCsvRecordBoundary(currentRecord, line, expectedColumnCount)
    ) {
      records.push(currentRecord);
      currentRecord = line;
    } else {
      currentRecord += `\n${line}`;
    }
  }
  if (currentRecord !== null) records.push(currentRecord);
  return [parseCsvRecord(header), ...records.filter((record) => record.trim()).map(parseCsvRecord)];
}

function validateHeaderCells<const TColumns extends readonly string[]>(
  header: readonly string[],
  columns: TColumns,
  label: string
): void {
  const normalized = header.map((value, index) => (index === 0 ? value.replace(/^\uFEFF/, "") : value));
  if (normalized.length !== columns.length || normalized.some((value, index) => value !== columns[index])) {
    throw new Error(`New Hampshire CFS ${label} CSV header changed: ${JSON.stringify(normalized)}`);
  }
}

function validateHeaderForColumns<const TColumns extends readonly string[]>(
  headerRecord: string,
  columns: TColumns,
  label: string
): void {
  validateHeaderCells(parseCsvRecord(headerRecord), columns, label);
}

function parseRecordForColumns<const TColumns extends readonly string[]>(
  record: string,
  columns: TColumns,
  label: string,
  rowNumber: number
): Record<TColumns[number], string> {
  const cells = parseCsvRecord(record);
  if (cells.length !== columns.length) {
    throw new Error(
      `New Hampshire CFS ${label} CSV row ${rowNumber} has ${cells.length} columns; expected ${columns.length}`
    );
  }
  return Object.fromEntries(columns.map((column, index) => [column, cells[index] ?? ""])) as Record<
    TColumns[number],
    string
  >;
}

function parseForColumns<const TColumns extends readonly string[]>(
  csv: string,
  columns: TColumns,
  label: string
): Record<TColumns[number], string>[] {
  const rows = parseCsvRows(csv, columns.length);
  const header = rows[0];
  if (!header) {
    throw new Error(`New Hampshire CFS ${label} CSV is empty`);
  }
  validateHeaderCells(header, columns, label);
  return rows.slice(1).map((cells, rowIndex) => {
    if (cells.length !== columns.length) {
      throw new Error(
        `New Hampshire CFS ${label} CSV row ${rowIndex + 2} has ${cells.length} columns; expected ${columns.length}`
      );
    }
    return Object.fromEntries(columns.map((column, index) => [column, cells[index] ?? ""])) as Record<
      TColumns[number],
      string
    >;
  });
}

export function validateNewHampshireReceiptCsvHeader(headerRecord: string): void {
  validateHeaderForColumns(headerRecord, NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS, "receipt");
}

export function validateNewHampshireExpenditureCsvHeader(headerRecord: string): void {
  validateHeaderForColumns(headerRecord, NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS, "expenditure");
}

export function parseNewHampshireReceiptCsvRecord(
  record: string,
  rowNumber: number
): NewHampshireReceiptCsvRow {
  return parseRecordForColumns(record, NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS, "receipt", rowNumber);
}

export function parseNewHampshireExpenditureCsvRecord(
  record: string,
  rowNumber: number
): NewHampshireExpenditureCsvRow {
  return parseRecordForColumns(record, NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS, "expenditure", rowNumber);
}

export function parseNewHampshireReceiptCsv(csv: string): NewHampshireReceiptCsvRow[] {
  return parseForColumns(csv, NEW_HAMPSHIRE_RECEIPT_CSV_COLUMNS, "receipt");
}

export function parseNewHampshireExpenditureCsv(csv: string): NewHampshireExpenditureCsvRow[] {
  return parseForColumns(csv, NEW_HAMPSHIRE_EXPENDITURE_CSV_COLUMNS, "expenditure");
}

export function parseNewHampshireCurrencyCents(value: string): number {
  const normalized = value.trim().replace(/^\$/, "").replace(/,/g, "");
  if (!/^-?\d+(?:\.\d{1,2})?$/.test(normalized)) {
    throw new Error(`Invalid New Hampshire CFS currency amount: ${JSON.stringify(value)}`);
  }
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    throw new Error(`Invalid New Hampshire CFS currency amount: ${JSON.stringify(value)}`);
  }
  return Math.round(amount * 100);
}
