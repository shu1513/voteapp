// Idaho Sunshine (Civix CFIS) bulk CSV contracts. Verified live 2026-09-01
// against filing years 2023–2026 (see backend/docs/idaho-campaign-finance.md).
//
// Quirks this module absorbs:
// - bytes are windows-1252, not UTF-8 (NBSP inside category strings);
// - the expenditure header spells "Filing Entity Name " with a trailing space;
// - the "Election Type" column holds the election YEAR and "Election Year"
//   holds the stage (Primary/General/...) — see idahoCsvElectionYear();
// - zip codes arrive as Excel formulas (="83702");
// - ~0.1% of records are split across physical lines by a raw newline
//   inside an unquoted field (re-joined below, NH precedent);
// - ~0.03% of records carry a corrupted apostrophe that became a comma
//   (",Äô") and cannot be repaired — they are quarantined, never thrown.

export const IDAHO_RECEIPT_CSV_COLUMNS = [
  "Filing Entity ID",
  "Filing Entity Name",
  "Campaign Name",
  "Registration Type",
  "Transaction Id",
  "Transaction Type",
  "Transaction Sub Type",
  "Contributor Type",
  "Contributor Last Name",
  "Contributor First Name",
  "Contributor Company Name",
  "Contributor Address Line 1",
  "Contributor Address Line 2",
  "Contributor Address City",
  "Contributor Address State",
  "Contributor Address Zip Code",
  "Transaction Date",
  "Transaction Amount",
  "Loan Interest Amount",
  "Total Loan Amount",
  "Election Type",
  "Election Year",
  "Transaction Description",
  "Amended",
  "Timed Report Name",
  "Timed Report Date",
  "Report Name",
  "Report Filed Date",
] as const;

export const IDAHO_EXPENDITURE_CSV_COLUMNS = [
  "Filing Entity ID",
  "Filing Entity Name ",
  "Campaign Name",
  "Registration Type",
  "Transaction Id",
  "Transaction Type",
  "Transaction Sub Type",
  "Purpose",
  "Payee Type",
  "Payee Last Name",
  "Payee First Name",
  "Payee Company Name",
  "Payee Address Line 1",
  "Payee Address Line 2",
  "Payee Address City",
  "Payee Address State",
  "Payee Address Zip Code",
  "Transaction Date",
  "Transaction Amount",
  "Election Type",
  "Election Year",
  "Transaction Description",
  "Public Distribution Start Date",
  "Public Distribution End Date",
  "Candidate Supported/Opposed",
  "Candidate Office Sought",
  "Measure Supported/Opposed",
  "Stance",
  "Amount Applied",
  "Amended",
  "Timed Report Name",
  "Timed Report Date",
  "Report Name",
  "Report Filed Date",
] as const;

export type IdahoReceiptCsvRow = Record<(typeof IDAHO_RECEIPT_CSV_COLUMNS)[number], string>;
export type IdahoExpenditureCsvRow = Record<(typeof IDAHO_EXPENDITURE_CSV_COLUMNS)[number], string>;

export type IdahoCsvQuarantinedRecord = {
  // 1-based physical line where the record starts (line 1 is the header).
  lineNumber: number;
  columnCount: number;
};

export type IdahoCsvParseResult<TRow> = {
  rows: TRow[];
  quarantined: IdahoCsvQuarantinedRecord[];
};

// Fail closed when more than this share of records is unparseable: that is a
// changed export, not the known trickle of corrupted rows.
export const IDAHO_CSV_MAX_QUARANTINED_SHARE = 0.01;

// No BOM observed live; strip one defensively (a UTF-8 BOM decodes to
// "\u00EF\u00BB\u00BF" under windows-1252).
export function decodeIdahoCfsCsv(bytes: Uint8Array): string {
  return new TextDecoder("windows-1252").decode(bytes).replace(/^(?:\uFEFF|\u00EF\u00BB\u00BF)/, "");
}

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
      // here is a re-joined split record and stays part of the field value.
      field += char;
    }
  }
  cells.push(field);
  return cells;
}

// Idaho exports contain no quoted line breaks (verified on every 2023–2026
// file), so a digits-then-comma line starts a new record whenever the current
// record is already complete, or whenever that line alone has the exact
// expected shape (then the short record before it is corrupt and is
// quarantined on its own instead of swallowing a good row).
function isRecordBoundary(currentRecord: string, nextLine: string, expectedColumnCount: number): boolean {
  return (
    parseCsvRecord(currentRecord).length >= expectedColumnCount ||
    parseCsvRecord(nextLine).length === expectedColumnCount
  );
}

const RECORD_START = /^\d+,/;

function splitRecords(
  csv: string,
  expectedColumnCount: number
): { header: string[]; records: Array<{ lineNumber: number; cells: string[] }> } {
  const lines = csv.split(/\r?\n/);
  if (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
  const headerLine = lines[0];
  if (headerLine === undefined || headerLine.trim() === "") {
    throw new Error("Idaho CFS CSV is empty");
  }
  const records: Array<{ lineNumber: number; cells: string[] }> = [];
  let currentRecord: string | null = null;
  let recordStartLine = 0;
  for (let index = 1; index < lines.length; index += 1) {
    const line = lines[index]!;
    const lineNumber = index + 1;
    if (currentRecord === null) {
      currentRecord = line;
      recordStartLine = lineNumber;
      continue;
    }
    // Filing Entity ID is numeric. A digits-then-comma line is a new record
    // only once the previous record already has all expected columns;
    // otherwise it is the tail of a record split by a raw newline.
    if (RECORD_START.test(line) && isRecordBoundary(currentRecord, line, expectedColumnCount)) {
      records.push({ lineNumber: recordStartLine, cells: parseCsvRecord(currentRecord) });
      currentRecord = line;
      recordStartLine = lineNumber;
    } else {
      currentRecord += `\n${line}`;
    }
  }
  if (currentRecord !== null && currentRecord.trim() !== "") {
    records.push({ lineNumber: recordStartLine, cells: parseCsvRecord(currentRecord) });
  }
  return { header: parseCsvRecord(headerLine), records };
}

function validateHeader<const TColumns extends readonly string[]>(
  header: readonly string[],
  columns: TColumns,
  label: string
): void {
  if (header.length !== columns.length || header.some((value, index) => value !== columns[index])) {
    throw new Error(`Idaho CFS ${label} CSV header changed: ${JSON.stringify(header)}`);
  }
}

function parseForColumns<const TColumns extends readonly string[]>(
  csv: string,
  columns: TColumns,
  label: string
): IdahoCsvParseResult<Record<TColumns[number], string>> {
  const { header, records } = splitRecords(csv, columns.length);
  validateHeader(header, columns, label);
  const rows: Record<TColumns[number], string>[] = [];
  const quarantined: IdahoCsvQuarantinedRecord[] = [];
  for (const record of records) {
    if (record.cells.length !== columns.length) {
      quarantined.push({ lineNumber: record.lineNumber, columnCount: record.cells.length });
      continue;
    }
    rows.push(
      Object.fromEntries(columns.map((column, index) => [column, record.cells[index] ?? ""])) as Record<
        TColumns[number],
        string
      >
    );
  }
  return { rows, quarantined };
}

// Callers that consume a whole export run this after parsing: a quarantine
// share above the tolerance means the export shape changed, not the known
// trickle of corrupted rows.
export function assertIdahoCsvQuarantineTolerance(
  result: IdahoCsvParseResult<unknown>,
  label: string
): void {
  const total = result.rows.length + result.quarantined.length;
  if (total > 0 && result.quarantined.length / total > IDAHO_CSV_MAX_QUARANTINED_SHARE) {
    throw new Error(
      `Idaho CFS ${label} CSV quarantined ${result.quarantined.length} of ${total} records; export shape has changed`
    );
  }
}

export function parseIdahoReceiptCsv(csv: string): IdahoCsvParseResult<IdahoReceiptCsvRow> {
  return parseForColumns(csv, IDAHO_RECEIPT_CSV_COLUMNS, "receipt");
}

export function parseIdahoExpenditureCsv(csv: string): IdahoCsvParseResult<IdahoExpenditureCsvRow> {
  return parseForColumns(csv, IDAHO_EXPENDITURE_CSV_COLUMNS, "expenditure");
}

export function parseIdahoCurrencyCents(value: string): number {
  const normalized = value.trim().replace(/^\$/, "").replace(/,/g, "");
  if (!/^-?\d+(?:\.\d{1,2})?$/.test(normalized)) {
    throw new Error(`Invalid Idaho CFS currency amount: ${JSON.stringify(value)}`);
  }
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    throw new Error(`Invalid Idaho CFS currency amount: ${JSON.stringify(value)}`);
  }
  return Math.round(amount * 100);
}

// Zip codes are exported as Excel formulas: ="83702" → 83702.
export function normalizeIdahoCsvZipCode(value: string): string {
  const match = /^="(.*)"$/.exec(value.trim());
  return (match ? match[1]! : value).trim();
}

type IdahoElectionColumns = { "Election Type": string; "Election Year": string };

// The two election columns are swapped in the export: "Election Type" holds
// the year ("2026") and "Election Year" holds the stage ("Primary").
export function idahoCsvElectionYear(row: IdahoElectionColumns): number | null {
  const value = row["Election Type"].trim();
  return /^\d{4}$/.test(value) ? Number(value) : null;
}

export function idahoCsvElectionStage(row: IdahoElectionColumns): string | null {
  const value = row["Election Year"].trim();
  return value.length > 0 ? value : null;
}
