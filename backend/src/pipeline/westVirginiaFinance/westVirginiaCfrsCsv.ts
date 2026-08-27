// Parsers for the WV CFRS nightly bulk CSVs. The files are cp1252 and
// malformed beyond encoding (verified live 2026-08-27):
// - quote characters appear inside unquoted fields (strict RFC-4180 parsers
//   fail; a lenient reader yields correct rows), and
// - both the contributions and expenditures files have rows with 1-3 extra
//   columns from unescaped commas in name/address fields ("Alonzio Perry,
//   II"; vendor addresses). Every observed one keeps a valid typed prefix
//   and a recoverable trailing FiledDate.
// Recovered rows keep only the first column of the damaged name/address span
// as the counterparty name — the ambiguous tail (which mixes in the street
// address) is discarded, matching the rule that addresses are never retained.
// Header drift always fails the artifact.

export type WestVirginiaCsvParseError = { line: number; reason: string };

export type WestVirginiaContributionCsvRow = {
  line: number;
  registrantId: string;
  committeeName: string;
  candidateName: string;
  transactionType: string;
  transactionCategory: string;
  transactionDate: string;
  amountCents: number;
  contributorType: string;
  contributorName: string;
  employerName: string | null;
  filedDate: string;
  /** True when the row had extra columns; the contributor name is then only
   * the first column of the damaged span and the employer column may be
   * unreliable. */
  recovered: boolean;
};

export type WestVirginiaExpenditureCsvRow = {
  line: number;
  registrantId: string;
  committeeName: string;
  candidateName: string;
  transactionType: string;
  expenditureType: string;
  expenditurePurpose: string;
  transactionDate: string;
  amountCents: number;
  recipientType: string;
  recipientName: string;
  filedDate: string;
  /** True when the row had extra columns; the recipient name is then only
   * the first column of the damaged span. */
  recovered: boolean;
};

export type WestVirginiaRegistrationCsvRow = {
  line: number;
  registrantId: string;
  committeeName: string;
  candidateName: string;
  committeeType: string;
  committeeSubType: string;
  registrationDate: string;
  committeeStatus: string;
};

export type WestVirginiaReportingScheduleCsvRow = {
  line: number;
  electionName: string;
  reportingCycle: string;
  reportingPeriodDescription: string;
  formType: string;
  reportType: string;
  beginDate: string;
  endDate: string;
  dueDate: string;
};

export type WestVirginiaCsvParseResult<T> = {
  rows: T[];
  errors: WestVirginiaCsvParseError[];
  recoveredRowCount: number;
};

const CONTRIBUTION_HEADER = [
  "RegistrantID",
  "CommitteeName",
  "CandidateName",
  "TransactionType",
  "TransactionCategory",
  "TransactionDate",
  "TransactionAmount",
  "ContributorPayeeType",
  "ContributorPayeeName",
  "ContributorAddress",
  "EmployerName",
  "FiledDate",
] as const;

const EXPENDITURE_HEADER = [
  "RegistrantID",
  "CommitteeName",
  "CandidateName",
  "TransactionType",
  "ExpenditureType",
  "ExpenditurePurpose",
  "TransactionDate",
  "TransactionAmount",
  "RecipientType",
  "RecipientName",
  "RecipientAddress",
  "FiledDate",
] as const;

const REGISTRATION_HEADER = [
  "RegistrantID",
  "CommitteeName",
  "CandidateName",
  "CommitteeType",
  "CommitteeSubType",
  "RegistrationDate",
  "CommitteeStatus",
] as const;

const REPORTING_SCHEDULE_HEADER = [
  "ElectionName",
  "ReportingCycle",
  "ReportingPeriodDescription",
  "FormType",
  "ReportType",
  "BeginDate",
  "Enddate",
  "DueDate",
] as const;

// Observed malformation caps at 3 extra columns; anything beyond that is
// unknown damage and fails the row rather than guessing.
const MAX_EXTRA_COLUMNS = 3;

const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
// Amounts arrive as plain decimals with up to four decimal places, and
// sub-dollar values omit the leading zero ("25.4400", "897.1000", ".9300");
// sub-cent precision has never been observed and fails closed rather than
// rounding.
const AMOUNT_PATTERN = /^-?(\d+(\.\d{1,4})?|\.\d{1,4})$/;

// Node's TextDecoder("windows-1252") silently degrades to latin1 on
// small-ICU builds (0x92 became U+0092 instead of U+2019 on the probe
// machine), so the cp1252-specific 0x80-0x9F range is remapped explicitly.
const CP1252_HIGH_CONTROL_MAP: Record<number, string> = {
  0x80: "€", 0x82: "‚", 0x83: "ƒ", 0x84: "„", 0x85: "…",
  0x86: "†", 0x87: "‡", 0x88: "ˆ", 0x89: "‰", 0x8a: "Š",
  0x8b: "‹", 0x8c: "Œ", 0x8e: "Ž", 0x91: "‘", 0x92: "’",
  0x93: "“", 0x94: "”", 0x95: "•", 0x96: "–", 0x97: "—",
  0x98: "˜", 0x99: "™", 0x9a: "š", 0x9b: "›", 0x9c: "œ",
  0x9e: "ž", 0x9f: "Ÿ",
};

export function decodeWestVirginiaCsvBytes(bytes: Uint8Array): string {
  const latin1 = Buffer.from(bytes).toString("latin1");
  return latin1.replace(/[\u0080-\u009f]/g, (char) => CP1252_HIGH_CONTROL_MAP[char.charCodeAt(0)] ?? char);
}

export function parseWestVirginiaAmountToCents(value: string): number | null {
  const trimmed = value.trim();
  if (!AMOUNT_PATTERN.test(trimmed)) return null;
  const negative = trimmed.startsWith("-");
  const unsigned = negative ? trimmed.slice(1) : trimmed;
  const [wholePart, fractionPart = ""] = unsigned.split(".");
  const fraction = fractionPart.padEnd(4, "0");
  const subCents = Number(fraction.slice(2));
  if (subCents !== 0) return null;
  const cents = Number(wholePart || "0") * 100 + Number(fraction.slice(0, 2));
  if (!Number.isSafeInteger(cents)) return null;
  return negative ? -cents : cents;
}

// Lenient CSV reader: a field starting with a double quote is a standard
// quoted field ("" escapes, embedded commas/newlines allowed); anywhere else
// a double quote is a literal character. This matches both observed
// malformations without breaking well-formed rows.
export function parseWestVirginiaCsvText(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let fieldStarted = false;

  const pushField = () => {
    row.push(field);
    field = "";
    fieldStarted = false;
  };
  const pushRow = () => {
    pushField();
    rows.push(row);
    row = [];
  };

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (inQuotes) {
      if (char === '"') {
        if (text[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }
      continue;
    }
    if (char === '"' && !fieldStarted && field.length === 0) {
      inQuotes = true;
      fieldStarted = true;
      continue;
    }
    if (char === ",") {
      pushField();
      continue;
    }
    if (char === "\n") {
      pushRow();
      continue;
    }
    if (char === "\r") {
      if (text[index + 1] === "\n") index += 1;
      pushRow();
      continue;
    }
    field += char;
    fieldStarted = true;
  }
  if (field.length > 0 || fieldStarted || row.length > 0) {
    pushRow();
  }
  // A trailing newline produces one empty single-field row; drop it.
  if (rows.length > 0) {
    const last = rows[rows.length - 1];
    if (last.length === 1 && last[0] === "") rows.pop();
  }
  return rows;
}

function requireHeader(rows: string[][], expected: readonly string[], label: string): void {
  const header = rows[0];
  if (!header || header.length !== expected.length || expected.some((name, i) => header[i]?.trim() !== name)) {
    throw new Error(
      `West Virginia ${label} CSV header drift: expected [${expected.join(", ")}], got [${(header ?? []).join(", ")}]`
    );
  }
}

function cleanField(value: string): string {
  return value.trim();
}

export function parseWestVirginiaContributionCsv(
  text: string
): WestVirginiaCsvParseResult<WestVirginiaContributionCsvRow> {
  const raw = parseWestVirginiaCsvText(text);
  requireHeader(raw, CONTRIBUTION_HEADER, "contributions");
  const rows: WestVirginiaContributionCsvRow[] = [];
  const errors: WestVirginiaCsvParseError[] = [];
  let recoveredRowCount = 0;
  for (let i = 1; i < raw.length; i += 1) {
    const line = i + 1;
    const cols = raw[i];
    const extraColumns = cols.length - CONTRIBUTION_HEADER.length;
    if (extraColumns < 0 || extraColumns > MAX_EXTRA_COLUMNS) {
      errors.push({ line, reason: `expected ${CONTRIBUTION_HEADER.length} columns, got ${cols.length}` });
      continue;
    }
    // Prefix through ContributorPayeeType (8 columns) is intact on every
    // observed bad-width row; the trailing two columns stay EmployerName +
    // FiledDate; the damaged span is the contributor name/address.
    const transactionDate = cleanField(cols[5]);
    const filedDate = cleanField(cols[cols.length - 1]);
    const amountCents = parseWestVirginiaAmountToCents(cols[6]);
    if (!ISO_DATE_PATTERN.test(transactionDate) || !ISO_DATE_PATTERN.test(filedDate) || amountCents === null) {
      errors.push({ line, reason: "invalid date or amount" });
      continue;
    }
    const recovered = extraColumns > 0;
    if (recovered) recoveredRowCount += 1;
    rows.push({
      line,
      registrantId: cleanField(cols[0]),
      committeeName: cleanField(cols[1]),
      candidateName: cleanField(cols[2]),
      transactionType: cleanField(cols[3]),
      transactionCategory: cleanField(cols[4]),
      transactionDate,
      amountCents,
      contributorType: cleanField(cols[7]),
      contributorName: cleanField(cols[8]),
      employerName: cleanField(cols[cols.length - 2]) || null,
      filedDate,
      recovered,
    });
  }
  return { rows, errors, recoveredRowCount };
}

export function parseWestVirginiaExpenditureCsv(
  text: string
): WestVirginiaCsvParseResult<WestVirginiaExpenditureCsvRow> {
  const raw = parseWestVirginiaCsvText(text);
  requireHeader(raw, EXPENDITURE_HEADER, "expenditures");
  const rows: WestVirginiaExpenditureCsvRow[] = [];
  const errors: WestVirginiaCsvParseError[] = [];
  let recoveredRowCount = 0;
  for (let i = 1; i < raw.length; i += 1) {
    const line = i + 1;
    const cols = raw[i];
    const extraColumns = cols.length - EXPENDITURE_HEADER.length;
    if (extraColumns < 0 || extraColumns > MAX_EXTRA_COLUMNS) {
      errors.push({ line, reason: `expected ${EXPENDITURE_HEADER.length} columns, got ${cols.length}` });
      continue;
    }
    // Prefix through RecipientType (9 columns) is intact on every observed
    // bad-width row; the trailing column is FiledDate; the damaged span is
    // the recipient name/address.
    const transactionDate = cleanField(cols[6]);
    const filedDate = cleanField(cols[cols.length - 1]);
    const amountCents = parseWestVirginiaAmountToCents(cols[7]);
    if (!ISO_DATE_PATTERN.test(transactionDate) || !ISO_DATE_PATTERN.test(filedDate) || amountCents === null) {
      errors.push({ line, reason: "invalid date or amount" });
      continue;
    }
    const recovered = extraColumns > 0;
    if (recovered) recoveredRowCount += 1;
    rows.push({
      line,
      registrantId: cleanField(cols[0]),
      committeeName: cleanField(cols[1]),
      candidateName: cleanField(cols[2]),
      transactionType: cleanField(cols[3]),
      expenditureType: cleanField(cols[4]),
      expenditurePurpose: cleanField(cols[5]),
      transactionDate,
      amountCents,
      recipientType: cleanField(cols[8]),
      recipientName: cleanField(cols[9]),
      filedDate,
      recovered,
    });
  }
  return { rows, errors, recoveredRowCount };
}

export function parseWestVirginiaRegistrationCsv(
  text: string
): WestVirginiaCsvParseResult<WestVirginiaRegistrationCsvRow> {
  const raw = parseWestVirginiaCsvText(text);
  requireHeader(raw, REGISTRATION_HEADER, "registrations");
  const rows: WestVirginiaRegistrationCsvRow[] = [];
  const errors: WestVirginiaCsvParseError[] = [];
  for (let i = 1; i < raw.length; i += 1) {
    const line = i + 1;
    const cols = raw[i];
    if (cols.length !== REGISTRATION_HEADER.length) {
      errors.push({ line, reason: `expected ${REGISTRATION_HEADER.length} columns, got ${cols.length}` });
      continue;
    }
    rows.push({
      line,
      registrantId: cleanField(cols[0]),
      committeeName: cleanField(cols[1]),
      candidateName: cleanField(cols[2]),
      committeeType: cleanField(cols[3]),
      committeeSubType: cleanField(cols[4]),
      registrationDate: cleanField(cols[5]),
      committeeStatus: cleanField(cols[6]),
    });
  }
  return { rows, errors, recoveredRowCount: 0 };
}

export function parseWestVirginiaReportingScheduleCsv(
  text: string
): WestVirginiaCsvParseResult<WestVirginiaReportingScheduleCsvRow> {
  const raw = parseWestVirginiaCsvText(text);
  requireHeader(raw, REPORTING_SCHEDULE_HEADER, "reporting-schedules");
  const rows: WestVirginiaReportingScheduleCsvRow[] = [];
  const errors: WestVirginiaCsvParseError[] = [];
  for (let i = 1; i < raw.length; i += 1) {
    const line = i + 1;
    const cols = raw[i];
    if (cols.length !== REPORTING_SCHEDULE_HEADER.length) {
      errors.push({ line, reason: `expected ${REPORTING_SCHEDULE_HEADER.length} columns, got ${cols.length}` });
      continue;
    }
    rows.push({
      line,
      electionName: cleanField(cols[0]),
      reportingCycle: cleanField(cols[1]),
      reportingPeriodDescription: cleanField(cols[2]),
      formType: cleanField(cols[3]),
      reportType: cleanField(cols[4]),
      beginDate: cleanField(cols[5]),
      endDate: cleanField(cols[6]),
      dueDate: cleanField(cols[7]),
    });
  }
  return { rows, errors, recoveredRowCount: 0 };
}
