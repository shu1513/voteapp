// Montana CERS payload parsers (docs/plans/montana-finance.md, Phase 1).
//
// Data surfaces (backend/docs/montana-campaign-finance.md, shapes re-verified
// live 2026-08-27 against Bedey / candidateId 21020):
// - Pipe-delimited CSV exports (CONTR + EXPEND): 18 columns, header row, no
//   quoting — a wrong column count is drift and fails closed. CONTR `Date
//   Paid` is the report-period START date (synthetic), never a transaction
//   date; real dates live in the report-detail JSON.
// - `listFinanceReports` DataTables JSON: one row per filed report with the
//   cash-begin chain anchors (`primCashBeg`/`genCashBeg`). The unitemized
//   lump fields (`totalContrLessThan35`/`grandTotalLessThan35*`) are ALWAYS
//   0 in the public flow — dead fields, deliberately not parsed.
// - `financeRepDetailList` JSON: a PLAIN ARRAY (not DataTables) of entry
//   rows per list name. An EMPTY BODY here is a failure (observed for
//   listNames that do not apply to the entity, e.g. `expendIndependent` on a
//   candidate report); a legitimate empty list is the two-byte `[]`.
//
// Money: JSON amounts are floats with 2 decimals; CSV amounts are plain
// `1234.56` strings (no $ or commas). Everything is converted to integer
// cents here and stays cents through the pipeline.

export const MONTANA_CERS_PARSER_VERSION = 1;

export const MONTANA_CERS_CONTRIBUTION_EXPORT_HEADER = [
  "Candidate ID",
  "Candidate Name",
  "Candidate Address",
  "Candidate Zip",
  "Reporting Date Range",
  "Contributor Name",
  "Contributor Address",
  "Contributor City/State/Zip",
  "Occupation",
  "Employer",
  "Date Paid",
  "Purpose",
  "Description",
  "Line Item",
  "Amount",
  "Election Type",
  "Amount Subtype",
  "Office Title",
] as const;

export const MONTANA_CERS_EXPENDITURE_EXPORT_HEADER = [
  "Candidate ID",
  "Candidate Name",
  "Candidate Address",
  "Candidate Zip",
  "Reporting Date Range",
  "Payee Name",
  "Payee Address",
  "Payee City/State/Zip",
  "Occupation",
  "Employer",
  "Date Paid",
  "Purpose",
  "Description",
  "Line Item",
  "Amount",
  "Election Type",
  "Amount Subtype",
  "Office Title",
] as const;

export type MontanaCersExportRow = {
  candidateId: number;
  candidateName: string;
  reportingDateRange: string;
  /** Contributor name on CONTR exports, payee name on EXPEND exports. */
  entityName: string;
  occupation: string | null;
  employer: string | null;
  /** MM/DD/YYYY. On CONTR rows this is the report-period START (synthetic). */
  datePaid: string;
  purpose: string | null;
  description: string | null;
  lineItem: string;
  amountCents: number;
  electionType: "Primary" | "General";
  amountSubtype: "Cash" | "In-Kind";
  officeTitle: string;
};

export type MontanaCersReportInventoryRow = {
  reportId: number;
  /** CERS entity id the report belongs to (candidateId for candidates). */
  entitySubId: number;
  formTypeCode: string;
  formTypeDescr: string | null;
  /** MM/DD/YYYY period bounds. */
  fromDateStr: string;
  toDateStr: string;
  reportTypeDescr: string | null;
  statusCode: string;
  statusDescr: string;
  /** Cash-begin chain anchors; null on report types without them (C7/C7E). */
  primCashBegCents: number | null;
  genCashBegCents: number | null;
  /** Epoch milliseconds. */
  receivedDate: number;
  amendedDate: number | null;
};

export type MontanaCersDetailRow = {
  amountTypeDescr: "Primary" | "General";
  cashAmtCents: number;
  inKindAmtCents: number;
  totalAmtCents: number;
  debtAmtCents: number;
  entityName: string | null;
  occupationDescr: string | null;
  employerDescr: string | null;
  /** Epoch milliseconds; real transaction date. */
  datePaid: number | null;
  lineItemCompositeDescr: string | null;
  purposeDescr: string | null;
  electioneeringInd: "Y" | "N";
  candidateContrInd: "Y" | "N";
};

export type MontanaCersCandidateSearchRow = {
  candidateId: number;
  lastName: string;
  firstName: string | null;
  middleInitial: string | null;
  electionYear: number | null;
  officeTitle: string | null;
  officeCode: string | null;
  partyDescr: string | null;
  candidateStatusDescr: string | null;
  resCountyDescr: string | null;
};

export class MontanaCersParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MontanaCersParseError";
  }
}

/**
 * Exact-cents parser for CSV amount strings. CERS exports write plain
 * `1234.56` / `1234.5` / `1234`; anything else (grouping commas, currency
 * symbols, parentheses) is drift and fails closed.
 */
export function parseMontanaCersCsvAmountCents(value: string): number {
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(value.trim());
  if (match === null) {
    throw new MontanaCersParseError(`Unparseable Montana CERS amount: ${JSON.stringify(value)}`);
  }
  const sign = match[1] === "-" ? -1 : 1;
  const cents = Number(match[2]) * 100 + Number((match[3] ?? "0").padEnd(2, "0"));
  if (!Number.isSafeInteger(cents)) {
    throw new MontanaCersParseError(`Montana CERS amount out of range: ${value}`);
  }
  return sign * cents;
}

/**
 * JSON amounts arrive as floats representing 2-decimal dollar values
 * (e.g. 17840.09). Round-to-cents is exact for that shape; anything not
 * within float noise of a cent grid point is drift.
 */
export function montanaCersJsonAmountToCents(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new MontanaCersParseError(`Non-numeric Montana CERS amount in ${field}: ${JSON.stringify(value)}`);
  }
  const cents = Math.round(value * 100);
  // Tolerance covers double representation error only (~2e-7 even at $10M);
  // a genuinely fractional-cent value (12.34009) is upstream precision
  // drift and must fail closed, never silently truncate.
  if (!Number.isSafeInteger(cents) || Math.abs(value * 100 - cents) > 1e-6) {
    throw new MontanaCersParseError(`Montana CERS amount is not a cent value in ${field}: ${value}`);
  }
  return cents;
}

function optionalJsonAmountToCents(value: unknown, field: string): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  return montanaCersJsonAmountToCents(value, field);
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new MontanaCersParseError(`Missing Montana CERS field ${field}: ${JSON.stringify(value)}`);
  }
  return value.trim();
}

function optionalString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

function requireSafeInteger(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new MontanaCersParseError(`Non-integer Montana CERS field ${field}: ${JSON.stringify(value)}`);
  }
  return value;
}

function optionalEpochMs(value: unknown, field: string): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  return requireSafeInteger(value, field);
}

function requireYesNo(value: unknown, field: string): "Y" | "N" {
  if (value === "Y" || value === "N") {
    return value;
  }
  throw new MontanaCersParseError(`Unexpected Montana CERS ${field} flag: ${JSON.stringify(value)}`);
}

function requireElectionSide(value: unknown, field: string): "Primary" | "General" {
  if (value === "Primary" || value === "General") {
    return value;
  }
  throw new MontanaCersParseError(`Unexpected Montana CERS ${field}: ${JSON.stringify(value)}`);
}

const PERIOD_DATE_PATTERN = /^\d{2}\/\d{2}\/\d{4}$/;

function requirePeriodDate(value: unknown, field: string): string {
  const text = requireString(value, field);
  if (!PERIOD_DATE_PATTERN.test(text)) {
    throw new MontanaCersParseError(`Unexpected Montana CERS date format in ${field}: ${text}`);
  }
  return text;
}

function parseJsonBody(body: string, label: string): unknown {
  if (body.trim() === "") {
    // An empty body is how CERS answers a listName that does not apply to
    // the entity (and how truncation looks) — never a legitimate empty list,
    // which is the JSON `[]`.
    throw new MontanaCersParseError(`Empty Montana CERS ${label} response body`);
  }
  try {
    return JSON.parse(body);
  } catch {
    // Never echo raw JSON-ish bodies into errors — report-detail payloads
    // carry donor names and street addresses. An HTML error page's head is
    // safe and is the useful diagnostic (Tomcat error titles).
    const head = body.trimStart().startsWith("<")
      ? `: ${body.slice(0, 120).replaceAll(/\s+/g, " ")}`
      : ` (${body.length} bytes withheld)`;
    throw new MontanaCersParseError(`Montana CERS ${label} response is not JSON${head}`);
  }
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new MontanaCersParseError(`Montana CERS ${label} is not an object`);
  }
  return value as Record<string, unknown>;
}

function requireDataTablesRows(body: string, label: string): Record<string, unknown>[] {
  const parsed = requireRecord(parseJsonBody(body, label), label);
  const rows = parsed["aaData"];
  if (!Array.isArray(rows)) {
    throw new MontanaCersParseError(`Montana CERS ${label} has no aaData array`);
  }
  // Both DataTables endpoints carry the total counts (verified live
  // 2026-08-27). A page smaller than the total means the iDisplayLength
  // cap silently truncated the result — fail closed, never a partial read.
  const total = parsed["iTotalDisplayRecords"] ?? parsed["iTotalRecords"];
  if (typeof total !== "number" || !Number.isSafeInteger(total)) {
    throw new MontanaCersParseError(`Montana CERS ${label} has no iTotalDisplayRecords/iTotalRecords count`);
  }
  if (rows.length !== total) {
    throw new MontanaCersParseError(
      `Montana CERS ${label} is truncated: ${rows.length} of ${total} rows (raise the display length)`
    );
  }
  return rows.map((row) => requireRecord(row, `${label} row`));
}

function parseExportLines(
  body: string,
  header: readonly string[],
  label: string
): string[][] {
  if (body.trim() === "") {
    throw new MontanaCersParseError(`Empty Montana CERS ${label} export body`);
  }
  if (/^\s*[<{]/.test(body)) {
    throw new MontanaCersParseError(`Montana CERS ${label} export is not pipe-delimited text`);
  }
  const lines = body.split(/\r?\n/).filter((line) => line.trim() !== "");
  const headerLine = lines[0] ?? "";
  if (headerLine.split("|").map((cell) => cell.trim()).join("|") !== header.join("|")) {
    throw new MontanaCersParseError(`Unexpected Montana CERS ${label} export header: ${headerLine.slice(0, 200)}`);
  }
  return lines.slice(1).map((line, index) => {
    const cells = line.split("|");
    if (cells.length !== header.length) {
      throw new MontanaCersParseError(
        `Montana CERS ${label} export row ${index + 1} has ${cells.length} columns, expected ${header.length}`
      );
    }
    return cells;
  });
}

function parseExportRow(cells: string[], label: string, rowIndex: number): MontanaCersExportRow {
  const context = `${label} row ${rowIndex + 1}`;
  const candidateId = Number(cells[0]!.trim());
  if (!Number.isSafeInteger(candidateId) || candidateId <= 0) {
    throw new MontanaCersParseError(`Bad candidate id in Montana CERS ${context}: ${cells[0]}`);
  }
  const amountSubtype = cells[16]!.trim();
  if (amountSubtype !== "Cash" && amountSubtype !== "In-Kind") {
    throw new MontanaCersParseError(`Unexpected Montana CERS amount subtype in ${context}: ${amountSubtype}`);
  }
  return {
    candidateId,
    candidateName: requireString(cells[1], `${context} candidate name`),
    reportingDateRange: requireString(cells[4], `${context} reporting date range`),
    entityName: requireString(cells[5], `${context} entity name`),
    occupation: optionalString(cells[8]),
    employer: optionalString(cells[9]),
    datePaid: requirePeriodDate(cells[10], `${context} date paid`),
    purpose: optionalString(cells[11]),
    description: optionalString(cells[12]),
    lineItem: requireString(cells[13], `${context} line item`),
    amountCents: parseMontanaCersCsvAmountCents(cells[14]!),
    electionType: requireElectionSide(cells[15]!.trim(), `${context} election type`),
    amountSubtype,
    officeTitle: requireString(cells[17], `${context} office title`),
  };
}

export function parseMontanaCersContributionExport(body: string): MontanaCersExportRow[] {
  return parseExportLines(body, MONTANA_CERS_CONTRIBUTION_EXPORT_HEADER, "contribution").map(
    (cells, index) => parseExportRow(cells, "contribution", index)
  );
}

export function parseMontanaCersExpenditureExport(body: string): MontanaCersExportRow[] {
  return parseExportLines(body, MONTANA_CERS_EXPENDITURE_EXPORT_HEADER, "expenditure").map(
    (cells, index) => parseExportRow(cells, "expenditure", index)
  );
}

export function parseMontanaCersReportInventory(body: string): MontanaCersReportInventoryRow[] {
  return requireDataTablesRows(body, "report inventory").map((row) => ({
    reportId: requireSafeInteger(row["reportId"], "reportId"),
    entitySubId: requireSafeInteger(row["entitySubId"], "entitySubId"),
    formTypeCode: requireString(row["formTypeCode"], "formTypeCode"),
    formTypeDescr: optionalString(row["formTypeDescr"]),
    fromDateStr: requirePeriodDate(row["fromDateStr"], "fromDateStr"),
    toDateStr: requirePeriodDate(row["toDateStr"], "toDateStr"),
    reportTypeDescr: optionalString(row["reportTypeDescr"]),
    statusCode: requireString(row["statusCode"], "statusCode"),
    statusDescr: requireString(row["statusDescr"], "statusDescr"),
    primCashBegCents: optionalJsonAmountToCents(row["primCashBeg"], "primCashBeg"),
    genCashBegCents: optionalJsonAmountToCents(row["genCashBeg"], "genCashBeg"),
    receivedDate: requireSafeInteger(row["receivedDate"], "receivedDate"),
    amendedDate: optionalEpochMs(row["amendedDate"], "amendedDate"),
  }));
}

export function parseMontanaCersFinanceRepDetailList(body: string): MontanaCersDetailRow[] {
  const parsed = parseJsonBody(body, "report detail list");
  if (!Array.isArray(parsed)) {
    throw new MontanaCersParseError("Montana CERS report detail list is not an array");
  }
  return parsed.map((value) => {
    const row = requireRecord(value, "report detail row");
    return {
      amountTypeDescr: requireElectionSide(row["amountTypeDescr"], "amountTypeDescr"),
      cashAmtCents: montanaCersJsonAmountToCents(row["cashAmt"], "cashAmt"),
      inKindAmtCents: montanaCersJsonAmountToCents(row["inKindAmt"], "inKindAmt"),
      totalAmtCents: montanaCersJsonAmountToCents(row["totalAmt"], "totalAmt"),
      debtAmtCents: montanaCersJsonAmountToCents(row["debtAmt"], "debtAmt"),
      entityName: optionalString(row["entityName"]),
      occupationDescr: optionalString(row["occupationDescr"]),
      employerDescr: optionalString(row["employerDescr"]),
      datePaid: optionalEpochMs(row["datePaid"], "datePaid"),
      lineItemCompositeDescr: optionalString(row["lineItemCompositeDescr"]),
      purposeDescr: optionalString(row["purposeDescr"]),
      // Explicit flags only: a missing classification flag is schema drift
      // that could reclassify electioneering money — fail closed.
      electioneeringInd: requireYesNo(row["electioneeringInd"], "electioneeringInd"),
      candidateContrInd: requireYesNo(row["candidateContrInd"], "candidateContrInd"),
    };
  });
}

export function parseMontanaCersCandidateSearchResults(body: string): MontanaCersCandidateSearchRow[] {
  return requireDataTablesRows(body, "candidate search results").map((row) => {
    const person = requireRecord(row["personDTO"] ?? {}, "candidate personDTO");
    const electionYearText = optionalString(row["electionYear"]);
    const electionYear = electionYearText === null ? null : Number(electionYearText);
    if (electionYear !== null && !Number.isSafeInteger(electionYear)) {
      throw new MontanaCersParseError(`Bad Montana CERS election year: ${JSON.stringify(row["electionYear"])}`);
    }
    return {
      candidateId: requireSafeInteger(row["candidateId"], "candidateId"),
      lastName: requireString(person["lastName"], "lastName"),
      firstName: optionalString(person["firstName"]),
      middleInitial: optionalString(person["middleInitial"]),
      electionYear,
      officeTitle: optionalString(row["officeTitle"]),
      officeCode: optionalString(row["officeCode"]),
      partyDescr: optionalString(row["partyDescr"]),
      candidateStatusDescr: optionalString(row["candidateStatusDescr"]),
      resCountyDescr: optionalString(row["resCountyDescr"]),
    };
  });
}

/**
 * Report-detail lists a candidate-report harvest fetches, and the side of
 * the cash-begin chain each feeds (plan "Term definitions", load-bearing):
 * cash inflows = contributions + cash loan proceeds + the misc-receipts
 * family; cash outflows = cash portions of the disbursement lists. debtLoan
 * tracks debt, not cash, and feeds neither side. `expendIndependent` is
 * deliberately absent — CERS answers it with an empty body on candidate
 * reports.
 */
export const MONTANA_CERS_CANDIDATE_DETAIL_LISTS = {
  inflow: ["individual", "committee", "candidate", "loan", "refunds", "fundraisers"],
  outflow: ["payment", "expendOther", "pettyCash"],
  neither: ["debtLoan"],
} as const;

export type MontanaCersCandidateDetailListName =
  | (typeof MONTANA_CERS_CANDIDATE_DETAIL_LISTS.inflow)[number]
  | (typeof MONTANA_CERS_CANDIDATE_DETAIL_LISTS.outflow)[number]
  | (typeof MONTANA_CERS_CANDIDATE_DETAIL_LISTS.neither)[number];

export const MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS: readonly MontanaCersCandidateDetailListName[] = [
  ...MONTANA_CERS_CANDIDATE_DETAIL_LISTS.inflow,
  ...MONTANA_CERS_CANDIDATE_DETAIL_LISTS.outflow,
  ...MONTANA_CERS_CANDIDATE_DETAIL_LISTS.neither,
];

export type MontanaCersReportDetailArtifact = {
  reportId: number;
  lists: Record<MontanaCersCandidateDetailListName, MontanaCersDetailRow[]>;
};

/**
 * Parses the combined report-detail artifact this module stores: one JSON
 * object bundling every fetched detail list for a report. Every known list
 * must be present (missing = truncated harvest, fail closed).
 */
export function parseMontanaCersReportDetailArtifact(body: string): MontanaCersReportDetailArtifact {
  const parsed = requireRecord(parseJsonBody(body, "report detail artifact"), "report detail artifact");
  const reportId = requireSafeInteger(parsed["reportId"], "reportId");
  const listsRecord = requireRecord(parsed["lists"], "report detail artifact lists");
  const lists = {} as Record<MontanaCersCandidateDetailListName, MontanaCersDetailRow[]>;
  for (const name of MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS) {
    const value = listsRecord[name];
    if (!Array.isArray(value)) {
      throw new MontanaCersParseError(`Montana CERS report detail artifact is missing list ${name}`);
    }
    lists[name] = parseMontanaCersFinanceRepDetailList(JSON.stringify(value));
  }
  const unknown = Object.keys(listsRecord).filter(
    (name) => !(MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS as readonly string[]).includes(name)
  );
  if (unknown.length > 0) {
    throw new MontanaCersParseError(`Montana CERS report detail artifact has unknown lists: ${unknown.join(", ")}`);
  }
  return { reportId, lists };
}
