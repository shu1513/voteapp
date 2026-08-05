import {
  normalizeOhioSosHeader,
  normalizeOhioSosText,
  normalizeOhioSosTextOrNull,
  parseOhioSosAmountCents,
  parseOhioSosCsvText,
  parseOhioSosDateIso,
} from "./ohioSosCsv.js";

// Form 31-U detail rows — the second stage of the two-stage independent
// expenditure model (ohio_plan.md decision 4). The annual bulk files carry
// 31-U rows with the spender and amount but no target candidate, office, or
// direction; those live only in each report's own detail view at
// `f?p=CFDISCLOSURE:48:::::P48_LISTTYPE,P48_REPORT_ID,P48_TYPE:simple,<REPORT_KEY>,31U`.
//
// The detail page's CSV-export link is a session-bound APEX widget URL with a
// checksum, so it is not a stable fetch target; the acquisition script
// scrapes the page's HTML table instead, which the spike verified reproduces
// the exported CSV column-for-column. Both shapes land here.

export const OHIO_SOS_31U_DETAIL_HEADER = [
  "Payee Name",
  "Payee  Non Individual",
  " Address",
  "City",
  "State",
  "Zip",
  "Report Type",
  "Amount",
  "Year",
  "Expend Date",
  "Event Date",
  "Purpose",
  "Committee Name",
  "Office",
  "Candidate Name /Ballot Issue",
  "Support/Opposed",
] as const;

export function ohioSos31uDetailUrl(reportKey: string): string {
  const trimmed = reportKey.trim();
  if (!/^\d+$/.test(trimmed)) {
    throw new Error(`Invalid Ohio SoS 31-U report key: ${reportKey}`);
  }
  return `https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:48:::::P48_LISTTYPE,P48_REPORT_ID,P48_TYPE:simple,${trimmed},31U`;
}

// Fail-closed per decision 3: only an explicit SUPPORT or OPPOSE becomes a
// directional row. Blank direction is real (8 of 43 rows in the 2026 cycle)
// and is excluded with diagnostics — never inferred.
export type OhioSos31uDirection = "support" | "oppose";

export function parseOhioSos31uDirection(raw: string | undefined): OhioSos31uDirection | null {
  const normalized = normalizeOhioSosText(raw).toUpperCase();
  if (normalized === "SUPPORT" || normalized === "SUPPORTED" || normalized === "SUPPORTING") {
    return "support";
  }
  if (normalized === "OPPOSE" || normalized === "OPPOSED" || normalized === "OPPOSING") {
    return "oppose";
  }
  return null;
}

export type OhioSos31uDetailRow = {
  // Carried in from the annual file, never re-derived from the detail export
  // — the detail has no MASTER_KEY and names the spender only by committee
  // name (decision 4).
  reportKey: string;
  spenderCommitteeName: string;
  payeeName: string | null;
  payeeNonIndividual: string | null;
  payeeAddress: string | null;
  payeeCity: string | null;
  payeeState: string | null;
  payeeZip: string | null;
  reportType: string | null;
  amountCents: number | null;
  year: number | null;
  expendDateIso: string | null;
  eventDateIso: string | null;
  purpose: string | null;
  office: string | null;
  // Mixes candidates with ballot issues and non-state races; the resolver
  // (PR 5) rejects anything that does not resolve to exactly one candidate.
  candidateNameOrBallotIssue: string | null;
  direction: OhioSos31uDirection | null;
  rawDirection: string | null;
};

const INDEX = Object.fromEntries(
  OHIO_SOS_31U_DETAIL_HEADER.map((name, index) => [normalizeOhioSosHeader(name), index])
) as Record<string, number>;

function detailRowFromFields(fields: readonly string[], reportKey: string): OhioSos31uDetailRow {
  const at = (header: string): string | undefined => fields[INDEX[normalizeOhioSosHeader(header)]!];
  const yearRaw = normalizeOhioSosText(at("Year"));
  const rawDirection = normalizeOhioSosTextOrNull(at("Support/Opposed"));
  return {
    reportKey,
    spenderCommitteeName: normalizeOhioSosText(at("Committee Name")),
    payeeName: normalizeOhioSosTextOrNull(at("Payee Name")),
    payeeNonIndividual: normalizeOhioSosTextOrNull(at("Payee  Non Individual")),
    payeeAddress: normalizeOhioSosTextOrNull(at(" Address")),
    payeeCity: normalizeOhioSosTextOrNull(at("City")),
    payeeState: normalizeOhioSosTextOrNull(at("State")),
    payeeZip: normalizeOhioSosTextOrNull(at("Zip")),
    reportType: normalizeOhioSosTextOrNull(at("Report Type")),
    amountCents: parseOhioSosAmountCents(at("Amount")),
    year: /^\d{4}$/.test(yearRaw) ? Number(yearRaw) : null,
    expendDateIso: parseOhioSosDateIso(at("Expend Date")),
    eventDateIso: parseOhioSosDateIso(at("Event Date")),
    purpose: normalizeOhioSosTextOrNull(at("Purpose")),
    office: normalizeOhioSosTextOrNull(at("Office")),
    candidateNameOrBallotIssue: normalizeOhioSosTextOrNull(at("Candidate Name /Ballot Issue")),
    direction: parseOhioSos31uDirection(at("Support/Opposed")),
    rawDirection,
  };
}

export function parseOhioSos31uDetailCsv(csv: string, input: { reportKey: string }): OhioSos31uDetailRow[] {
  const rows: OhioSos31uDetailRow[] = [];
  parseOhioSosCsvText(csv, {
    label: `31-U detail ${input.reportKey}`,
    expectedHeader: OHIO_SOS_31U_DETAIL_HEADER,
    visit: (fields) => rows.push(detailRowFromFields(fields, input.reportKey)),
  });
  return rows;
}

// APEX renders an empty cell as a bare "-" in the HTML table, where the CSV
// export leaves it blank. Only an exact "-" is a placeholder; a real value
// that merely contains a dash is untouched.
function scrapedCellValue(value: string): string {
  return normalizeOhioSosText(value) === "-" ? "" : value;
}

// The scraped page table arrives as header cells plus row cells, already
// extracted from the DOM by the acquisition script. Validating the header
// here means a portal layout change fails loudly instead of silently
// shifting every column.
export function parseOhioSos31uDetailTable(
  table: { headers: readonly string[]; rows: readonly (readonly string[])[] },
  input: { reportKey: string }
): OhioSos31uDetailRow[] {
  const expected = OHIO_SOS_31U_DETAIL_HEADER.map(normalizeOhioSosHeader);
  const actual = table.headers.map(normalizeOhioSosHeader);
  if (actual.length !== expected.length || actual.some((value, index) => value !== expected[index])) {
    throw new Error(
      `Ohio SoS 31-U detail ${input.reportKey} table header does not match the pinned schema: got ${JSON.stringify(table.headers)}`
    );
  }
  return table.rows
    .map((fields) => fields.map(scrapedCellValue))
    .filter((fields) => fields.some((value) => normalizeOhioSosText(value).length > 0))
    .map((fields, rowIndex) => {
      if (fields.length !== expected.length) {
        throw new Error(
          `Ohio SoS 31-U detail ${input.reportKey} table row ${rowIndex + 1} has ${fields.length} cells; expected ${expected.length}`
        );
      }
      return detailRowFromFields(fields, input.reportKey);
    });
}

export type OhioSos31uReconciliation = {
  reportKey: string;
  // Sum of the annual bulk file's 31-U rows for this report key.
  annualTotalCents: number;
  // Sum of the detail export's rows, regardless of direction.
  detailTotalCents: number;
  matches: boolean;
  differenceCents: number;
  // Directional rows that will actually be aggregated.
  supportCents: number;
  opposeCents: number;
  supportRowCount: number;
  opposeRowCount: number;
  // Rows excluded for a blank/unrecognized direction (decision 3).
  excludedDirectionRowCount: number;
  excludedDirectionCents: number;
  // Rows whose amount would not parse; excluded from every total above.
  unparseableAmountRowCount: number;
};

// Three-way reconciliation input (decision 4/7): the detail total must equal
// the annual bulk total for the report key. Annual and detail amounts are
// never summed together — the annual rows are discovery data only.
export function reconcileOhioSos31uReport(input: {
  reportKey: string;
  annualTotalCents: number;
  detailRows: readonly OhioSos31uDetailRow[];
  // Cents of slack allowed between the two totals; the spike found exact
  // agreement across all 13 report keys, so the default is zero.
  toleranceCents?: number;
}): OhioSos31uReconciliation {
  const tolerance = input.toleranceCents ?? 0;
  if (!Number.isInteger(tolerance) || tolerance < 0) {
    throw new Error(`Invalid Ohio SoS 31-U reconciliation tolerance: ${input.toleranceCents}`);
  }
  let detailTotalCents = 0;
  let supportCents = 0;
  let opposeCents = 0;
  let supportRowCount = 0;
  let opposeRowCount = 0;
  let excludedDirectionRowCount = 0;
  let excludedDirectionCents = 0;
  let unparseableAmountRowCount = 0;

  for (const row of input.detailRows) {
    if (row.amountCents === null) {
      unparseableAmountRowCount += 1;
      continue;
    }
    detailTotalCents += row.amountCents;
    if (row.direction === "support") {
      supportCents += row.amountCents;
      supportRowCount += 1;
    } else if (row.direction === "oppose") {
      opposeCents += row.amountCents;
      opposeRowCount += 1;
    } else {
      excludedDirectionRowCount += 1;
      excludedDirectionCents += row.amountCents;
    }
  }

  const differenceCents = detailTotalCents - input.annualTotalCents;
  return {
    reportKey: input.reportKey,
    annualTotalCents: input.annualTotalCents,
    detailTotalCents,
    matches: Math.abs(differenceCents) <= tolerance,
    differenceCents,
    supportCents,
    opposeCents,
    supportRowCount,
    opposeRowCount,
    excludedDirectionRowCount,
    excludedDirectionCents,
    unparseableAmountRowCount,
  };
}
