// Parsers for the eCRIS "Search Independent Expenditures" page
// (seec.ct.gov/eCrisReporting/SearchingIndependentExpenditure.aspx).
//
// Verified live 2026-09-01:
// - The page is ASP.NET WebForms. A search is a POST of the form's hidden
//   fields plus the filter inputs; the results render in the same page as
//   `<table id="ctl00_ContentPlaceHolder1_gvSearchResult">` with 16 columns.
// - An empty search renders no table and the text
//   "No documents found matching selected criteria!".
// - The committee cell holds an <a> to the filed PDF followed by a
//   `<span id="..._lblDocName">(SEEC40)</span>` form tag. Search-term
//   highlighting can interleave empty `<span class=highlight>` tags through
//   the text, so cell text is read with every tag stripped.
// - Candidate and office cells are comma-separated lists with no space after
//   the comma ("Doug McCrory,Ayana Taylor"). The office list is a SET of the
//   offices named on the line, not positionally paired with the candidates.
// - Amounts render as "$8,544.45"; registration-amendment rows (SEEC8) render
//   a blank amount. Anything else is drift and fails closed.

export const CONNECTICUT_ECRIS_REPORTING_BASE_URL = "https://seec.ct.gov/eCrisReporting/";
export const CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL =
  "https://seec.ct.gov/eCrisReporting/SearchingIndependentExpenditure.aspx";
export const CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_RESULTS_TABLE_ID = "ctl00_ContentPlaceHolder1_gvSearchResult";
export const CONNECTICUT_ECRIS_NO_DOCUMENTS_MESSAGE = "No documents found matching selected criteria!";

export const CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_COLUMNS = [
  "Root Expenditure ID",
  "Committee/Entity Name",
  "Report Type",
  "Document Type",
  "Payee",
  "Received Date",
  "File Year",
  "Period Covered Start Date",
  "Period Covered End Date",
  "Amount",
  "Form Section",
  "Supporting Candidates",
  "Supporting Offices",
  "Opposing Candidates",
  "Opposing Offices",
  "Data Source",
] as const;

export type ConnecticutEcrisIndependentExpenditureRow = {
  rootExpenditureId: string;
  committeeName: string;
  /** Form tag rendered next to the committee, e.g. "SEEC40"; null when absent. */
  formTag: string | null;
  /** Absolute URL of the filed document the row came from. */
  documentUrl: string | null;
  reportType: string;
  documentType: string;
  payee: string;
  /** ISO date (YYYY-MM-DD); the transaction date, not the filing date. */
  receivedDate: string | null;
  fileYear: number;
  periodStartDate: string | null;
  periodEndDate: string | null;
  /** Integer cents; null when the cell is blank (registration amendments). */
  amountCents: number | null;
  formSection: string;
  supportingCandidates: string[];
  supportingOffices: string[];
  opposingCandidates: string[];
  opposingOffices: string[];
  dataSource: string;
};

export type ConnecticutEcrisIndependentExpenditureSearchResult =
  | { status: "rows"; rows: ConnecticutEcrisIndependentExpenditureRow[] }
  | { status: "no_documents" };

const HTML_ENTITY_MAP: Record<string, string> = {
  "&amp;": "&",
  "&lt;": "<",
  "&gt;": ">",
  "&quot;": '"',
  "&#39;": "'",
  "&nbsp;": " ",
};

export function decodeConnecticutEcrisHtmlText(value: string): string {
  return value.replace(/&(?:#x([0-9a-fA-F]+)|#(\d+)|amp|lt|gt|quot|nbsp);/g, (entity, hex?: string, decimal?: string) => {
    if (hex !== undefined) return String.fromCodePoint(Number.parseInt(hex, 16));
    if (decimal !== undefined) return String.fromCodePoint(Number.parseInt(decimal, 10));
    return HTML_ENTITY_MAP[entity] ?? entity;
  });
}

// Tags are removed, not replaced by spaces: highlighting interleaves empty
// spans between the letters of a name, and a space there would split it.
function cellText(html: string): string {
  return decodeConnecticutEcrisHtmlText(html.replace(/<br\s*\/?>/gi, " ").replace(/<[^>]+>/g, ""))
    .replace(/\s+/g, " ")
    .trim();
}

function readAttribute(tag: string, name: string): string | null {
  const match = new RegExp(`\\s${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s"'>]+))`, "i").exec(tag);
  if (!match) return null;
  return decodeConnecticutEcrisHtmlText(match[1] ?? match[2] ?? match[3] ?? "");
}

/** Every hidden input (name -> value) of the WebForms page; the next POST must echo them all. */
export function parseConnecticutEcrisHiddenFields(html: string): Record<string, string> {
  const fields: Record<string, string> = {};
  for (const tag of html.matchAll(/<input\b[^>]*>/gi)) {
    if (!/\stype\s*=\s*["']?hidden["']?/i.test(tag[0])) continue;
    const name = readAttribute(tag[0], "name");
    if (!name) continue;
    fields[name] = readAttribute(tag[0], "value") ?? "";
  }
  return fields;
}

/** "$8,544.45" -> 854445; "" -> null; "($1.00)" -> -100. Anything else throws. */
export function parseConnecticutEcrisMoneyCents(raw: string): number | null {
  const text = raw.trim();
  if (text.length === 0) return null;
  const match = /^(\()?\$?\s*(\d{1,3}(?:,\d{3})*|\d+)\.(\d{2})(\))?$/.exec(text);
  if (!match || Boolean(match[1]) !== Boolean(match[4])) {
    throw new Error(`Unparseable Connecticut eCRIS amount: ${JSON.stringify(raw)}`);
  }
  const cents = Number.parseInt(match[2]!.replace(/,/g, ""), 10) * 100 + Number.parseInt(match[3]!, 10);
  if (!Number.isSafeInteger(cents)) {
    throw new Error(`Connecticut eCRIS amount out of range: ${JSON.stringify(raw)}`);
  }
  return match[1] ? -cents : cents;
}

/** "07/17/2026" -> "2026-07-17"; "" -> null. Anything else throws. */
export function parseConnecticutEcrisDate(raw: string): string | null {
  const text = raw.trim();
  if (text.length === 0) return null;
  const match = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(text);
  if (!match) {
    throw new Error(`Unparseable Connecticut eCRIS date: ${JSON.stringify(raw)}`);
  }
  const [, month, day, year] = match;
  const iso = `${year}-${month}-${day}`;
  const check = new Date(`${iso}T00:00:00.000Z`);
  if (Number.isNaN(check.getTime()) || check.toISOString().slice(0, 10) !== iso) {
    throw new Error(`Invalid Connecticut eCRIS date: ${JSON.stringify(raw)}`);
  }
  return iso;
}

function parseFileYear(raw: string): number {
  const text = raw.trim();
  if (!/^\d{4}$/.test(text)) {
    throw new Error(`Unparseable Connecticut eCRIS file year: ${JSON.stringify(raw)}`);
  }
  return Number.parseInt(text, 10);
}

export function splitConnecticutEcrisNameList(raw: string): string[] {
  return raw
    .split(",")
    .map((part) => part.replace(/\s+/g, " ").trim())
    .filter((part) => part.length > 0);
}

function parseCommitteeCell(html: string): Pick<ConnecticutEcrisIndependentExpenditureRow, "committeeName" | "formTag" | "documentUrl"> {
  const anchor = /<a\b([^>]*)>([\s\S]*?)<\/a>/i.exec(html);
  const formTagMatch = /\((SEEC\d+)\)/i.exec(cellText(html));
  const formTag = formTagMatch ? formTagMatch[1]!.toUpperCase() : null;
  let committeeName: string;
  let documentUrl: string | null = null;
  if (anchor) {
    committeeName = cellText(anchor[2]!);
    const href = readAttribute(anchor[1]!, "href");
    if (href && !/^javascript:/i.test(href)) {
      documentUrl = new URL(href, CONNECTICUT_ECRIS_REPORTING_BASE_URL).toString();
    }
  } else {
    committeeName = cellText(html);
  }
  if (formTag) {
    committeeName = committeeName.replace(new RegExp(`\\s*\\(${formTag}\\)\\s*$`, "i"), "").trim();
  }
  return { committeeName, formTag, documentUrl };
}

function extractResultsTable(html: string): string | null {
  const open = new RegExp(`<table\\b[^>]*\\bid="${CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_RESULTS_TABLE_ID}"[^>]*>`, "i").exec(html);
  if (!open) return null;
  const start = open.index + open[0].length;
  const end = html.indexOf("</table>", start);
  if (end === -1) {
    throw new Error("Connecticut eCRIS independent expenditure results table is not closed");
  }
  return html.slice(start, end);
}

function splitCells(rowHtml: string): { header: boolean; cells: string[] } {
  const header = /<th\b/i.test(rowHtml);
  const pattern = header ? /<th\b[^>]*>([\s\S]*?)<\/th>/gi : /<td\b[^>]*>([\s\S]*?)<\/td>/gi;
  const cells: string[] = [];
  for (const match of rowHtml.matchAll(pattern)) {
    cells.push(match[1]!);
  }
  return { header, cells };
}

function requireColumnCount(cells: readonly string[], kind: string): void {
  if (cells.length !== CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_COLUMNS.length) {
    throw new Error(
      `Connecticut eCRIS independent expenditure ${kind} has ${cells.length} cells; ` +
        `expected ${CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_COLUMNS.length}`
    );
  }
}

function parseRow(cells: readonly string[]): ConnecticutEcrisIndependentExpenditureRow {
  requireColumnCount(cells, "row");
  const text = cells.map(cellText);
  return {
    rootExpenditureId: text[0]!,
    ...parseCommitteeCell(cells[1]!),
    reportType: text[2]!,
    documentType: text[3]!,
    payee: text[4]!,
    receivedDate: parseConnecticutEcrisDate(text[5]!),
    fileYear: parseFileYear(text[6]!),
    periodStartDate: parseConnecticutEcrisDate(text[7]!),
    periodEndDate: parseConnecticutEcrisDate(text[8]!),
    amountCents: parseConnecticutEcrisMoneyCents(text[9]!),
    formSection: text[10]!,
    supportingCandidates: splitConnecticutEcrisNameList(text[11]!),
    supportingOffices: splitConnecticutEcrisNameList(text[12]!),
    opposingCandidates: splitConnecticutEcrisNameList(text[13]!),
    opposingOffices: splitConnecticutEcrisNameList(text[14]!),
    dataSource: text[15]!,
  };
}

/**
 * Parse a search response. Fails closed: a page with neither the results
 * table nor the no-documents message, a header that drifted from the 16
 * known columns, or a malformed cell all throw rather than return partial
 * data.
 */
export function parseConnecticutEcrisIndependentExpenditureSearchResults(
  html: string
): ConnecticutEcrisIndependentExpenditureSearchResult {
  const table = extractResultsTable(html);
  if (table === null) {
    if (html.includes(CONNECTICUT_ECRIS_NO_DOCUMENTS_MESSAGE)) {
      return { status: "no_documents" };
    }
    throw new Error("Connecticut eCRIS independent expenditure search answered neither results nor a no-documents message");
  }

  let sawHeader = false;
  const rows: ConnecticutEcrisIndependentExpenditureRow[] = [];
  for (const match of table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const { header, cells } = splitCells(match[1]!);
    if (header) {
      requireColumnCount(cells, "header");
      const headers = cells.map(cellText);
      for (const [index, expected] of CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_COLUMNS.entries()) {
        if (headers[index] !== expected) {
          throw new Error(
            `Connecticut eCRIS independent expenditure column ${index + 1} is ${JSON.stringify(headers[index])}; expected ${JSON.stringify(expected)}`
          );
        }
      }
      sawHeader = true;
      continue;
    }
    if (cells.length === 0) continue;
    rows.push(parseRow(cells));
  }
  if (!sawHeader) {
    throw new Error("Connecticut eCRIS independent expenditure results table has no header row");
  }
  return { status: "rows", rows };
}
