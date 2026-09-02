// eCRIS independent-expenditure search client.
//
// Verified live 2026-09-01:
// - GET the search page for its hidden fields, then POST them back with the
//   filters. Plain fetch with a Mozilla/5.0-prefixed User-Agent works.
// - The largest page size the form offers is 200 and the results render no
//   pager, so a filter that matches more than 200 rows silently truncates.
//   The walk below therefore searches by received-date window and splits any
//   window that comes back full until every window is under the cap. A
//   single day that is still full is a hard error, never silent truncation.
// - Only SEEC Form 40 documents (independent-expenditure filers, periodic
//   and 24-hour reports) are requested. SEEC Form 20 rows are party/PAC
//   statements whose candidate-tagged lines are organization expenditures or
//   contributions (money the candidate's own receipts already count), and
//   SEEC Form 8 rows are registration amendments with no amount.
// - Show History = No returns only the current version of each document.

import {
  CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL,
  parseConnecticutEcrisHiddenFields,
  parseConnecticutEcrisIndependentExpenditureSearchResults,
  type ConnecticutEcrisIndependentExpenditureRow,
} from "./connecticutEcrisIndependentExpenditureParsers.js";

export const CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FORM_TAG = "SEEC40";
export const CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_PAGE_SIZE = 200;
export const CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FETCH_TIMEOUT_MS = 120_000;
export const DEFAULT_CONNECTICUT_ECRIS_USER_AGENT =
  "Mozilla/5.0 (compatible; VoteApp election research; +https://electionssimplified.com)";

const FIELD_PREFIX = "ctl00$ContentPlaceHolder1$";
const FORM_FIELDS = {
  fileYear: `${FIELD_PREFIX}lstFILE_YEAR`,
  pageSize: `${FIELD_PREFIX}lstNoOfRecords`,
  showHistory: `${FIELD_PREFIX}rblShowHistory`,
  receivedStartDate: `${FIELD_PREFIX}ReceivedStartDate`,
  receivedEndDate: `${FIELD_PREFIX}ReceivedEndDate`,
  /** Checkbox index 4 = "SEEC Form 40" (indexes 0-4 = Forms 8, 20, 22, 26, 40). */
  form40: `${FIELD_PREFIX}chkFormName$4`,
  search: `${FIELD_PREFIX}btnSearch`,
} as const;

export type ConnecticutEcrisIndependentExpenditureSearchWindow = {
  /** Inclusive ISO dates. */
  startDate: string;
  endDate: string;
  rowCount: number;
};

export type ConnecticutEcrisIndependentExpenditureFetchResult = {
  year: number;
  sourceUrl: string;
  rows: ConnecticutEcrisIndependentExpenditureRow[];
  searchWindows: ConnecticutEcrisIndependentExpenditureSearchWindow[];
};

export type ConnecticutEcrisIndependentExpenditureFetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  userAgent?: string;
  /** Rows per page requested from the form; a window returning this many is split. */
  pageSize?: number;
};

function normalizeYear(year: number): number {
  if (!Number.isInteger(year) || year < 2008 || year > 2100) {
    throw new Error(`Invalid Connecticut eCRIS independent expenditure year: ${year}`);
  }
  return year;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function utcDate(iso: string): Date {
  return new Date(`${iso}T00:00:00.000Z`);
}

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function toFormDate(iso: string): string {
  const [year, month, day] = iso.split("-");
  return `${month}/${day}/${year}`;
}

function midpoint(startIso: string, endIso: string): string {
  const start = utcDate(startIso).getTime();
  const end = utcDate(endIso).getTime();
  const days = Math.round((end - start) / 86_400_000);
  return toIsoDate(new Date(start + Math.floor(days / 2) * 86_400_000));
}

function nextDay(iso: string): string {
  return toIsoDate(new Date(utcDate(iso).getTime() + 86_400_000));
}

function parseSetCookies(response: Response): string[] {
  const headers = response.headers as Headers & { getSetCookie?: () => string[] };
  const raw = headers.getSetCookie?.() ?? [];
  return raw.map((header) => header.split(";", 1)[0]!.trim()).filter((pair) => pair.includes("="));
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  options: ConnecticutEcrisIndependentExpenditureFetchOptions
): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await (options.fetchImpl ?? fetch)(url, { ...init, signal: controller.signal });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Connecticut eCRIS independent expenditure request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function readBody(response: Response, label: string): Promise<string> {
  if (!response.ok) {
    throw new Error(`Connecticut eCRIS independent expenditure ${label} answered ${response.status} ${response.statusText}`);
  }
  return await response.text();
}

async function searchWindow(input: {
  year: number;
  startDate: string;
  endDate: string;
  pageSize: number;
  options: ConnecticutEcrisIndependentExpenditureFetchOptions;
}): Promise<ConnecticutEcrisIndependentExpenditureRow[]> {
  const userAgent = input.options.userAgent ?? DEFAULT_CONNECTICUT_ECRIS_USER_AGENT;
  const url = CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL;

  // Every search starts from a fresh form so the echoed hidden fields always
  // belong to the page they came from (stale WebForms state 500s).
  const formResponse = await fetchWithTimeout(
    url,
    { method: "GET", headers: { "user-agent": userAgent, accept: "text/html" } },
    input.options
  );
  const formHtml = await readBody(formResponse, "search form");
  const hiddenFields = parseConnecticutEcrisHiddenFields(formHtml);
  if (!("__VIEWSTATE" in hiddenFields)) {
    throw new Error("Connecticut eCRIS independent expenditure search form has no __VIEWSTATE");
  }
  const cookies = parseSetCookies(formResponse);

  const body = new URLSearchParams({
    ...hiddenFields,
    [FORM_FIELDS.fileYear]: String(input.year),
    [FORM_FIELDS.pageSize]: String(input.pageSize),
    [FORM_FIELDS.showHistory]: "0",
    [FORM_FIELDS.receivedStartDate]: toFormDate(input.startDate),
    [FORM_FIELDS.receivedEndDate]: toFormDate(input.endDate),
    [FORM_FIELDS.form40]: "on",
    [FORM_FIELDS.search]: "Search",
  }).toString();
  const headers: Record<string, string> = {
    "user-agent": userAgent,
    accept: "text/html",
    "content-type": "application/x-www-form-urlencoded",
    referer: url,
  };
  if (cookies.length > 0) headers.cookie = cookies.join("; ");

  const resultResponse = await fetchWithTimeout(url, { method: "POST", headers, body }, input.options);
  const resultHtml = await readBody(resultResponse, "search");
  const parsed = parseConnecticutEcrisIndependentExpenditureSearchResults(resultHtml);
  return parsed.status === "rows" ? parsed.rows : [];
}

function validateRows(rows: readonly ConnecticutEcrisIndependentExpenditureRow[], year: number): void {
  for (const row of rows) {
    if (row.fileYear !== year) {
      throw new Error(`Connecticut eCRIS independent expenditure search for ${year} returned a ${row.fileYear} row`);
    }
    if (row.formTag !== CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FORM_TAG) {
      throw new Error(
        `Connecticut eCRIS independent expenditure search returned a ${row.formTag ?? "untagged"} document; ` +
          `only ${CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FORM_TAG} was requested`
      );
    }
  }
}

/**
 * Every current SEEC Form 40 independent-expenditure line for one file year.
 * Windows are disjoint day ranges, so rows are never fetched twice.
 */
export async function fetchConnecticutEcrisIndependentExpenditures(
  input: { year: number } & ConnecticutEcrisIndependentExpenditureFetchOptions
): Promise<ConnecticutEcrisIndependentExpenditureFetchResult> {
  const year = normalizeYear(input.year);
  const pageSize = input.pageSize ?? CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_PAGE_SIZE;
  if (!Number.isInteger(pageSize) || pageSize <= 0) {
    throw new Error(`Invalid Connecticut eCRIS independent expenditure page size: ${pageSize}`);
  }

  const rows: ConnecticutEcrisIndependentExpenditureRow[] = [];
  const searchWindows: ConnecticutEcrisIndependentExpenditureSearchWindow[] = [];
  const pending: Array<{ startDate: string; endDate: string }> = [{ startDate: `${year}-01-01`, endDate: `${year}-12-31` }];
  while (pending.length > 0) {
    const window = pending.shift()!;
    const windowRows = await searchWindow({ year, ...window, pageSize, options: input });
    if (windowRows.length >= pageSize) {
      if (window.startDate === window.endDate) {
        throw new Error(
          `Connecticut eCRIS independent expenditure search for ${window.startDate} returned ${windowRows.length} rows, ` +
            `the page cap; the day cannot be split further`
        );
      }
      const middle = midpoint(window.startDate, window.endDate);
      pending.unshift({ startDate: window.startDate, endDate: middle }, { startDate: nextDay(middle), endDate: window.endDate });
      continue;
    }
    validateRows(windowRows, year);
    rows.push(...windowRows);
    searchWindows.push({ ...window, rowCount: windowRows.length });
  }

  return {
    year,
    sourceUrl: CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_SEARCH_URL,
    rows,
    searchWindows,
  };
}
