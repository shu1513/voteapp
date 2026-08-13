// PR 3 acquisition spike for the Rhode Island finance module
// (rhode_island_plan.md, "PR sequence" step 3).
//
// NO migration, NO database, NO writes: this script only reads the public ERTS
// portal (ricampaignfinance.com), writes every fetched artifact to the
// gitignored scratch directory as evidence, and checks the plan's spike gates.
// The transport and URL builders are pinned here rather than in the pipeline
// module on purpose — PR 4 lifts them into `rhodeIslandErtsClient.ts` once the
// shapes below are proven (san diego precedent: the probe pins the agency
// config first).
//
// Gates (each hand-derived live on 2026-08-13; a FAIL means the portal changed
// and the finding must be re-verified by hand before any build work):
//   1. Transaction report is a stateless GET. `Reporting/TransactionReport.aspx`
//      serves McKee's Q2 2026 contribution report with no cookie, no viewstate
//      and no prior search, and its summary groupings are cent-exact against
//      the plan's reconciliation fixture.
//   2. CSV export round-trip. `lnkExport` -> `DownloadFile.aspx` ->
//      `hypFileDownload` yields the detail CSV with the pinned column list,
//      and EVERY summary grouping is accounted for: either the export
//      reproduces it cent-exact, or a typed search proves the portal holds no
//      itemized rows for that type. An unexplained absence fails the gate —
//      the portal renders no exported-row count, so this reconciliation is
//      the only silent-truncation control.
//   3. The summary groupings are NOT reproducible from the export. Q2 2026
//      carries `Other Receipt $113.95` in the summary while the itemized search
//      for that type confirms no rows exist — so official totals must come from
//      the summary/CF-2 side, never from summing the export (the georgia
//      cover-arithmetic lesson, decision 2).
//   4. Organization discovery works and yields the numeric Board key: the
//      WebForms org search on `Contributions.aspx` ends in a redirect whose
//      `OrgID` query parameter is the canonical `committee_id`.
//   5. The org filing list exposes amendment state (`Amended` Yes/No) plus a
//      `FilingAmendmentSelect.aspx` link per amended family, and every version
//      is a generated (text-layer) PDF under `/ExportDocs/`.
//   6. Amendment semantics (decision 4, the release-gating question): for >= 5
//      CONCLUSIVE amended CF-2 families — both version PDFs parsed and at
//      least one comparable receipt field changed between original and latest
//      (identical totals match both versions and prove nothing) — the
//      date-bounded transaction search reproduces the LATEST version's
//      values, and therefore differs from the original's on the changed
//      fields. The public transaction data is current-ledger state.
//   7. The `dgdCF8FilingList` index paginates by WebForms pager postbacks with
//      dates descending page over page, and can be traversed to the cycle
//      boundary (decision 3c / decision 5's diff source).

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

// --- Pinned portal surface --------------------------------------------------

export const ERTS_PUBLIC_BASE_URL = "https://www.ricampaignfinance.com/RIPublic/";

export const DEFAULT_ERTS_USER_AGENT = "VoteApp election research (https://electionssimplified.com)";

// Delay before every request after the first. The spike ran at 2 s and drew no
// blocks; the portal is a single-server ASP.NET app, so this is not negotiable
// (north carolina transport discipline).
export const ERTS_REQUEST_SPACING_MS = 2_000;

export const ERTS_REQUEST_TIMEOUT_MS = 120_000;

export const RHODE_ISLAND_FINANCE_PROBE_CACHE_DIR = "scratch/rhode-island-campaign-finance/erts";

// R.I. Gen. Laws § 17-25-3 cycle window used by the plan (decision 2).
const CYCLE_BEGIN = "01/01/2025";
const CYCLE_END = "12/31/2026";

// Reconciliation fixture: Daniel J. McKee, ERTS organization key 2235.
const MCKEE_ORG_ID = "2235";
const MCKEE_SEARCH_LAST_NAME = "McKee";
const MCKEE_ORGANIZATION_NAME = "DANIEL J MCKEE";

const Q2_2026 = { begin: "04/01/2026", end: "06/30/2026" } as const;

// Summary groupings of the Q2 2026 contribution report, hand-read from the
// portal on 2026-08-12 and again on 2026-08-13.
const EXPECTED_Q2_2026_CONTRIBUTION_SUMMARY: ReadonlyMap<string, number> = new Map([
  ["Individual", 24_126_429],
  ["PAC", 1_245_000],
  ["Interest Received", 511_677],
  ["In-Kind - Individual", 350_800],
  ["Other Receipt", 11_395],
]);

const EXPECTED_Q2_2026_EXPENDITURE_TOTAL_CENTS = 94_543_457;

// Cash receipts exclude in-kind; the CF-2 arithmetic in the plan is
// 1,355,115.78 + 258,945.01 - 945,434.57 = 668,626.22.
const EXPECTED_Q2_2026_CASH_RECEIPTS_CENTS = 25_894_501;

// Contribution-type codes on `lstContributionType` (Contributions.aspx, read
// live 2026-08-13). Pinned so decision 13's mapping table can be checked
// against the live vocabulary; `0` means "all types".
export const ERTS_CONTRIBUTION_TYPE_CODES: Readonly<Record<string, number>> = {
  Individual: 2,
  "Aggregate - Individual": 1,
  "Aggregate - PAC": 20,
  "Aggregate - Party": 19,
  PAC: 4,
  Party: 3,
  "Loan Proceeds": 5,
  "Loan Proceeds - PAC": 23,
  "Loan Proceeds - Party": 22,
  "In-Kind - Individual": 6,
  "In-Kind - Party": 7,
  "In-Kind - PAC": 8,
  "In Kind - Aggregate": 21,
  "Interest Received": 10,
  "Refund/Rebate": 11,
  "State Check Off": 12,
  "Party Building - Individual": 16,
  "Party Building - PAC": 18,
  "Party Building - Party": 24,
  "Other Receipt": 17,
  "Matching Public Funds": 9,
};

// Detail-export columns, in order, as served on 2026-08-13.
export const ERTS_CONTRIBUTION_EXPORT_COLUMNS: readonly string[] = [
  "ContributionID",
  "ContDesc",
  "IncompleteDesc",
  "OrganizationName",
  "ViewIncomplete",
  "ReceiptDate",
  "DepositDate",
  "Amount",
  "ContribExplanation",
  "MPFMatchAmount",
  "FirstName",
  "LastName",
  "FullName",
  "Address",
  "CityStZip",
  "EmployerName",
  "EmpAddress",
  "EmpCityStZip",
  "ReceiptDesc",
  "BeginDate",
  "EndDate",
  "TransType",
];

// CF-2 page-1 labels the spike pins for the totals mapping (decision 2). The
// value sits on the same text baseline as its label, to its right.
const CF2_RECEIPT_LABELS = [
  "1. Beginning Cash Balance",
  "2. Individuals",
  "3. Political Parties",
  "4. Political Action Committees",
  "7. Interest Received",
  "3. Total Cash",
  "5. Ending Cash Balance",
  "6. Report of In-Kind Contributions",
] as const;

// How many amended CF-2 families the amendment gate must cover (decision 4).
// Summary-groupings grid ids: the contribution report and the expenditure
// report render the same block under different ids.
const CONTRIBUTION_SUMMARY_GRID_ID = "dgrReport";
const EXPENDITURE_SUMMARY_GRID_ID = "dgrExpenditureSummary";

const AMENDMENT_FAMILY_TARGET = 5;

type Gate = { name: string; pass: boolean; detail: string };

// --- Transport --------------------------------------------------------------

type ErtsResponse = {
  status: number;
  finalUrl: string;
  contentType: string;
  body: Uint8Array;
};

type ErtsTransport = {
  fetch: (url: string, body?: URLSearchParams) => Promise<ErtsResponse>;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * One request in flight, fixed spacing before every request after the first,
 * bounded retries on transient failures only. A cookie jar is kept because the
 * organization search is session-scoped (the transaction report itself is not).
 */
function createErtsTransport(options: { log?: (message: string) => void } = {}): ErtsTransport {
  const cookies = new Map<string, string>();
  let queue: Promise<unknown> = Promise.resolve();
  let anyRequestStarted = false;

  async function once(url: string, body?: URLSearchParams): Promise<ErtsResponse> {
    const cookieHeader = [...cookies].map(([name, value]) => `${name}=${value}`).join("; ");
    const response = await fetch(url, {
      method: body ? "POST" : "GET",
      headers: {
        "User-Agent": DEFAULT_ERTS_USER_AGENT,
        Accept: "*/*",
        ...(cookieHeader ? { Cookie: cookieHeader } : {}),
        ...(body ? { "Content-Type": "application/x-www-form-urlencoded" } : {}),
      },
      body,
      redirect: "follow",
      signal: AbortSignal.timeout(ERTS_REQUEST_TIMEOUT_MS),
    });
    for (const setCookie of response.headers.getSetCookie?.() ?? []) {
      const pair = setCookie.split(";", 1)[0];
      const separator = pair.indexOf("=");
      if (separator > 0) cookies.set(pair.slice(0, separator).trim(), pair.slice(separator + 1).trim());
    }
    return {
      status: response.status,
      finalUrl: response.url,
      contentType: response.headers.get("content-type") ?? "",
      body: new Uint8Array(await response.arrayBuffer()),
    };
  }

  return {
    fetch: (url, body) => {
      const run = queue.then(async () => {
        let lastError: Error | null = null;
        for (let attempt = 1; attempt <= 3; attempt += 1) {
          if (anyRequestStarted) await sleep(attempt === 1 ? ERTS_REQUEST_SPACING_MS : attempt * 5_000);
          anyRequestStarted = true;
          try {
            const response = await once(url, body);
            if (response.status === 200) return response;
            const failure = new Error(`ERTS request failed with HTTP ${response.status}: ${url}`);
            // A 404 will not become a 200 by asking again.
            if (response.status !== 429 && response.status < 500) throw failure;
            lastError = failure;
          } catch (error) {
            if ((error as Error).message.startsWith("ERTS request failed with HTTP ")) throw error;
            lastError = error as Error;
          }
          options.log?.(`ERTS attempt ${attempt}/3 failed for ${url}: ${lastError.message}`);
        }
        throw new Error(`ERTS request failed after 3 attempts: ${url} — ${lastError?.message}`);
      });
      queue = run.catch(() => {});
      return run;
    },
  };
}

const decoder = new TextDecoder("utf-8");

async function saveArtifact(name: string, body: Uint8Array): Promise<void> {
  const target = path.join(RHODE_ISLAND_FINANCE_PROBE_CACHE_DIR, name);
  await mkdir(path.dirname(target), { recursive: true });
  await writeFile(target, body);
}

// --- WebForms helpers -------------------------------------------------------

function decodeHtml(value: string): string {
  return value
    .replace(/&nbsp;/g, " ")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&#x27;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function stripTags(value: string): string {
  return decodeHtml(value.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

export function ertsHiddenFields(html: string): Record<string, string> {
  const fields: Record<string, string> = {};
  for (const match of html.matchAll(/<input\b[^>]*type=["']hidden["'][^>]*>/gi)) {
    const name = /\bname=["']([^"']+)["']/i.exec(match[0])?.[1];
    if (!name) continue;
    fields[decodeHtml(name)] = decodeHtml(/\bvalue=["']([^"']*)["']/i.exec(match[0])?.[1] ?? "");
  }
  return fields;
}

/**
 * ASP.NET rejects a postback that drops a `<select>` the page rendered, so the
 * selected (or first) option of every dropdown rides along.
 */
export function ertsSelectDefaults(html: string): Record<string, string> {
  const fields: Record<string, string> = {};
  for (const select of html.matchAll(/<select\b[^>]*name=["']([^"']+)["'][^>]*>([\s\S]*?)<\/select>/gi)) {
    let chosen: string | null = null;
    let first: string | null = null;
    for (const option of select[2].matchAll(/<option\b([^>]*)>/gi)) {
      const value = decodeHtml(/\bvalue=["']([^"']*)["']/i.exec(option[1])?.[1] ?? "");
      if (first === null) first = value;
      if (/\bselected\b/i.test(option[1])) chosen = value;
    }
    fields[decodeHtml(select[1])] = chosen ?? first ?? "";
  }
  return fields;
}

function postBody(html: string, overrides: Record<string, string>): URLSearchParams {
  return new URLSearchParams({ ...ertsSelectDefaults(html), ...ertsHiddenFields(html), ...overrides });
}

/**
 * Fail closed (plan decision 3): the portal answers a rejected search with a
 * 200 redirect back to the search page, so a page that does not carry its
 * expected marker is an error, never an empty result.
 */
function requireErtsPage(html: string, marker: string, context: string): string {
  if (!html.includes(marker)) {
    const message = /No [A-Za-z]+ were found for the Search criteria you entered/.exec(stripTags(html))?.[0];
    throw new Error(`${context}: response did not contain ${JSON.stringify(marker)}${message ? ` — ${message}` : ""}`);
  }
  return html;
}

// --- Report URLs ------------------------------------------------------------

function requireOrgId(orgId: string): string {
  if (!/^\d+$/.test(orgId)) throw new Error(`Invalid ERTS organization key: ${JSON.stringify(orgId)}`);
  return orgId;
}

function requireUsDate(value: string, label: string): string {
  if (!/^\d{2}\/\d{2}\/\d{4}$/.test(value)) throw new Error(`Invalid ${label}: ${JSON.stringify(value)}`);
  return value;
}

/**
 * The contribution report URL exactly as the portal's own search redirect
 * builds it. Verified stateless on 2026-08-13: served identically with no
 * cookie and no prior search.
 */
export function ertsContributionReportUrl(input: {
  orgId: string;
  begin: string;
  end: string;
  contributionTypeCode?: number;
}): string {
  return (
    `${ERTS_PUBLIC_BASE_URL}Reporting/TransactionReport.aspx?OrgID=${requireOrgId(input.orgId)}` +
    `&BeginDate=${requireUsDate(input.begin, "BeginDate")}&EndDate=${requireUsDate(input.end, "EndDate")}` +
    `&LastName=&FirstName=&ContType=${input.contributionTypeCode ?? 0}&State=&City=&ZIPCode=&EmployerName=` +
    "&Amount=0&ReportType=Contrib&CFStatus=F&MPFStatus=A&Level=S&SumBy=Type" +
    "&Sort1=ReceiptDate&Direct1=asc&Sort2=None&Direct2=asc&Sort3=None&Direct3=asc" +
    "&Site=Public&Incomplete=A&ContSource=CF"
  );
}

/** Expenditures live on their own report page with their own parameter set. */
export function ertsExpenditureReportUrl(input: { orgId: string; begin: string; end: string }): string {
  return (
    `${ERTS_PUBLIC_BASE_URL}Reporting/ExpenditureReport.aspx?OrgID=${requireOrgId(input.orgId)}` +
    `&BeginDate=${requireUsDate(input.begin, "BeginDate")}&EndDate=${requireUsDate(input.end, "EndDate")}` +
    "&LastName=&FirstName=&ContType=0&State=&City=&ZIPCode=&Amount=0&ReportType=Expend" +
    "&CFStatus=F&MPFStatus=F&Level=S&SumBy=Type&Sort1=None&Direct1=asc&Sort2=None&Direct2=asc" +
    "&Sort3=None&Direct3=asc&Site=Public&Incomplete=A&ContSource=CF"
  );
}

// --- Parsing ----------------------------------------------------------------

export function parseMoneyToCents(value: string): number | null {
  const trimmed = value.replace(/[$\s]/g, "");
  if (trimmed === "") return null;
  const negative = /^\(.*\)$/.test(trimmed);
  const digits = trimmed.replace(/[()]/g, "").replace(/,/g, "");
  // The rendered pages print two decimals; the detail export prints four
  // ("250.0000"). Sub-cent precision has never been observed and would break
  // the integer-cent contract, so it is rejected rather than rounded away.
  const parts = /^-?(\d+)(?:\.(\d{1,4}))?$/.exec(digits);
  if (!parts) return null;
  const fraction = (parts[2] ?? "").padEnd(4, "0");
  if (fraction.slice(2) !== "00") return null;
  const cents = Number(parts[1]) * 100 + Number(fraction.slice(0, 2));
  const signed = digits.startsWith("-") ? -cents : cents;
  return negative ? -signed : signed;
}

function gridRows(html: string, gridId: string): string[][] {
  const table = new RegExp(`<table[^>]*id="${gridId}"[\\s\\S]*?</table>`, "i").exec(html)?.[0];
  if (!table) return [];
  const rows: string[][] = [];
  for (const row of table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...row[1].matchAll(/<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((cell) => stripTags(cell[1]));
    if (cells.length > 0) rows.push(cells);
  }
  return rows;
}

function gridRowHtml(html: string, gridId: string): string[] {
  const table = new RegExp(`<table[^>]*id="${gridId}"[\\s\\S]*?</table>`, "i").exec(html)?.[0];
  if (!table) return [];
  return [...table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)].map((row) => row[1]);
}

/**
 * "Summary Groupings" block above a transaction grid: the portal's own
 * per-type totals for the searched window.
 */
export function parseSummaryGroupings(html: string, gridId: string): Map<string, number> {
  const totals = new Map<string, number>();
  for (const cells of gridRows(html, gridId)) {
    if (cells.length < 2) continue;
    const cents = parseMoneyToCents(cells[cells.length - 1]);
    const label = cells[0];
    if (cents === null || label === "" || /^total$/i.test(label)) continue;
    totals.set(label, cents);
  }
  return totals;
}

/**
 * Classify a contribution search response. The portal never 404s: a search
 * with no rows answers 200 with a redirect back to the search page carrying
 * "No Contributions were found", and anything else (a Cloudflare challenge,
 * an error page) must read as unreadable — never as "no rows".
 */
export function classifyContributionSearchResult(html: string): "rows" | "no_rows" | "unreadable" {
  if (/<table[^>]*id="dgrContribution"/i.test(html)) return "rows";
  if (/No Contributions were found for the Search criteria you entered/i.test(stripTags(html))) return "no_rows";
  return "unreadable";
}

/** Minimal RFC-4180 reader — the export quotes any field containing a comma. */
export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (quoted) {
      if (char === '"') {
        if (text[index + 1] === '"') {
          field += '"';
          index += 1;
        } else quoted = false;
      } else field += char;
      continue;
    }
    if (char === '"') quoted = true;
    else if (char === ",") {
      row.push(field);
      field = "";
    } else if (char === "\n" || char === "\r") {
      if (char === "\r" && text[index + 1] === "\n") index += 1;
      row.push(field);
      field = "";
      if (row.some((cell) => cell !== "")) rows.push(row);
      row = [];
    } else field += char;
  }
  row.push(field);
  if (row.some((cell) => cell !== "")) rows.push(row);
  return rows;
}

// --- CSV export round-trip --------------------------------------------------

/**
 * Three hops, all of them load-bearing (verified 2026-08-13):
 *   1. `lnkExport` postback on the report page generates a temp file and
 *      answers with a script carrying a `DownloadFile.aspx` URL;
 *   2. that page renders a `hypFileDownload` postback link;
 *   3. the postback answers with the CSV bytes.
 * The `path=` parameter is a server-side share path — it is echoed verbatim
 * from the portal's own script and never constructed here.
 */
async function exportTransactionCsv(
  transport: ErtsTransport,
  input: { reportUrl: string; reportHtml: string }
): Promise<string> {
  const exportResponse = await transport.fetch(
    input.reportUrl,
    postBody(input.reportHtml, { __EVENTTARGET: "lnkExport", __EVENTARGUMENT: "" })
  );
  const exportHtml = decoder.decode(exportResponse.body);
  const downloadUrl = /txtPage\s*=\s*'([^']+)'/.exec(exportHtml)?.[1];
  if (!downloadUrl) throw new Error("ERTS export postback did not return a DownloadFile URL");
  const secureDownloadUrl = downloadUrl.replace(/^http:/, "https:");

  const downloadResponse = await transport.fetch(secureDownloadUrl);
  const downloadHtml = requireErtsPage(
    decoder.decode(downloadResponse.body),
    "hypFileDownload",
    "ERTS download page"
  );

  const fileResponse = await transport.fetch(
    secureDownloadUrl,
    postBody(downloadHtml, { __EVENTTARGET: "hypFileDownload", __EVENTARGUMENT: "" })
  );
  const csv = decoder.decode(fileResponse.body);
  if (/^\s*</.test(csv)) throw new Error("ERTS file download returned HTML instead of CSV bytes");
  return csv;
}

// --- Organization discovery (session-scoped WebForms) -----------------------

export type ErtsOrganizationMatch = { orgId: string; organizationName: string };

/**
 * The only way to turn a name into the numeric Board key: run the portal's own
 * organization search, select the row, run a dated search, and read `OrgID`
 * off the redirect. Five requests per organization — acceptable for the
 * per-organization crawl the plan already assumes (decision 3).
 */
async function discoverOrganization(
  transport: ErtsTransport,
  input: { lastName: string; organizationName: string; begin: string; end: string }
): Promise<{ match: ErtsOrganizationMatch; candidates: string[] }> {
  const searchUrl = `${ERTS_PUBLIC_BASE_URL}Contributions.aspx`;
  const entry = decoder.decode((await transport.fetch(searchUrl)).body);

  const panelHtml = requireErtsPage(
    decoder.decode((await transport.fetch(searchUrl, postBody(entry, { lnkSearchOrg: "New Organization Search" }))).body),
    "txtOrgLastName",
    "ERTS organization search panel"
  );

  const resultsHtml = requireErtsPage(
    decoder.decode(
      (
        await transport.fetch(
          searchUrl,
          postBody(panelHtml, { txtOrgLastName: input.lastName, lnkSubSearchOrg: "Search" })
        )
      ).body
    ),
    "dgdOrgSearchResults",
    "ERTS organization search results"
  );

  const rowsHtml = gridRowHtml(resultsHtml, "dgdOrgSearchResults");
  const candidates: string[] = [];
  const exactMatches: string[] = [];
  for (const rowHtml of rowsHtml) {
    const target = /__doPostBack\('(dgdOrgSearchResults\$[^']+)'/.exec(rowHtml)?.[1];
    if (!target) continue;
    const name = stripTags(/<td\b[^>]*>([\s\S]*?)<\/td>/i.exec(rowHtml)?.[1] ?? "");
    candidates.push(name);
    if (name.toUpperCase() === input.organizationName.toUpperCase()) exactMatches.push(target);
  }
  if (exactMatches.length === 0) {
    throw new Error(
      `ERTS organization search for ${JSON.stringify(input.lastName)} did not offer ` +
        `${JSON.stringify(input.organizationName)} (offered: ${candidates.join(", ") || "nothing"})`
    );
  }
  // Two registrations can lawfully share a name (old vs new committee,
  // terminated vs active). Row order carries no identity evidence, so an
  // ambiguous name fails here; the production resolver disambiguates with
  // real evidence (cycle, office, status), never by position.
  if (exactMatches.length > 1) {
    throw new Error(
      `ERTS organization search offered ${exactMatches.length} organizations named ` +
        `${JSON.stringify(input.organizationName)} — ambiguous, refusing to pick by row order`
    );
  }
  const selectionTarget = exactMatches[0];

  const selectedHtml = decoder.decode(
    (await transport.fetch(searchUrl, postBody(resultsHtml, { __EVENTTARGET: selectionTarget, __EVENTARGUMENT: "" })))
      .body
  );

  const searchResponse = await transport.fetch(
    searchUrl,
    postBody(selectedHtml, { txtDateFrom: input.begin, txtDateTo: input.end, btnSearch: "Search" })
  );
  const orgId = new URL(searchResponse.finalUrl).searchParams.get("OrgID");
  if (!orgId || !/^\d+$/.test(orgId)) {
    throw new Error(`ERTS search redirect carried no numeric OrgID: ${searchResponse.finalUrl}`);
  }
  return { match: { orgId, organizationName: input.organizationName }, candidates };
}

// --- Filing list + amendments -----------------------------------------------

export type ErtsFilingRow = {
  reportType: string;
  periodBegin: string;
  periodEnd: string;
  filedAt: string;
  amended: boolean;
  filingId: string | null;
};

/**
 * Read the org's filing grid. Reached in the same session as the org selection
 * (the portal carries the selected organization across its tabs).
 */
async function fetchOrganizationFilings(transport: ErtsTransport): Promise<{ html: string; rows: ErtsFilingRow[] }> {
  const html = requireErtsPage(
    decoder.decode((await transport.fetch(`${ERTS_PUBLIC_BASE_URL}Filings.aspx`)).body),
    "grdSearchResults",
    "ERTS organization filing list"
  );
  const rows: ErtsFilingRow[] = [];
  for (const rowHtml of gridRowHtml(html, "grdSearchResults")) {
    const cells = [...rowHtml.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripTags(cell[1]));
    if (cells.length < 7 || cells[5] === "") continue;
    rows.push({
      reportType: cells[0],
      periodBegin: cells[1],
      periodEnd: cells[2],
      filedAt: cells[5],
      amended: /^yes$/i.test(cells[6]),
      filingId: /FilingID=(\d+)/.exec(decodeHtml(rowHtml))?.[1] ?? null,
    });
  }
  return { html, rows };
}

export type ErtsFilingVersion = { amendmentLabel: string; filedAt: string; pdfUrl: string };

async function fetchFilingVersions(transport: ErtsTransport, filingId: string): Promise<ErtsFilingVersion[]> {
  const url =
    "https://secure.ricampaignfinance.com/RhodeIslandCF/Candidate/FilingAmendmentSelect.aspx" +
    `?X=T&FilingID=${filingId}&FormName=RICF2`;
  const html = requireErtsPage(
    decoder.decode((await transport.fetch(url)).body),
    "grdAmendments",
    `ERTS amendment list for filing ${filingId}`
  );
  const versions: ErtsFilingVersion[] = [];
  for (const rowHtml of gridRowHtml(html, "grdAmendments")) {
    const pdfUrl = /href="([^"]*\/ExportDocs\/[^"]+\.pdf)"/i.exec(decodeHtml(rowHtml))?.[1];
    if (!pdfUrl) continue;
    const cells = [...rowHtml.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripTags(cell[1]));
    versions.push({ amendmentLabel: cells[0] ?? "", filedAt: cells[2] ?? "", pdfUrl });
  }
  return versions;
}

// --- CF-2 summary page ------------------------------------------------------

type PdfTextItem = { text: string; x: number; y: number };

async function cf2PageItems(pdf: Uint8Array): Promise<PdfTextItem[]> {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const document = await getDocument({ data: pdf }).promise;
  try {
    const page = await document.getPage(1);
    const content = await page.getTextContent();
    return content.items
      .map((item) => {
        const typed = item as { str?: string; transform?: number[] };
        return {
          text: (typed.str ?? "").trim(),
          x: Math.round(typed.transform?.[4] ?? 0),
          y: Math.round(typed.transform?.[5] ?? 0),
        };
      })
      .filter((item) => item.text !== "");
  } finally {
    await document.destroy();
  }
}

/**
 * CF-2 page 1 is a fixed form: each amount sits on its label's baseline, to the
 * right of it, and the nearest such amount is the label's value. Pinning the
 * mapping this way is what lets PR 6 read official totals instead of summing
 * transactions (decision 2).
 */
export function cf2SummaryValues(items: readonly PdfTextItem[], labels: readonly string[]): Map<string, number> {
  const values = new Map<string, number>();
  for (const label of labels) {
    const anchor = items.find((item) => item.text === label);
    if (!anchor) continue;
    let best: { x: number; cents: number } | null = null;
    for (const item of items) {
      if (Math.abs(item.y - anchor.y) > 3 || item.x <= anchor.x) continue;
      const cents = parseMoneyToCents(item.text);
      if (cents === null) continue;
      if (!best || item.x < best.x) best = { x: item.x, cents };
    }
    if (best) values.set(label, best.cents);
  }
  return values;
}

// --- CF-8 "Other Filings" index ---------------------------------------------

export type Cf8IndexRow = { filedDate: string; filingType: string; organizationName: string; scannedUrl: string | null };

export function parseCf8IndexPage(html: string): Cf8IndexRow[] {
  const rows: Cf8IndexRow[] = [];
  for (const rowHtml of gridRowHtml(html, "dgdCF8FilingList")) {
    const cells = [...rowHtml.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripTags(cell[1]));
    if (cells.length < 4 || !/^[A-Z][a-z]{2} \d{1,2} \d{4}$/.test(cells[0])) continue;
    const href = /href="([^"]*ReportsScanned\/[^"]+)"/i.exec(decodeHtml(rowHtml))?.[1] ?? null;
    rows.push({
      filedDate: cells[0],
      filingType: cells[2],
      organizationName: cells[3],
      scannedUrl: href ? new URL(href, `${ERTS_PUBLIC_BASE_URL}Homepage.aspx`).toString() : null,
    });
  }
  return rows;
}

export type Cf8Pager = { currentPage: number | null; links: { label: string; target: string }[] };

/**
 * The grid's pager row: the current page renders as a bare `<span>`, every
 * other page as a postback link labelled with its page number, and "..." jumps
 * to the next window of ten.
 */
export function parseCf8Pager(html: string): Cf8Pager {
  const pagerCell = [...gridRowHtml(html, "dgdCF8FilingList")]
    .reverse()
    .find((rowHtml) => /__doPostBack\('dgdCF8FilingList\$/.test(rowHtml));
  if (!pagerCell) return { currentPage: null, links: [] };
  const currentLabel = /<span>\s*(\d+)\s*<\/span>/i.exec(pagerCell)?.[1];
  const links: { label: string; target: string }[] = [];
  for (const anchor of pagerCell.matchAll(
    /<a\b[^>]*__doPostBack\('(dgdCF8FilingList\$[^']+)'[^>]*>([\s\S]*?)<\/a>/gi
  )) {
    links.push({ label: stripTags(anchor[2]), target: anchor[1] });
  }
  return { currentPage: currentLabel ? Number(currentLabel) : null, links };
}

function parseFiledDate(value: string): number {
  const parsed = Date.parse(`${value} 00:00:00Z`);
  return Number.isNaN(parsed) ? Number.NaN : parsed;
}

/**
 * Walk the pager (decision 3c): follow the numbered postbacks in order and stop
 * once an entire page predates the cycle start. Dates must descend page over
 * page or the traversal is not trustworthy.
 */
async function traverseCf8Index(
  transport: ErtsTransport,
  cycleStart: number
): Promise<{ pages: number; rows: Cf8IndexRow[]; descending: boolean; reachedBoundary: boolean }> {
  const url = `${ERTS_PUBLIC_BASE_URL}Homepage.aspx`;
  let html = decoder.decode((await transport.fetch(url)).body);
  const rows: Cf8IndexRow[] = [];
  let pages = 0;
  let descending = true;
  let reachedBoundary = false;
  let previousOldest = Number.POSITIVE_INFINITY;
  const visitedPages = new Set<number>();

  while (pages < 25) {
    const pager = parseCf8Pager(html);
    // The pager's control ids are positional, not page numbers, so following
    // them blindly walks backwards into an already-read page. Page identity
    // comes from the rendered number instead.
    if (pager.currentPage !== null && visitedPages.has(pager.currentPage)) break;
    if (pager.currentPage !== null) visitedPages.add(pager.currentPage);

    const pageRows = parseCf8IndexPage(html);
    pages += 1;
    rows.push(...pageRows);
    const dates = pageRows.map((row) => parseFiledDate(row.filedDate)).filter((value) => !Number.isNaN(value));
    const oldest = dates.length > 0 ? Math.min(...dates) : Number.NaN;
    const newest = dates.length > 0 ? Math.max(...dates) : Number.NaN;
    if (!Number.isNaN(newest) && newest > previousOldest) descending = false;
    if (!Number.isNaN(oldest)) previousOldest = oldest;
    if (!Number.isNaN(newest) && newest < cycleStart) {
      reachedBoundary = true;
      break;
    }

    // Advance by label: the next page number, or the "..." window jump when
    // the number is past the end of the rendered window.
    const wanted = pager.currentPage === null ? null : String(pager.currentPage + 1);
    const nextTarget =
      (wanted === null ? undefined : pager.links.find((link) => link.label === wanted)?.target) ??
      pager.links.find((link) => link.label === "...")?.target;
    if (!nextTarget) break;
    html = decoder.decode(
      (await transport.fetch(url, postBody(html, { __EVENTTARGET: nextTarget, __EVENTARGUMENT: "" }))).body
    );
  }
  return { pages, rows, descending, reachedBoundary };
}

// --- Gates ------------------------------------------------------------------

function formatCents(cents: number): string {
  return `$${(cents / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function compareTotals(actual: ReadonlyMap<string, number>, expected: ReadonlyMap<string, number>): string[] {
  const differences: string[] = [];
  for (const [label, cents] of expected) {
    const found = actual.get(label);
    if (found !== cents) differences.push(`${label}: expected ${formatCents(cents)}, got ${found === undefined ? "nothing" : formatCents(found)}`);
  }
  for (const label of actual.keys()) {
    if (!expected.has(label)) differences.push(`${label}: unexpected summary grouping ${formatCents(actual.get(label) ?? 0)}`);
  }
  return differences;
}

async function main(): Promise<void> {
  for (const arg of process.argv.slice(2)) {
    throw new Error(`Unknown Rhode Island finance probe flag: ${arg}`);
  }

  const transport = createErtsTransport({ log: (message) => console.log(`  ${message}`) });
  const gates: Gate[] = [];

  // --- Gate 1: stateless contribution report + pinned summary groupings. ---
  const contributionUrl = ertsContributionReportUrl({ orgId: MCKEE_ORG_ID, ...Q2_2026 });
  const contributionHtml = requireErtsPage(
    decoder.decode((await transport.fetch(contributionUrl)).body),
    "dgrContribution",
    "ERTS contribution report"
  );
  await saveArtifact("mckee-q2-2026-contributions.html", new TextEncoder().encode(contributionHtml));
  const summary = parseSummaryGroupings(contributionHtml, CONTRIBUTION_SUMMARY_GRID_ID);
  const summaryDifferences = compareTotals(summary, EXPECTED_Q2_2026_CONTRIBUTION_SUMMARY);

  const expenditureHtml = requireErtsPage(
    decoder.decode((await transport.fetch(ertsExpenditureReportUrl({ orgId: MCKEE_ORG_ID, ...Q2_2026 }))).body),
    "dgrExpenditure",
    "ERTS expenditure report"
  );
  await saveArtifact("mckee-q2-2026-expenditures.html", new TextEncoder().encode(expenditureHtml));
  const expenditureTotal = [...parseSummaryGroupings(expenditureHtml, EXPENDITURE_SUMMARY_GRID_ID).values()].reduce(
    (total, cents) => total + cents,
    0
  );

  // Cash receipts exclude in-kind (the CF-2 reports in-kind on its own line).
  const cashReceipts = [...summary].reduce(
    (total, [label, cents]) => (/^In[- ]Kind/i.test(label) ? total : total + cents),
    0
  );
  const gate1Failures = [
    ...summaryDifferences,
    ...(expenditureTotal === EXPECTED_Q2_2026_EXPENDITURE_TOTAL_CENTS
      ? []
      : [`expenditures: expected ${formatCents(EXPECTED_Q2_2026_EXPENDITURE_TOTAL_CENTS)}, got ${formatCents(expenditureTotal)}`]),
    ...(cashReceipts === EXPECTED_Q2_2026_CASH_RECEIPTS_CENTS
      ? []
      : [`cash receipts: expected ${formatCents(EXPECTED_Q2_2026_CASH_RECEIPTS_CENTS)}, got ${formatCents(cashReceipts)}`]),
  ];
  gates.push({
    name: "1. stateless transaction reports reconcile",
    pass: gate1Failures.length === 0,
    detail:
      gate1Failures.length === 0
        ? `${summary.size} groupings cent-exact; cash receipts ${formatCents(cashReceipts)}; expenditures ${formatCents(expenditureTotal)}`
        : gate1Failures.join("; "),
  });

  // --- Gate 2: CSV export round-trip and per-type agreement. ---
  const csvText = await exportTransactionCsv(transport, {
    reportUrl: contributionUrl,
    reportHtml: contributionHtml,
  });
  await saveArtifact("mckee-q2-2026-contributions.csv", new TextEncoder().encode(csvText));
  const csvRows = parseCsv(csvText);
  const header = csvRows[0] ?? [];
  const headerMatches =
    header.length === ERTS_CONTRIBUTION_EXPORT_COLUMNS.length &&
    header.every((column, index) => column === ERTS_CONTRIBUTION_EXPORT_COLUMNS[index]);
  const typeIndex = header.indexOf("ContDesc");
  const amountIndex = header.indexOf("Amount");
  const exportTotals = new Map<string, number>();
  for (const row of csvRows.slice(1)) {
    const cents = parseMoneyToCents(row[amountIndex] ?? "");
    if (cents === null) continue;
    exportTotals.set(row[typeIndex] ?? "", (exportTotals.get(row[typeIndex] ?? "") ?? 0) + cents);
  }
  // Every summary grouping must be accounted for in one of two ways: the
  // export reproduces it cent-exact, or a typed search proves the portal
  // itself holds no itemized rows for it. A type absent from the export
  // without that proof is evidence of silent truncation, not of a
  // summary-only type — and the portal renders no exported-row count
  // anywhere, so this reconciliation is the only truncation control.
  const exportMismatches: string[] = [];
  const confirmedSummaryOnly: string[] = [];
  for (const [label, cents] of summary) {
    const exported = exportTotals.get(label);
    if (exported === cents) continue;
    if (exported !== undefined) {
      exportMismatches.push(`${label}: export ${formatCents(exported)} != summary ${formatCents(cents)}`);
      continue;
    }
    const code = ERTS_CONTRIBUTION_TYPE_CODES[label];
    if (code === undefined) {
      exportMismatches.push(`${label}: absent from the export and not in the pinned type vocabulary`);
      continue;
    }
    const typedHtml = decoder.decode(
      (await transport.fetch(ertsContributionReportUrl({ orgId: MCKEE_ORG_ID, ...Q2_2026, contributionTypeCode: code })))
        .body
    );
    const verdict = classifyContributionSearchResult(typedHtml);
    if (verdict === "no_rows") {
      confirmedSummaryOnly.push(label);
    } else {
      exportMismatches.push(
        `${label}: absent from the export but the typed search ${verdict === "rows" ? "returned itemized rows — the export dropped them" : "was unreadable"}`
      );
    }
  }
  const exportOnlyTypes = [...exportTotals.keys()].filter((label) => !summary.has(label));
  const gate2Failures = [
    ...(headerMatches ? [] : [`export header changed: ${header.join(",")}`]),
    ...exportMismatches,
    ...exportOnlyTypes.map((label) => `${label}: in the export but missing from the summary groupings`),
    ...(exportTotals.size > 0 ? [] : ["export contained no parseable rows"]),
  ];
  gates.push({
    name: "2. every summary grouping is accounted for by the export",
    pass: gate2Failures.length === 0,
    detail:
      gate2Failures.length === 0
        ? `${csvRows.length - 1} rows; ${exportTotals.size} types cent-exact; ${confirmedSummaryOnly.length} confirmed summary-only`
        : gate2Failures.join("; "),
  });

  // --- Gate 3: summary-only receipt types (totals must not be summed). ---
  const summaryOnlyCents = confirmedSummaryOnly.reduce((total, label) => total + (summary.get(label) ?? 0), 0);
  gates.push({
    name: "3. official totals are not the export sum",
    pass: confirmedSummaryOnly.length > 0,
    detail:
      confirmedSummaryOnly.length > 0
        ? `${confirmedSummaryOnly.join(", ")} = ${formatCents(summaryOnlyCents)} in the summary; typed search confirms no itemized rows`
        : "every summary grouping was reproducible from the export — re-check decision 2 before trusting export sums",
  });

  // --- Gate 4: organization discovery yields the numeric Board key. ---
  const discovery = await discoverOrganization(transport, {
    lastName: MCKEE_SEARCH_LAST_NAME,
    organizationName: MCKEE_ORGANIZATION_NAME,
    begin: CYCLE_BEGIN,
    end: CYCLE_END,
  });
  gates.push({
    name: "4. organization search yields the ERTS key",
    pass: discovery.match.orgId === MCKEE_ORG_ID,
    detail: `"${MCKEE_SEARCH_LAST_NAME}" matched ${discovery.candidates.length} organizations; ${MCKEE_ORGANIZATION_NAME} -> OrgID ${discovery.match.orgId}`,
  });

  // --- Gate 5: filing list exposes amendment state and version documents. ---
  const filings = await fetchOrganizationFilings(transport);
  await saveArtifact("mckee-filings.html", new TextEncoder().encode(filings.html));
  const amendedFilings = filings.rows.filter((row) => row.amended && row.filingId !== null);
  gates.push({
    name: "5. filing list exposes amendment state",
    pass: filings.rows.length > 0 && amendedFilings.length >= AMENDMENT_FAMILY_TARGET,
    detail: `${filings.rows.length} filed reports, ${amendedFilings.length} amended families with FilingIDs`,
  });

  // --- Gate 6: amendment semantics (decision 4). ---
  // A family only counts as evidence when it can actually discriminate
  // between the original and the latest version: both PDFs must parse, at
  // least one comparable receipt field must have CHANGED between the two
  // (identical totals match both versions and prove nothing), and the search
  // must equal the latest value on every comparable field — which, for a
  // changed field, also means it differs from the original. Extraction
  // failures and identical-total families are skipped as inconclusive, never
  // silently counted as agreement.
  const amendmentResults: string[] = [];
  let amendmentFailures = 0;
  let conclusiveFamilies = 0;
  let inconclusiveFamilies = 0;
  // Bound on portal traffic: each family costs 3-4 paced requests, and 5
  // conclusive families out of 12 would itself be a finding worth reading.
  const maxFamiliesFetched = 12;
  let familiesFetched = 0;

  // The CF-2 lines the transaction search can be held against. Each CF-2
  // line aggregates a SET of search types: line 6 is every in-kind type
  // (verified live on the 2022 window: In-Kind - Individual $3,049.67 +
  // In-Kind - Party $5,927.90 = line 6's $8,977.57), and the itemized+
  // aggregate pairs roll up the same way per decision 13's table.
  const amendmentChecks: { cf2Label: string; searchLabel: string; matches: (label: string) => boolean }[] = [
    {
      cf2Label: "2. Individuals",
      searchLabel: "Individual (+ Aggregate)",
      matches: (label) => label === "Individual" || label === "Aggregate - Individual",
    },
    {
      cf2Label: "4. Political Action Committees",
      searchLabel: "PAC (+ Aggregate)",
      matches: (label) => label === "PAC" || label === "Aggregate - PAC",
    },
    {
      cf2Label: "7. Interest Received",
      searchLabel: "Interest Received",
      matches: (label) => label === "Interest Received",
    },
    {
      cf2Label: "6. Report of In-Kind Contributions",
      searchLabel: "all In-Kind types",
      matches: (label) => /^In[- ]Kind/i.test(label),
    },
  ];

  for (const filing of amendedFilings) {
    if (conclusiveFamilies >= AMENDMENT_FAMILY_TARGET || familiesFetched >= maxFamiliesFetched) break;
    if (!/^\d{2}\/\d{2}\/\d{4}$/.test(filing.periodBegin) || !/^\d{2}\/\d{2}\/\d{4}$/.test(filing.periodEnd)) continue;
    familiesFetched += 1;
    const versions = await fetchFilingVersions(transport, filing.filingId as string);
    if (versions.length < 2) continue;

    // `grdAmendments` lists a family oldest-first (original, then each
    // amendment in filing order) — confirmed across the tested families,
    // including a three-version one: reading the last row as "latest" is what
    // makes the search comparison below agree.
    const latest = versions[versions.length - 1];
    const original = versions[0];
    const latestPdf = (await transport.fetch(latest.pdfUrl)).body;
    const originalPdf = (await transport.fetch(original.pdfUrl)).body;
    await saveArtifact(`cf2-${filing.filingId}-latest.pdf`, latestPdf);
    const latestValues = cf2SummaryValues(await cf2PageItems(latestPdf), CF2_RECEIPT_LABELS);
    const originalValues = cf2SummaryValues(await cf2PageItems(originalPdf), CF2_RECEIPT_LABELS);

    const comparable = amendmentChecks.filter(
      (check) => latestValues.has(check.cf2Label) && originalValues.has(check.cf2Label)
    );
    const changed = comparable.filter(
      (check) => latestValues.get(check.cf2Label) !== originalValues.get(check.cf2Label)
    );
    if (comparable.length === 0 || changed.length === 0) {
      inconclusiveFamilies += 1;
      amendmentResults.push(
        `${filing.reportType} (${filing.periodBegin}-${filing.periodEnd}, ${versions.length} versions): INCONCLUSIVE — ` +
          (comparable.length === 0 ? "CF-2 extraction yielded no comparable fields" : "versions identical on every comparable field")
      );
      continue;
    }

    const periodHtml = decoder.decode(
      (
        await transport.fetch(
          ertsContributionReportUrl({ orgId: MCKEE_ORG_ID, begin: filing.periodBegin, end: filing.periodEnd })
        )
      ).body
    );
    const periodSummary = parseSummaryGroupings(periodHtml, CONTRIBUTION_SUMMARY_GRID_ID);

    const searchTotal = (check: (typeof amendmentChecks)[number]): number =>
      [...periodSummary].reduce((total, [label, cents]) => (check.matches(label) ? total + cents : total), 0);
    const mismatches = comparable.filter((check) => searchTotal(check) !== latestValues.get(check.cf2Label));
    conclusiveFamilies += 1;
    if (mismatches.length > 0) amendmentFailures += 1;
    amendmentResults.push(
      `${filing.reportType} (${filing.periodBegin}-${filing.periodEnd}, ${versions.length} versions, ` +
        `${changed.length}/${comparable.length} fields changed): ` +
        (mismatches.length === 0
          ? "search matches latest version (and so differs from the original on the changed fields)"
          : mismatches
              .map(
                (check) =>
                  `${check.searchLabel} ${formatCents(searchTotal(check))} vs ${check.cf2Label} ${formatCents(latestValues.get(check.cf2Label) ?? 0)}`
              )
              .join("; "))
    );
  }
  for (const line of amendmentResults) console.log(`  amendment: ${line}`);
  gates.push({
    name: "6. transaction search is current-ledger state",
    pass: conclusiveFamilies >= AMENDMENT_FAMILY_TARGET && amendmentFailures === 0,
    detail:
      `${conclusiveFamilies}/${AMENDMENT_FAMILY_TARGET} conclusive families (${inconclusiveFamilies} inconclusive of ` +
      `${familiesFetched} fetched), ${amendmentFailures} disagreed with the latest CF-2`,
  });

  // --- Gate 7: CF-8 index pagination to the cycle boundary. ---
  const cycleStart = Date.parse("2025-01-01T00:00:00Z");
  // Inclusive upper bound: without it, a 2027 re-run of this probe would
  // count next cycle's filings as this cycle's.
  const cycleEnd = Date.parse("2026-12-31T00:00:00Z");
  const cf8 = await traverseCf8Index(transport, cycleStart);
  const cycleRows = cf8.rows.filter((row) => {
    const filed = parseFiledDate(row.filedDate);
    return filed >= cycleStart && filed <= cycleEnd;
  });
  const independentExpenditures = cycleRows.filter((row) => /INDEPENDENT EXPENDITURE/i.test(row.filingType));
  const filingTypes = [...new Set(cycleRows.map((row) => row.filingType))];
  console.log(
    `\nCF-8 index: ${cf8.pages} pages, ${cf8.rows.length} rows scanned, ${cycleRows.length} in cycle, ` +
      `${independentExpenditures.length} independent expenditures; types: ${filingTypes.join(", ")}`
  );
  for (const row of independentExpenditures) {
    console.log(`  IE ${row.filedDate}  ${row.organizationName}  ${row.scannedUrl ?? "(no scan link)"}`);
  }
  gates.push({
    name: "7. CF-8 index paginates to the cycle boundary",
    pass: cf8.reachedBoundary && cf8.descending && cf8.rows.every((row) => row.scannedUrl !== null),
    detail: `${cf8.pages} pages traversed, dates ${cf8.descending ? "descend" : "DO NOT descend"}, boundary ${cf8.reachedBoundary ? "reached" : "NOT reached"}`,
  });

  // --- Summary. ---
  console.log("\n=== PR 3 acquisition-spike gates ===");
  let failures = 0;
  for (const gate of gates) {
    if (!gate.pass) failures += 1;
    console.log(`${gate.pass ? "PASS" : "FAIL"}  ${gate.name} — ${gate.detail}`);
  }
  console.log(`\nartifacts written to backend/${RHODE_ISLAND_FINANCE_PROBE_CACHE_DIR}`);
  if (failures > 0) {
    process.exitCode = 1;
    console.log(`${failures} gate(s) failed`);
  } else {
    console.log("all gates passed");
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Rhode Island candidate finance probe failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
