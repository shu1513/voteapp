import type { HoustonFinanceReportIndexRecord } from "./houstonFinanceTypes.js";

export const HOUSTON_LEGACY_BASE_URL = "https://cohweb.houstontx.gov/CampaignFinanceWeb/";
const SEARCH_PATH = "CFRwebsiteSimpleSearch.aspx";
const RESULTS_PATH = "CFRwebsiteSimpleSearchResult.aspx";
const DEFAULT_TIMEOUT_MS = 120_000;

type FetchLike = typeof fetch;
export type HoustonLegacyClientOptions = { fetchImpl?: FetchLike; timeoutMs?: number };
export type HoustonLegacySearchSession = { cookie: string; resultHtml: string; reports: HoustonFinanceReportIndexRecord[] };

function decodeHtml(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&#x27;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hiddenFields(html: string): Record<string, string> {
  const fields: Record<string, string> = {};
  for (const match of html.matchAll(/<input\b[^>]*type=["']hidden["'][^>]*>/gi)) {
    const name = /\bname=["']([^"']+)["']/i.exec(match[0])?.[1];
    const value = /\bvalue=["']([^"']*)["']/i.exec(match[0])?.[1] ?? "";
    if (name) fields[decodeHtml(name)] = decodeHtml(value);
  }
  return fields;
}

function firstCookie(response: Response): string {
  return response.headers.get("set-cookie")?.split(";", 1)[0]?.trim() ?? "";
}

async function request(input: {
  url: string;
  options: HoustonLegacyClientOptions;
  cookie?: string;
  body?: URLSearchParams;
}): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), input.options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  try {
    return await (input.options.fetchImpl ?? fetch)(input.url, {
      method: input.body ? "POST" : "GET",
      headers: {
        ...(input.cookie ? { cookie: input.cookie } : {}),
        ...(input.body ? { "content-type": "application/x-www-form-urlencoded" } : {}),
      },
      body: input.body,
      redirect: "follow",
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

export function parseHoustonLegacySearchResults(html: string): HoustonFinanceReportIndexRecord[] {
  const table = /<table\b[^>]*id=["']ctl00_ContentPlaceHolder1_grdCandidate["'][^>]*>([\s\S]*?)<\/table>/i.exec(html)?.[1];
  if (!table) return [];
  const reports: HoustonFinanceReportIndexRecord[] = [];
  for (const rowMatch of table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...rowMatch[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map((match) => decodeHtml(match[1]));
    const selection = /Select\$(\d+)/i.exec(rowMatch[1])?.[1];
    if (!selection || cells.length < 14) continue;
    const campaignYear = Number(cells[8]);
    const received = cells[5];
    const dateMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})/.exec(received);
    if (!dateMatch || !Number.isInteger(campaignYear)) continue;
    const receivedDate = `${dateMatch[3]}-${dateMatch[1].padStart(2, "0")}-${dateMatch[2].padStart(2, "0")}`;
    reports.push({
      sourceSystem: "legacy_webforms",
      reportId: cells[6],
      filerId: cells[13],
      filerName: [cells[2], cells[3]].filter(Boolean).join(" "),
      filerType: cells[4],
      reportType: cells[7],
      receivedDate,
      filedAt: cells[9] || received,
      periodStart: null,
      periodEnd: null,
      officeDescription: null,
      campaignYear,
      pdfUrl: null,
      legacySelectionIndex: Number(selection),
    });
  }
  return reports;
}

export async function searchHoustonLegacyCandidateReports(
  input: { firstName: string; lastName: string },
  options: HoustonLegacyClientOptions = {}
): Promise<HoustonLegacySearchSession> {
  const getResponse = await request({ url: new URL(SEARCH_PATH, HOUSTON_LEGACY_BASE_URL).toString(), options });
  if (!getResponse.ok) throw new Error(`Houston legacy search page failed: ${getResponse.status}`);
  const cookie = firstCookie(getResponse);
  const getHtml = await getResponse.text();
  const body = new URLSearchParams(hiddenFields(getHtml));
  body.set("ctl00$ContentPlaceHolder1$rdoWildCard", "Exact");
  body.set("ctl00$ContentPlaceHolder1$txtLast_EntityName_coh", input.lastName.trim());
  body.set("ctl00$ContentPlaceHolder1$txtFirstName_coh", input.firstName.trim());
  body.set("ctl00$ContentPlaceHolder1$btnSearch_coh", "Search");
  const response = await request({
    url: new URL(SEARCH_PATH, HOUSTON_LEGACY_BASE_URL).toString(),
    options,
    cookie,
    body,
  });
  if (!response.ok) throw new Error(`Houston legacy candidate search failed: ${response.status}`);
  const resultHtml = await response.text();
  return { cookie, resultHtml, reports: parseHoustonLegacySearchResults(resultHtml) };
}

export async function downloadHoustonLegacyReportPdf(
  session: HoustonLegacySearchSession,
  report: HoustonFinanceReportIndexRecord,
  options: HoustonLegacyClientOptions = {}
): Promise<Uint8Array> {
  if (report.sourceSystem !== "legacy_webforms" || report.legacySelectionIndex === undefined) {
    throw new Error("Houston legacy PDF download requires a legacy report selection");
  }
  const body = new URLSearchParams(hiddenFields(session.resultHtml));
  body.set("__EVENTTARGET", "ctl00$ContentPlaceHolder1$grdCandidate");
  body.set("__EVENTARGUMENT", `Select$${report.legacySelectionIndex}`);
  const response = await request({
    url: new URL(RESULTS_PATH, HOUSTON_LEGACY_BASE_URL).toString(),
    options,
    cookie: session.cookie,
    body,
  });
  if (!response.ok) throw new Error(`Houston legacy PDF request failed: ${response.status}`);
  const data = new Uint8Array(await response.arrayBuffer());
  if (data.length < 5 || new TextDecoder().decode(data.slice(0, 5)) !== "%PDF-") {
    throw new Error("Houston legacy report response was not a PDF");
  }
  return data;
}
