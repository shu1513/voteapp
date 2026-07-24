export const MINNESOTA_CFB_BASE_URL = "https://register.cfb.mn.gov";
export const MINNESOTA_CFB_FINANCIAL_SUMMARY_API_URL =
  `${MINNESOTA_CFB_BASE_URL}/reports-and-data/viewers/campaign-finance/candidates/api`;
export const MINNESOTA_CFB_FINANCIAL_SUMMARY_TIMEOUT_MS = 30_000;

export type MinnesotaCandidateFinancialSummary = {
  committeeId: string;
  electionYear: number;
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number;
  sourceUrl: string;
};

export type MinnesotaCandidateFinancialSummaryClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

type MinnesotaCfbFinancialSummaryPayload = {
  tabcontent: string;
};

const DIRECT_CONTRIBUTION_LABELS = [
  "Individuals contributions",
  "Lobbyist contributions",
  "Committee/fund contributions",
  "Party unit contributions",
  "Other contributions",
] as const;

function normalizeCommitteeId(value: string): string {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    throw new Error(`Invalid Minnesota CFB committee id: ${value}`);
  }
  return normalized;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Minnesota CFB election year: ${value}`);
  }
  return value;
}

function htmlDecode(value: string): string {
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_match, code) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_match, code) => String.fromCodePoint(Number.parseInt(code, 16)));
}

function htmlText(value: string): string {
  return htmlDecode(value.replace(/<[^>]*>/g, " ")).replace(/\s+/g, " ").trim();
}

function parseMoneyCents(value: string, label: string): number {
  const normalized = value.replace(/[$,\s]/g, "");
  if (!/^\d+(?:\.\d{1,2})?$/.test(normalized)) {
    throw new Error(`Minnesota CFB financial summary has invalid ${label}: ${value}`);
  }
  const cents = Math.round(Number(normalized) * 100);
  if (!Number.isSafeInteger(cents) || cents < 0) {
    throw new Error(`Minnesota CFB financial summary has invalid ${label}: ${value}`);
  }
  return cents;
}

function centsToDollars(value: number): number {
  return value / 100;
}

function tableRowsByLabel(tableHtml: string): Map<string, string> {
  const rows = new Map<string, string>();
  for (const rowMatch of tableHtml.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = rowMatch[1] ?? "";
    const labelMatch = /<th\b[^>]*>([\s\S]*?)<\/th>/i.exec(rowHtml);
    const valueMatch = /<td\b[^>]*>([\s\S]*?)<\/td>/i.exec(rowHtml);
    if (!labelMatch || !valueMatch) {
      continue;
    }
    const label = htmlText(labelMatch[1] ?? "");
    if (label) {
      rows.set(label, htmlText(valueMatch[1] ?? ""));
    }
  }
  return rows;
}

function requireMoneyRow(rows: ReadonlyMap<string, string>, label: string, year: number): number {
  const value = rows.get(label);
  if (value === undefined) {
    throw new Error(`Minnesota CFB financial summary is missing ${label} for ${year}`);
  }
  return parseMoneyCents(value, `${label} for ${year}`);
}

export function buildMinnesotaCandidateFinancialSummaryUrl(input: {
  committeeId: string;
  electionYear: number;
}): string {
  const committeeId = normalizeCommitteeId(input.committeeId);
  const electionYear = normalizeElectionYear(input.electionYear);
  return `${MINNESOTA_CFB_BASE_URL}/reports-and-data/viewers/campaign-finance/candidates/${committeeId}/${electionYear}/`;
}

export function parseMinnesotaCandidateFinancialSummaryHtml(input: {
  committeeId: string;
  electionYear: number;
  html: string;
  sourceUrl?: string;
}): MinnesotaCandidateFinancialSummary | null {
  const committeeId = normalizeCommitteeId(input.committeeId);
  const electionYear = normalizeElectionYear(input.electionYear);
  const sourceUrl = input.sourceUrl ?? buildMinnesotaCandidateFinancialSummaryUrl({ committeeId, electionYear });
  const allowedYears = new Set([electionYear - 1, electionYear]);
  const seenYears = new Set<number>();
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;
  let totalDisbursementsCents = 0;

  for (const tableMatch of input.html.matchAll(/<table\b[^>]*>([\s\S]*?)<\/table>/gi)) {
    const tableHtml = tableMatch[1] ?? "";
    const yearMatch = /<th\b[^>]*colspan=["']?2["']?[^>]*>\s*(\d{4})\s*-\s*Election year\s*<\/th>/i.exec(
      tableHtml
    );
    if (!yearMatch) {
      continue;
    }
    const year = Number(yearMatch[1]);
    if (!allowedYears.has(year)) {
      throw new Error(`Minnesota CFB financial summary returned unexpected election year ${year}`);
    }
    if (seenYears.has(year)) {
      throw new Error(`Minnesota CFB financial summary returned duplicate election year ${year}`);
    }
    seenYears.add(year);

    const rows = tableRowsByLabel(tableHtml);
    totalReceiptsCents += requireMoneyRow(rows, "Total receipts", year);
    totalDisbursementsCents += requireMoneyRow(rows, "Total expenditures", year);
    for (const label of DIRECT_CONTRIBUTION_LABELS) {
      directContributionTotalCents += requireMoneyRow(rows, label, year);
    }
  }

  if (seenYears.size === 0) {
    const unavailableYears = [...input.html.matchAll(/Data\s+not\s+available\s+for\s+(\d{4})/gi)].map(
      (match) => Number(match[1])
    );
    if (unavailableYears.length > 0 && unavailableYears.every((year) => allowedYears.has(year))) {
      return null;
    }
    throw new Error("Minnesota CFB financial summary contains no election-year tables");
  }
  if (
    !Number.isSafeInteger(totalReceiptsCents) ||
    !Number.isSafeInteger(directContributionTotalCents) ||
    !Number.isSafeInteger(totalDisbursementsCents)
  ) {
    throw new Error("Minnesota CFB financial summary totals exceed the safe numeric range");
  }

  return {
    committeeId,
    electionYear,
    totalReceipts: centsToDollars(totalReceiptsCents),
    directContributionTotal: centsToDollars(directContributionTotalCents),
    totalDisbursements: centsToDollars(totalDisbursementsCents),
    sourceUrl,
  };
}

function responseCookies(response: Response): string {
  const headers = response.headers as Headers & { getSetCookie?: () => string[] };
  const setCookies = headers.getSetCookie?.() ?? [];
  return setCookies
    .map((setCookie) => setCookie.split(";", 1)[0]?.trim() ?? "")
    .filter(Boolean)
    .join("; ");
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  options: MinnesotaCandidateFinancialSummaryClientOptions
): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? MINNESOTA_CFB_FINANCIAL_SUMMARY_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`Invalid Minnesota CFB financial summary timeout: ${timeoutMs}`);
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await (options.fetchImpl ?? fetch)(url, { ...init, signal: controller.signal });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Minnesota CFB financial summary request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function parseFinancialSummaryPayload(value: unknown): MinnesotaCfbFinancialSummaryPayload {
  if (typeof value !== "object" || value === null || typeof (value as { tabcontent?: unknown }).tabcontent !== "string") {
    throw new Error("Minnesota CFB financial summary response is missing tabcontent HTML");
  }
  return { tabcontent: (value as { tabcontent: string }).tabcontent };
}

export async function fetchMinnesotaCandidateFinancialSummary(
  input: { committeeId: string; electionYear: number },
  options: MinnesotaCandidateFinancialSummaryClientOptions = {}
): Promise<MinnesotaCandidateFinancialSummary | null> {
  const committeeId = normalizeCommitteeId(input.committeeId);
  const electionYear = normalizeElectionYear(input.electionYear);
  const sourceUrl = buildMinnesotaCandidateFinancialSummaryUrl({ committeeId, electionYear });
  const pageResponse = await fetchWithTimeout(
    sourceUrl,
    {
      headers: {
        accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
        "user-agent": "voteApp Minnesota campaign finance sync/1.0",
      },
    },
    options
  );
  if (!pageResponse.ok) {
    throw new Error(`Minnesota CFB candidate page request failed: ${pageResponse.status} ${pageResponse.statusText}`);
  }
  const cookie = responseCookies(pageResponse);
  if (!cookie) {
    throw new Error("Minnesota CFB candidate page did not establish a session");
  }
  await pageResponse.arrayBuffer();

  const body = new URLSearchParams({
    id: committeeId,
    year: String(electionYear),
    "year_data[ElectionSegmentEndDate]": String(electionYear),
    "year_data[ElectionSegmentStartDate]": String(electionYear - 1),
    tabname: "financial",
  });
  const summaryResponse = await fetchWithTimeout(
    MINNESOTA_CFB_FINANCIAL_SUMMARY_API_URL,
    {
      method: "POST",
      headers: {
        accept: "application/json,text/javascript,*/*;q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        cookie,
        referer: sourceUrl,
        "user-agent": "voteApp Minnesota campaign finance sync/1.0",
        "x-requested-with": "XMLHttpRequest",
      },
      body,
    },
    options
  );
  if (!summaryResponse.ok) {
    throw new Error(
      `Minnesota CFB financial summary request failed: ${summaryResponse.status} ${summaryResponse.statusText}`
    );
  }

  let payload: unknown;
  try {
    payload = JSON.parse(await summaryResponse.text());
  } catch {
    throw new Error("Minnesota CFB financial summary response is not valid JSON");
  }
  return parseMinnesotaCandidateFinancialSummaryHtml({
    committeeId,
    electionYear,
    html: parseFinancialSummaryPayload(payload).tabcontent,
    sourceUrl,
  });
}
