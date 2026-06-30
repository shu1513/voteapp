import {
  isOregonOrestarBlockedPage,
  OREGON_ORESTAR_BASE_URL,
  OREGON_ORESTAR_TRANSACTION_SEARCH_URL,
  parseOregonOrestarSearchForm,
  parseOregonOrestarTransactionDetail,
  parseOregonOrestarTransactionSearchResults,
  type OregonOrestarSearchForm,
  type OregonOrestarTransactionDetail,
  type OregonOrestarTransactionSearchResultRow,
  type OregonOrestarTransactionSearchResults,
} from "./oregonOrestarParser.js";

export type OregonOrestarFetchResponse = {
  ok: boolean;
  status: number;
  statusText?: string;
  headers?: Pick<Headers, "get"> & {
    getSetCookie?: () => string[];
  };
  text: () => Promise<string>;
};

export type OregonOrestarFetch = (
  url: string,
  init?: RequestInit
) => Promise<OregonOrestarFetchResponse>;

export type OregonOrestarClientOptions = {
  fetchFn?: OregonOrestarFetch;
  userAgent?: string;
  timeoutMs?: number;
};

const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_USER_AGENT = "voteApp Oregon campaign finance sync";
const GENERIC_SEARCH_URL = new URL(OREGON_ORESTAR_TRANSACTION_SEARCH_URL).toString();
const OREGON_ORESTAR_CSRF_GUARD_SCRIPT_URL = `${OREGON_ORESTAR_BASE_URL}/orestar/JavaScriptServlet`;

function parseAllowedOrestarUrl(url: string): URL | null {
  let parsed: URL;
  try {
    parsed = new URL(url, OREGON_ORESTAR_TRANSACTION_SEARCH_URL);
  } catch {
    return null;
  }
  return parsed.origin === OREGON_ORESTAR_BASE_URL ? parsed : null;
}

function normalizeAllowedOrestarUrl(url: string): string {
  const parsed = parseAllowedOrestarUrl(url);
  if (!parsed) {
    throw new Error(`Oregon ORESTAR URL must use ${OREGON_ORESTAR_BASE_URL}`);
  }
  return parsed.toString();
}

function getFetch(fetchFn: OregonOrestarFetch | undefined): OregonOrestarFetch {
  if (fetchFn) {
    return fetchFn;
  }
  if (typeof globalThis.fetch !== "function") {
    throw new Error("fetch is not available for Oregon ORESTAR requests");
  }
  return globalThis.fetch.bind(globalThis) as OregonOrestarFetch;
}

function requestHeaders(userAgent: string | undefined, extraHeaders: HeadersInit = {}): HeadersInit {
  return {
    "user-agent": userAgent?.trim() || DEFAULT_USER_AGENT,
    accept: "text/html,application/xhtml+xml",
    ...extraHeaders,
  };
}

function responseCookies(response: OregonOrestarFetchResponse): string[] {
  const getSetCookie = response.headers?.getSetCookie;
  if (typeof getSetCookie === "function") {
    return getSetCookie.call(response.headers);
  }
  const setCookie = response.headers?.get("set-cookie");
  return setCookie ? [setCookie] : [];
}

function cookieHeader(cookies: readonly string[]): string | null {
  const pairs = cookies
    .map((cookie) => cookie.split(";")[0]?.trim() ?? "")
    .filter(Boolean);
  return pairs.length > 0 ? pairs.join("; ") : null;
}

async function fetchOrestarHtmlPage(
  url: string,
  options: OregonOrestarClientOptions = {},
  init: RequestInit = {}
): Promise<{ html: string; cookies: string[] }> {
  const requestUrl = normalizeAllowedOrestarUrl(url);
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`Invalid Oregon ORESTAR timeoutMs: ${options.timeoutMs}`);
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await getFetch(options.fetchFn)(requestUrl, {
      ...init,
      headers: requestHeaders(options.userAgent, init.headers),
      signal: controller.signal,
    });
    const html = await response.text();
    if (!response.ok) {
      throw new Error(`ORESTAR request failed ${response.status} ${response.statusText ?? ""}`.trim());
    }
    if (isOregonOrestarBlockedPage(html)) {
      throw new Error("ORESTAR request blocked by cyber-security page");
    }
    return { html, cookies: responseCookies(response) };
  } catch (error) {
    if (controller.signal.aborted) {
      throw new Error(`ORESTAR request timed out after ${timeoutMs}ms for ${requestUrl}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchOrestarHtml(url: string, options: OregonOrestarClientOptions = {}): Promise<string> {
  return (await fetchOrestarHtmlPage(url, options)).html;
}

async function fetchOrestarCsrfToken(input: {
  cookies: readonly string[];
  referer: string;
  options?: OregonOrestarClientOptions;
}): Promise<string> {
  const cookie = cookieHeader(input.cookies);
  const { html } = await fetchOrestarHtmlPage(
    OREGON_ORESTAR_CSRF_GUARD_SCRIPT_URL,
    input.options,
    {
      method: "POST",
      headers: {
        accept: "text/plain,*/*",
        "FETCH-CSRF-TOKEN": "1",
        referer: input.referer,
        ...(cookie ? { cookie } : {}),
      },
    }
  );
  const rawToken = html.trim();
  const separatorIndex = rawToken.indexOf(":");
  const tokenName = separatorIndex >= 0 ? rawToken.slice(0, separatorIndex) : rawToken;
  const tokenValue = separatorIndex >= 0 ? rawToken.slice(separatorIndex + 1) : "";
  if (tokenName !== "OWASP_CSRFTOKEN" || !tokenValue.trim()) {
    throw new Error("ORESTAR transaction search CSRF token not found");
  }
  return tokenValue.trim();
}

function isTransactionDetailUrl(url: string): boolean {
  return parseAllowedOrestarUrl(url)?.pathname === "/orestar/gotoPublicTransactionDetail.do";
}

function isGenericSearchUrl(url: string): boolean {
  return parseAllowedOrestarUrl(url)?.toString() === GENERIC_SEARCH_URL;
}

function formatOregonDate(year: number, month: number, day: number): string {
  return `${String(month).padStart(2, "0")}/${String(day).padStart(2, "0")}/${year}`;
}

function assertOregonSearchElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100) {
    throw new Error(`Invalid Oregon ORESTAR electionYear: ${electionYear}`);
  }
}

function buildOregonCandidateSearchFormData(input: {
  candidateName: string;
  electionYear: number;
  csrfToken: string;
}): URLSearchParams {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    throw new Error("Oregon ORESTAR candidate search requires candidateName");
  }
  assertOregonSearchElectionYear(input.electionYear);
  const params = new URLSearchParams();
  params.set("cneSearchButtonName", "search");
  params.set("cneSearchPageIdx", "0");
  params.set("cneSearchContributorTypeName", "");
  params.set("cneSearchTranTypeName", "");
  params.set("cneSearchTranSubTypeName", "");
  params.set("cneSearchTranPurposeName", "");
  params.set("cneSearchFilerCommitteeId", "");
  params.set("cneSearchFilerCommitteeTxt", candidateName);
  params.set("cneSearchFilerCommitteeTxtSearchType", "C");
  params.set("cneSearchTranStartDate", formatOregonDate(input.electionYear, 1, 1));
  params.set("cneSearchTranEndDate", formatOregonDate(input.electionYear, 12, 31));
  params.set("cneSearchTranFiledStartDate", "");
  params.set("cneSearchTranFiledEndDate", "");
  params.set("transactionId", "");
  params.set("cneSearchTranType", "");
  params.set("cneSearchTranAmountFrom", "");
  params.set("cneSearchTranAmountTo", "");
  params.set("cneSearchContributorTxt", "");
  params.set("cneSearchContributorTxtSearchType", "C");
  params.set("cneSearchContributorType", "");
  params.set("addressLine1", "");
  params.set("city", "");
  params.set("state", "");
  params.set("zip", "");
  params.set("zipPlusFour", "");
  params.set("occupation", "");
  params.set("employer", "");
  params.set("employerCity", "");
  params.set("employerState", "");
  params.set("OWASP_CSRFTOKEN", input.csrfToken);
  return params;
}

export async function getOregonOrestarSearchForm(
  options: OregonOrestarClientOptions = {}
): Promise<OregonOrestarSearchForm> {
  const page = await fetchOrestarHtmlPage(OREGON_ORESTAR_TRANSACTION_SEARCH_URL, options);
  const form = parseOregonOrestarSearchForm(page.html, OREGON_ORESTAR_TRANSACTION_SEARCH_URL);
  const pageCookieHeader = cookieHeader(page.cookies);
  if (form.csrfToken) {
    return {
      ...form,
      cookieHeader: pageCookieHeader,
    };
  }
  return {
    ...form,
    cookieHeader: pageCookieHeader,
    csrfToken: await fetchOrestarCsrfToken({
      cookies: page.cookies,
      referer: OREGON_ORESTAR_TRANSACTION_SEARCH_URL,
      options,
    }),
  };
}

export async function getOregonOrestarCandidateSearchRows(input: {
  candidateName: string;
  electionYear: number;
  options?: OregonOrestarClientOptions;
}): Promise<OregonOrestarTransactionSearchResultRow[]> {
  const form = await getOregonOrestarSearchForm(input.options);
  if (!form.csrfToken) {
    throw new Error("ORESTAR transaction search CSRF token not found");
  }
  const { html } = await fetchOrestarHtmlPage(
    form.actionUrl,
    input.options,
    {
      method: "POST",
      headers: {
        accept: "text/html,application/xhtml+xml",
        "content-type": "application/x-www-form-urlencoded",
        referer: OREGON_ORESTAR_TRANSACTION_SEARCH_URL,
        ...(form.cookieHeader ? { cookie: form.cookieHeader } : {}),
      },
      body: buildOregonCandidateSearchFormData({
        candidateName: input.candidateName,
        electionYear: input.electionYear,
        csrfToken: form.csrfToken,
      }).toString(),
    }
  );
  return parseOregonOrestarTransactionSearchResults(html, form.actionUrl).rows;
}

export async function getOregonOrestarTransactionSearchResults(input: {
  url: string;
  options?: OregonOrestarClientOptions;
}): Promise<OregonOrestarTransactionSearchResults> {
  const html = await fetchOrestarHtml(input.url, input.options);
  return parseOregonOrestarTransactionSearchResults(html, input.url);
}

export async function getOregonOrestarTransactionDetail(input: {
  url: string;
  options?: OregonOrestarClientOptions;
}): Promise<OregonOrestarTransactionDetail> {
  const html = await fetchOrestarHtml(input.url, input.options);
  return parseOregonOrestarTransactionDetail(html, input.url);
}

export async function getOregonOrestarTransactionDetailsFromSourceUrl(input: {
  sourceUrl: string | null | undefined;
  maxDetails?: number;
  options?: OregonOrestarClientOptions;
}): Promise<OregonOrestarTransactionDetail[]> {
  const sourceUrl = input.sourceUrl?.trim();
  if (!sourceUrl || isGenericSearchUrl(sourceUrl)) {
    throw new Error("Oregon ORESTAR source URL must point to a transaction detail or populated search result");
  }
  if (isTransactionDetailUrl(sourceUrl)) {
    return [await getOregonOrestarTransactionDetail({ url: sourceUrl, options: input.options })];
  }

  const maxDetails = input.maxDetails ?? 500;
  if (!Number.isInteger(maxDetails) || maxDetails <= 0) {
    throw new Error(`Invalid Oregon ORESTAR maxDetails: ${input.maxDetails}`);
  }

  const details: OregonOrestarTransactionDetail[] = [];
  const visitedPageUrls = new Set<string>();
  let pageUrl: string | null = sourceUrl;
  while (pageUrl && details.length < maxDetails) {
    if (visitedPageUrls.has(pageUrl)) {
      break;
    }
    visitedPageUrls.add(pageUrl);
    const searchResults = await getOregonOrestarTransactionSearchResults({
      url: pageUrl,
      options: input.options,
    });
    for (const row of searchResults.rows) {
      if (!row.detailUrl || details.length >= maxDetails) {
        continue;
      }
      details.push(await getOregonOrestarTransactionDetail({ url: row.detailUrl, options: input.options }));
    }
    pageUrl = searchResults.nextPageUrl;
  }
  return details;
}
