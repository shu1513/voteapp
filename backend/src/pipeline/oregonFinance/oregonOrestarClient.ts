import {
  isOregonOrestarBlockedPage,
  OREGON_ORESTAR_TRANSACTION_SEARCH_URL,
  parseOregonOrestarSearchForm,
  parseOregonOrestarTransactionDetail,
  parseOregonOrestarTransactionSearchResults,
  type OregonOrestarSearchForm,
  type OregonOrestarTransactionDetail,
  type OregonOrestarTransactionSearchResults,
} from "./oregonOrestarParser.js";

export type OregonOrestarFetchResponse = {
  ok: boolean;
  status: number;
  statusText?: string;
  text: () => Promise<string>;
};

export type OregonOrestarFetch = (
  url: string,
  init?: RequestInit
) => Promise<OregonOrestarFetchResponse>;

export type OregonOrestarClientOptions = {
  fetchFn?: OregonOrestarFetch;
  userAgent?: string;
};

const DEFAULT_USER_AGENT = "voteApp Oregon campaign finance sync";
const GENERIC_SEARCH_URL = new URL(OREGON_ORESTAR_TRANSACTION_SEARCH_URL).toString();

function getFetch(fetchFn: OregonOrestarFetch | undefined): OregonOrestarFetch {
  if (fetchFn) {
    return fetchFn;
  }
  if (typeof globalThis.fetch !== "function") {
    throw new Error("fetch is not available for Oregon ORESTAR requests");
  }
  return globalThis.fetch.bind(globalThis) as OregonOrestarFetch;
}

function requestHeaders(userAgent: string | undefined): HeadersInit {
  return {
    "user-agent": userAgent?.trim() || DEFAULT_USER_AGENT,
    accept: "text/html,application/xhtml+xml",
  };
}

async function fetchOrestarHtml(url: string, options: OregonOrestarClientOptions = {}): Promise<string> {
  const response = await getFetch(options.fetchFn)(url, {
    headers: requestHeaders(options.userAgent),
  });
  const html = await response.text();
  if (!response.ok) {
    throw new Error(`ORESTAR request failed ${response.status} ${response.statusText ?? ""}`.trim());
  }
  if (isOregonOrestarBlockedPage(html)) {
    throw new Error("ORESTAR request blocked by cyber-security page");
  }
  return html;
}

function isTransactionDetailUrl(url: string): boolean {
  return /\/orestar\/gotoPublicTransactionDetail\.do\b/i.test(url);
}

function isGenericSearchUrl(url: string): boolean {
  const parsed = new URL(url, OREGON_ORESTAR_TRANSACTION_SEARCH_URL);
  return parsed.toString() === GENERIC_SEARCH_URL;
}

export async function getOregonOrestarSearchForm(
  options: OregonOrestarClientOptions = {}
): Promise<OregonOrestarSearchForm> {
  const html = await fetchOrestarHtml(OREGON_ORESTAR_TRANSACTION_SEARCH_URL, options);
  return parseOregonOrestarSearchForm(html, OREGON_ORESTAR_TRANSACTION_SEARCH_URL);
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

  const searchResults = await getOregonOrestarTransactionSearchResults({
    url: sourceUrl,
    options: input.options,
  });
  const details: OregonOrestarTransactionDetail[] = [];
  for (const row of searchResults.rows) {
    if (!row.detailUrl || details.length >= maxDetails) {
      continue;
    }
    details.push(await getOregonOrestarTransactionDetail({ url: row.detailUrl, options: input.options }));
  }
  return details;
}
