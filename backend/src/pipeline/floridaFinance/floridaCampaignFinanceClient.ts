import { createHash } from "node:crypto";

import { isFloridaCampaignFinanceBrowserExportEnabled } from "../../config/featureFlags.js";
import {
  normalizeFloridaTextKey,
  parseFloridaContributionTsv,
  type FloridaContributionRow,
} from "./floridaCampaignFinanceRows.js";

export const FLORIDA_CAMPAIGN_FINANCE_CONTRIBUTION_SEARCH_URL =
  "https://dos.elections.myflorida.com/campaign-finance/contributions/";
export const FLORIDA_CAMPAIGN_FINANCE_CONTRIBUTION_EXPORT_URL =
  "https://dos.elections.myflorida.com/cgi-bin/contrib.exe";
export const FLORIDA_CAMPAIGN_FINANCE_DEFAULT_ROW_LIMIT = 10_000;
export const FLORIDA_CAMPAIGN_FINANCE_MAX_ROW_LIMIT = 100_000;

export type FloridaContributionSearchType = "candidate_detail" | "committee_detail";

export type FloridaContributionExportQuery = {
  searchType: FloridaContributionSearchType;
  electionCode?: string | null;
  candidateFirstName?: string | null;
  candidateLastName?: string | null;
  committeeName?: string | null;
  committeeType?: string | null;
  dateFrom?: string | null;
  dateTo?: string | null;
  rowLimit?: number | null;
};

export type NormalizedFloridaContributionExportQuery = {
  searchType: FloridaContributionSearchType;
  electionCode: string | null;
  candidateFirstName: string | null;
  candidateLastName: string | null;
  committeeName: string | null;
  committeeType: string | null;
  dateFrom: string | null;
  dateTo: string | null;
  rowLimit: number;
};

export type FloridaContributionExportTransportRequest = {
  query: NormalizedFloridaContributionExportQuery;
  searchPageUrl: string;
  exportUrl: string;
  formData: URLSearchParams;
  cacheKey: string;
};

export type FloridaContributionExportTransportResult = {
  tsv: string;
  finalUrl?: string | null;
  retrievedAt?: Date;
};

export type FloridaContributionExportTransport = (
  request: FloridaContributionExportTransportRequest
) => Promise<FloridaContributionExportTransportResult>;

export type FloridaContributionExportFetchTransportOptions = {
  fetchFn?: typeof fetch;
  timeoutMs?: number;
  userAgent?: string;
};

export type FloridaContributionExportRateLimiter = (
  request: FloridaContributionExportTransportRequest
) => Promise<void>;

export type FloridaContributionExportRateLimiterOptions = {
  minIntervalMs: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
};

export type FloridaContributionExportRowsInput = FloridaContributionExportQuery & {
  transport?: FloridaContributionExportTransport;
  rateLimiter?: FloridaContributionExportRateLimiter;
  force?: boolean;
};

export type FloridaContributionExportRowsResult = {
  query: NormalizedFloridaContributionExportQuery;
  searchPageUrl: string;
  exportUrl: string;
  sourceUrl: string;
  cacheKey: string;
  retrievedAt: Date;
  rowCount: number;
  formData: Record<string, string>;
  tsv: string;
  rows: FloridaContributionRow[];
};

function normalizeOptionalText(value: string | null | undefined): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ");
  return normalized && normalized.length > 0 ? normalized : null;
}

function normalizeSearchType(value: FloridaContributionSearchType): FloridaContributionSearchType {
  if (value === "candidate_detail" || value === "committee_detail") {
    return value;
  }
  throw new Error(`Invalid Florida contribution search type: ${value}`);
}

function normalizeRowLimit(value: number | null | undefined): number {
  const normalized = value ?? FLORIDA_CAMPAIGN_FINANCE_DEFAULT_ROW_LIMIT;
  if (
    !Number.isInteger(normalized) ||
    normalized <= 0 ||
    normalized > FLORIDA_CAMPAIGN_FINANCE_MAX_ROW_LIMIT
  ) {
    throw new Error(
      `Florida contribution rowLimit must be an integer between 1 and ${FLORIDA_CAMPAIGN_FINANCE_MAX_ROW_LIMIT}`
    );
  }
  return normalized;
}

function normalizeRetrievedAt(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Florida contribution export retrieval timestamp");
  }
  return normalized;
}

function normalizeMinIntervalMs(value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`Florida contribution export minIntervalMs must be a nonnegative integer: ${value}`);
  }
  return value;
}

function normalizeTimeoutMs(value: number | undefined): number {
  const normalized = value ?? 30_000;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Florida contribution export timeoutMs must be a positive integer: ${value}`);
  }
  return normalized;
}

function assertFloridaContributionExportTsv(tsv: string): void {
  const firstNonEmptyLine = tsv
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => line.length > 0);
  const lower = firstNonEmptyLine?.toLowerCase() ?? "";
  if (
    !firstNonEmptyLine ||
    /^<\??[a-z!/]/i.test(firstNonEmptyLine) ||
    lower.startsWith("<!doctype") ||
    lower.startsWith("<html") ||
    lower.includes("<body") ||
    lower.includes("</html>")
  ) {
    throw new Error("Florida contribution export returned non-TSV content");
  }
  // contrib.exe can emit a VALID TSV header followed by an HTML error body
  // (seen live: "Overflow Error Number = 6" when rowlimit exceeds ~32767),
  // so the first-line check alone would parse and cache the error page.
  if (/error in \/cgi-bin\/contrib\.exe/i.test(tsv) || /<\/html>/i.test(tsv)) {
    throw new Error("Florida contribution export returned an error page after the TSV header");
  }
}

export function createFloridaContributionExportFetchTransport(
  options: FloridaContributionExportFetchTransportOptions = {}
): FloridaContributionExportTransport {
  const fetchFn = options.fetchFn ?? globalThis.fetch;
  if (typeof fetchFn !== "function") {
    throw new Error("Florida contribution export fetch transport requires global fetch or fetchFn");
  }
  const timeoutMs = normalizeTimeoutMs(options.timeoutMs);
  const userAgent = options.userAgent?.trim() || "VoteApp Florida campaign finance export";

  return async (request) => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    timeout.unref?.();

    try {
      const response = await fetchFn(request.exportUrl, {
        method: "POST",
        headers: {
          accept: "text/tab-separated-values,text/plain,*/*",
          "content-type": "application/x-www-form-urlencoded",
          "user-agent": userAgent,
          referer: request.searchPageUrl,
        },
        body: request.formData.toString(),
        signal: controller.signal,
      });

      if (!response.ok) {
        throw new Error(
          `Florida contribution export failed with HTTP ${response.status} ${response.statusText}`.trim()
        );
      }
      const tsv = await response.text();
      assertFloridaContributionExportTsv(tsv);
      return {
        tsv,
        finalUrl: response.url || request.exportUrl,
        retrievedAt: new Date(),
      };
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        throw new Error(`Florida contribution export timed out after ${timeoutMs}ms`);
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  };
}

export function createFloridaContributionExportRateLimiter(
  options: FloridaContributionExportRateLimiterOptions
): FloridaContributionExportRateLimiter {
  const minIntervalMs = normalizeMinIntervalMs(options.minIntervalMs);
  const now = options.now ?? (() => Date.now());
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  let lastRequestStartedAtMs: number | null = null;

  return async () => {
    const currentTimeMs = now();
    if (!Number.isFinite(currentTimeMs)) {
      throw new Error("Florida contribution export rate limiter clock returned an invalid timestamp");
    }
    if (lastRequestStartedAtMs !== null) {
      const waitMs = minIntervalMs - (currentTimeMs - lastRequestStartedAtMs);
      if (waitMs > 0) {
        await sleep(waitMs);
      }
    }
    const startedAtMs = now();
    if (!Number.isFinite(startedAtMs)) {
      throw new Error("Florida contribution export rate limiter clock returned an invalid timestamp");
    }
    lastRequestStartedAtMs = startedAtMs;
  };
}

export function normalizeFloridaContributionExportQuery(
  input: FloridaContributionExportQuery
): NormalizedFloridaContributionExportQuery {
  const query: NormalizedFloridaContributionExportQuery = {
    searchType: normalizeSearchType(input.searchType),
    electionCode: normalizeOptionalText(input.electionCode),
    candidateFirstName: normalizeOptionalText(input.candidateFirstName),
    candidateLastName: normalizeOptionalText(input.candidateLastName),
    committeeName: normalizeOptionalText(input.committeeName),
    committeeType: normalizeOptionalText(input.committeeType),
    dateFrom: normalizeOptionalText(input.dateFrom),
    dateTo: normalizeOptionalText(input.dateTo),
    rowLimit: normalizeRowLimit(input.rowLimit),
  };

  if (
    query.searchType === "candidate_detail" &&
    (!query.candidateFirstName || !query.candidateLastName)
  ) {
    throw new Error("Florida candidate contribution export requires candidateFirstName and candidateLastName");
  }
  if (query.searchType === "committee_detail" && !query.committeeName) {
    throw new Error("Florida committee contribution export requires committeeName");
  }

  return query;
}

function contributionSearchOnValue(searchType: FloridaContributionSearchType): string {
  return searchType === "candidate_detail" ? "2" : "4";
}

export function buildFloridaContributionExportFormData(
  input: FloridaContributionExportQuery
): URLSearchParams {
  const query = normalizeFloridaContributionExportQuery(input);
  const params = new URLSearchParams();
  params.set("election", query.electionCode ?? "All");
  params.set("search_on", contributionSearchOnValue(query.searchType));
  params.set("CanFName", "");
  params.set("CanLName", "");
  params.set("CanNameSrch", "2");
  params.set("office", "All");
  params.set("cdistrict", "");
  params.set("cgroup", "");
  params.set("party", "All");
  params.set("ComName", "");
  params.set("ComNameSrch", "2");
  params.set("committee", "All");
  params.set("cfname", "");
  params.set("clname", "");
  params.set("namesearch", "2");
  params.set("ccity", "");
  params.set("cstate", "");
  params.set("czipcode", "");
  params.set("coccupation", "");
  params.set("cdollar_minimum", "");
  params.set("cdollar_maximum", "");
  params.set("rowlimit", String(query.rowLimit));
  params.set("csort1", "NAM");
  params.set("csort2", "CAN");
  params.set("cdatefrom", query.dateFrom ?? "");
  params.set("cdateto", query.dateTo ?? "");
  params.set("queryformat", "2");
  if (query.searchType === "candidate_detail") {
    params.set("CanFName", query.candidateFirstName!);
    params.set("CanLName", query.candidateLastName!);
  } else {
    params.set("ComName", query.committeeName!);
    params.set("committee", query.committeeType ?? "All");
  }
  params.set("Submit", "Submit");
  return params;
}

export function floridaContributionExportFormDataObject(
  input: URLSearchParams | FloridaContributionExportQuery
): Record<string, string> {
  const params = input instanceof URLSearchParams ? input : buildFloridaContributionExportFormData(input);
  return Object.fromEntries(params.entries());
}

function slugPart(value: string | null): string {
  const normalized = normalizeFloridaTextKey(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || "all";
}

export function buildFloridaContributionExportCacheKey(input: FloridaContributionExportQuery): string {
  const query = normalizeFloridaContributionExportQuery(input);
  const stablePayload = JSON.stringify(query);
  const hash = createHash("sha256").update(stablePayload).digest("hex").slice(0, 12);
  const subject =
    query.searchType === "candidate_detail"
      ? `${slugPart(query.candidateLastName)}-${slugPart(query.candidateFirstName)}`
      : slugPart(query.committeeName);
  return [
    "fl-contrib",
    query.searchType === "candidate_detail" ? "candidate" : "committee",
    slugPart(query.electionCode),
    subject,
    hash,
  ].join("-");
}

export function buildFloridaContributionExportTransportRequest(
  input: FloridaContributionExportQuery
): FloridaContributionExportTransportRequest {
  const query = normalizeFloridaContributionExportQuery(input);
  const formData = buildFloridaContributionExportFormData(query);
  return {
    query,
    searchPageUrl: FLORIDA_CAMPAIGN_FINANCE_CONTRIBUTION_SEARCH_URL,
    exportUrl: FLORIDA_CAMPAIGN_FINANCE_CONTRIBUTION_EXPORT_URL,
    formData,
    cacheKey: buildFloridaContributionExportCacheKey(query),
  };
}

export async function exportFloridaContributionRows(
  input: FloridaContributionExportRowsInput
): Promise<FloridaContributionExportRowsResult> {
  if (!isFloridaCampaignFinanceBrowserExportEnabled(input.force)) {
    throw new Error("Florida campaign finance browser export is disabled");
  }

  const request = buildFloridaContributionExportTransportRequest(input);
  await input.rateLimiter?.(request);
  const transport = input.transport ?? createFloridaContributionExportFetchTransport();
  const exported = await transport({
    ...request,
    formData: new URLSearchParams(request.formData),
  });
  const retrievedAt = normalizeRetrievedAt(exported.retrievedAt);
  const sourceUrl = exported.finalUrl ?? request.exportUrl;
  const rows = parseFloridaContributionTsv(exported.tsv, {
    electionCode: request.query.electionCode ?? undefined,
    sourceUrl,
  });

  return {
    query: request.query,
    searchPageUrl: request.searchPageUrl,
    exportUrl: request.exportUrl,
    sourceUrl,
    cacheKey: request.cacheKey,
    retrievedAt,
    rowCount: rows.length,
    formData: floridaContributionExportFormDataObject(request.formData),
    tsv: exported.tsv,
    rows,
  };
}
