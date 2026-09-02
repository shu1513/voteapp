// Arkansas CFIS (Civix "Financial Disclosure") API client. Same vendor build
// as New Hampshire's CFS (newHampshireCfsClient.ts); endpoint names and row
// shapes are the Arkansas deployment's, pinned live 2026-08-26. Anonymous
// JSON, no auth, no reCAPTCHA on any endpoint used here.

import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { chmod, mkdir, rm, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const ARKANSAS_CFIS_API_BASE_URL = "https://api-ethics-disclosures.sos.arkansas.gov/api";
export const ARKANSAS_CFIS_PUBLIC_ORIGIN = "https://ethics-disclosures.sos.arkansas.gov";
// Filer views are reached through POST APIs inside the SPA, so published
// snapshots point at the portal home (the api-client home-URL fallback).
export const ARKANSAS_CFIS_PUBLIC_URL = `${ARKANSAS_CFIS_PUBLIC_ORIGIN}/`;

export const ARKANSAS_CFIS_ENDPOINTS = {
  bulkExport: "ExportData/GetExportPublicDownloadData",
  filerRegistrations: "PublicFilerDetails/GetCandidateCommitteDetails",
  transactions: "PublicTransactionDetails/GetTransactionDetails",
  filedReports: "PublicFiledReportAndDownload/GetPublicFilingReport",
  officeLookup: "PublicLookup/GetOfficeSoughtLookup",
  nextElectionYear: "PublicFilerDetails/GetNextElectionYear",
} as const;

// Both CFIS hostnames return NXDOMAIN from some DNS resolvers while public
// resolvers (8.8.8.8 / 1.1.1.1) answer fine — verified 2026-08-26. Fix the
// run host's resolver rather than pinning Azure Front Door IPs (they rotate).
export const ARKANSAS_CFIS_DNS_DEFECT_HINT =
  "Arkansas CFIS hostnames fail on some DNS resolvers (NXDOMAIN) while 8.8.8.8/1.1.1.1 resolve them; " +
  "use a resolver that answers, do not pin Front Door IPs.";

export type ArkansasCfisTransactionTypeCode = "TCON" | "TEXP";

export type ArkansasCfisClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class ArkansasCfisClientError extends Error {
  constructor(
    public readonly code: ArkansasCfisClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "ArkansasCfisClientError";
  }
}

export type ArkansasCfisClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  userAgent?: string;
};

export type ArkansasCfisBulkDownloadResult = {
  url: string;
  requestBody: {
    type: "CSV";
    filingYear: number;
    transactionTypeCode: ArkansasCfisTransactionTypeCode;
  };
  contentType: string | null;
  contentDisposition: string | null;
  responseDate: string | null;
  outputPath: string;
  bytesWritten: number;
  sha256: string;
};

// PublicFilerDetails/GetCandidateCommitteDetails row. One row per per-cycle
// registration; totalRaised/totalSpent/balanceofFunds are server-computed
// cycle cumulatives (plan-arkansas-finance.md hard fact 1).
export type ArkansasFilerRegistrationRow = {
  registrationGuid: string;
  filerEntityId: number;
  filerEntityVersionId: number;
  filerType: string;
  filerTypeCode: string;
  filerStatus: string;
  // Comma-form display name with middle initials and sometimes honorifics
  // ("Richardson, Robert S.", "Sanders, Governor. Sarah H."); the structured
  // firstName/lastName fields carry no middle name (verified 2026-09-02).
  filerName: string | null;
  firstName: string | null;
  lastName: string | null;
  suffix: string | null;
  committeeName: string | null;
  office: string | null;
  officeDistrictName: string | null;
  jurisdictionName: string | null;
  politicalParty: string | null;
  electionYear: number | null;
  filingYear: number | null;
  isPaperFiler: boolean;
  totalRaised: number;
  totalSpent: number;
  balanceOfFunds: number;
};

// PublicTransactionDetails/GetTransactionDetails row. Thinner than NH's
// receipt rows: no report id/version fields, no transaction type, no
// election type (verified 2026-08-26) — amendment discrimination cannot come
// from this endpoint.
export type ArkansasTransactionRow = {
  guid: string;
  filerName: string;
  filerRegistrationGuid: string;
  transactionAmount: number;
  transactionDate: string;
  sourceName: string | null;
  employerName: string | null;
  occupation: string | null;
  transactionSource: string | null;
  reportName: string | null;
  transactionSubTypeDescription: string | null;
  transactionCategory: string | null;
  hasChild: boolean;
};

// PublicFiledReportAndDownload/GetPublicFilingReport row — the amendment
// lineage source (reportVersion / filerReportVersionId).
export type ArkansasFiledReportRow = {
  reportName: string;
  reportType: string | null;
  reportStatus: string | null;
  reportVersion: string;
  filerReportVersionId: number;
  filerReportGuid: string;
  filerRegistrationGuid: string;
  filerEntityId: number;
  filerName: string;
  filerType: string | null;
  officeName: string | null;
  jurisdictionName: string | null;
  startDate: string | null;
  endDate: string | null;
  dueDate: string | null;
  filedDate: string | null;
  isPaperFile: boolean;
};

export type ArkansasOfficeLookupEntry = {
  value: string;
  name: string;
};

export type ArkansasCfisPage<T> = {
  items: T[];
  totalItems: number;
};

export type ArkansasFilerRegistrationSearchInput = {
  pageNumber?: number;
  pageSize?: number;
  // The deployment filters on `filerName` only; omit for a full-registry sweep.
  filerName?: string;
};

// Sort keys the deployment accepts (verified 2026-08-27; anything else is a
// server error). None is unique — heavy ties make multi-page pulls unstable
// (Sanders, 41 pages: 1,789 duplicate rows unsorted, 2,046 sorted by date).
// Complete pulls therefore partition by inclusive `fromDate`/`toDate`
// (MM/DD/YYYY) until each window fits in one page.
export type ArkansasTransactionSortBy =
  | "TransactionDate"
  | "TransactionAmount"
  | "SourceName"
  | "ReportName"
  | "FilerName";

export type ArkansasTransactionSearchInput = {
  pageNumber?: number;
  pageSize?: number;
  filerRegistrationGuid: string;
  transactionTypeCode: ArkansasCfisTransactionTypeCode;
  // Inclusive transaction-date window, MM/DD/YYYY.
  fromDate?: string;
  toDate?: string;
  sortBy?: ArkansasTransactionSortBy;
  sortType?: "asc" | "desc";
};

export type ArkansasFiledReportSearchInput = {
  pageNumber?: number;
  pageSize?: number;
  filerName: string;
};

export const ARKANSAS_CFIS_FETCH_TIMEOUT_MS = 900_000;
const DEFAULT_USER_AGENT = "Mozilla/5.0";
const DEFAULT_PAGE_SIZE = 200;
const MAX_PAGE_SIZE = 1_000;
const MAX_JSON_RESPONSE_BYTES = 32 * 1024 * 1024;
const MAX_CSV_RESPONSE_BYTES = 512 * 1024 * 1024;

function requirePositiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new ArkansasCfisClientError("invalid_request", `Invalid ${label}: ${value}`);
  }
  return value;
}

function requirePageSize(value: number | undefined): number {
  const pageSize = value ?? DEFAULT_PAGE_SIZE;
  requirePositiveInteger(pageSize, "Arkansas CFIS page size");
  if (pageSize > MAX_PAGE_SIZE) {
    throw new ArkansasCfisClientError(
      "invalid_request",
      `Arkansas CFIS page size exceeds ${MAX_PAGE_SIZE}: ${pageSize}`
    );
  }
  return pageSize;
}

function requireFilingYear(value: number): number {
  // CFIS transaction exports start at filing year 2022 (verified 2026-08-26).
  if (!Number.isInteger(value) || value < 2022 || value > 2100) {
    throw new ArkansasCfisClientError("invalid_request", `Invalid Arkansas CFIS filing year: ${value}`);
  }
  return value;
}

function requireGuid(value: string, label: string): string {
  const guid = value.trim().toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(guid)) {
    throw new ArkansasCfisClientError("invalid_request", `Invalid ${label}: ${value}`);
  }
  return guid;
}

function objectValue(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
  }
  return value as Record<string, unknown>;
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
  }
  return value.trim();
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function requiredNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
  }
  return value;
}

function requiredInteger(value: unknown, label: string): number {
  const parsed = requiredNumber(value, label);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
  }
  return parsed;
}

// electionYear/filingYear arrive as strings and can be empty (SFI rows).
function nullableYear(value: unknown, label: string): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") {
    if (!Number.isInteger(value) || value < 1900 || value > 2100) {
      throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
    }
    return value;
  }
  if (typeof value !== "string") {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
  }
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (!/^\d{4}$/.test(trimmed)) {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}: ${value}`);
  }
  return Number(trimmed);
}

function booleanValue(value: unknown): boolean {
  return value === true;
}

function buildUrl(endpoint: string): string {
  if (!/^[A-Za-z][A-Za-z0-9/]+$/.test(endpoint)) {
    throw new ArkansasCfisClientError("invalid_request", `Invalid Arkansas CFIS endpoint: ${endpoint}`);
  }
  return `${ARKANSAS_CFIS_API_BASE_URL}/${endpoint}`;
}

function networkError(endpoint: string, error: unknown): ArkansasCfisClientError {
  const message = error instanceof Error ? error.message : String(error);
  const cause =
    error instanceof Error && error.cause instanceof Error ? ` (${error.cause.message})` : "";
  const combined = `${message}${cause}`;
  const hint = /ENOTFOUND|EAI_AGAIN/i.test(combined) ? ` ${ARKANSAS_CFIS_DNS_DEFECT_HINT}` : "";
  return new ArkansasCfisClientError(
    "network_error",
    `Arkansas CFIS request failed (${endpoint}): ${combined}.${hint}`
  );
}

async function request(input: {
  endpoint: string;
  method: "GET" | "POST";
  body?: Record<string, unknown>;
  expectedContentType: "json" | "csv";
  options?: ArkansasCfisClientOptions;
}): Promise<Response> {
  const fetchImpl = input.options?.fetchImpl ?? fetch;
  const timeoutMs = input.options?.timeoutMs ?? ARKANSAS_CFIS_FETCH_TIMEOUT_MS;
  const userAgent = input.options?.userAgent?.trim() || DEFAULT_USER_AGENT;
  requirePositiveInteger(timeoutMs, "Arkansas CFIS timeout");

  let response: Response;
  try {
    response = await fetchImpl(buildUrl(input.endpoint), {
      method: input.method,
      headers: {
        Accept:
          input.expectedContentType === "json" ? "application/json, text/plain, */*" : "text/csv, */*",
        ...(input.method === "POST" ? { "Content-Type": "application/json" } : {}),
        Origin: ARKANSAS_CFIS_PUBLIC_ORIGIN,
        Referer: `${ARKANSAS_CFIS_PUBLIC_ORIGIN}/`,
        "User-Agent": userAgent,
      },
      ...(input.body !== undefined ? { body: JSON.stringify(input.body) } : {}),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error) {
    throw networkError(input.endpoint, error);
  }

  if (!response.ok) {
    throw new ArkansasCfisClientError(
      "http_error",
      `Arkansas CFIS request returned HTTP ${response.status}: ${input.endpoint}`,
      response.status
    );
  }

  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  const validContentType =
    input.expectedContentType === "json"
      ? contentType.includes("application/json")
      : contentType.includes("text/csv") || contentType.includes("application/octet-stream");
  if (!validContentType) {
    throw new ArkansasCfisClientError(
      "bad_response",
      `Arkansas CFIS ${input.endpoint} returned unexpected content type: ${contentType || "missing"}`
    );
  }

  return response;
}

async function requestJson(input: {
  endpoint: string;
  method: "GET" | "POST";
  body?: Record<string, unknown>;
  options?: ArkansasCfisClientOptions;
}): Promise<Record<string, unknown>> {
  const response = await request({ ...input, expectedContentType: "json" });
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_JSON_RESPONSE_BYTES) {
    throw new ArkansasCfisClientError(
      "bad_response",
      `Arkansas CFIS ${input.endpoint} exceeded ${MAX_JSON_RESPONSE_BYTES} bytes`
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new ArkansasCfisClientError("bad_response", `Arkansas CFIS ${input.endpoint} returned invalid JSON`);
  }
  const envelope = objectValue(parsed, "response envelope");
  if (envelope.succeeded !== true) {
    const error =
      typeof envelope.error === "object" && envelope.error !== null
        ? (envelope.error as Record<string, unknown>).message
        : envelope.error;
    throw new ArkansasCfisClientError(
      "bad_response",
      `Arkansas CFIS ${input.endpoint} failed${typeof error === "string" ? `: ${error}` : ""}`
    );
  }
  return envelope;
}

function parsePage<T>(envelope: Record<string, unknown>, parseItem: (value: unknown) => T): ArkansasCfisPage<T> {
  const data = objectValue(envelope.data, "page data");
  if (!Array.isArray(data.items)) {
    throw new ArkansasCfisClientError("bad_response", "Invalid Arkansas CFIS page items");
  }
  const totalItems = requiredNumber(data.totalItems, "page totalItems");
  if (!Number.isInteger(totalItems) || totalItems < 0) {
    throw new ArkansasCfisClientError("bad_response", "Invalid Arkansas CFIS page totalItems");
  }
  return { items: data.items.map(parseItem), totalItems };
}

function parseFilerRegistrationRow(value: unknown): ArkansasFilerRegistrationRow {
  const row = objectValue(value, "filer-registration row");
  return {
    registrationGuid: requireGuidResponse(row.guid, "filer-registration guid"),
    filerEntityId: requiredInteger(row.filerEntityID, "filer-registration filerEntityID"),
    filerEntityVersionId: requiredInteger(row.filerEntityVersionID, "filer-registration filerEntityVersionID"),
    filerType: requiredString(row.filerType, "filer-registration filerType"),
    filerTypeCode: requiredString(row.filerTypeCode, "filer-registration filerTypeCode"),
    filerStatus: requiredString(row.filerStatus, "filer-registration filerStatus"),
    filerName: nullableString(row.filerName),
    firstName: nullableString(row.firstName),
    lastName: nullableString(row.lastName),
    suffix: nullableString(row.suffix),
    committeeName: nullableString(row.committeeName),
    office: nullableString(row.office),
    officeDistrictName: nullableString(row.officeDistrictName),
    jurisdictionName: nullableString(row.jurisdictionName),
    politicalParty: nullableString(row.politicalParty),
    electionYear: nullableYear(row.electionYear, "filer-registration electionYear"),
    filingYear: nullableYear(row.filingYear, "filer-registration filingYear"),
    isPaperFiler: booleanValue(row.isPaperFiler),
    totalRaised: requiredNumber(row.totalRaised, "filer-registration totalRaised"),
    totalSpent: requiredNumber(row.totalSpent, "filer-registration totalSpent"),
    balanceOfFunds: requiredNumber(row.balanceofFunds, "filer-registration balanceofFunds"),
  };
}

function requireGuidResponse(value: unknown, label: string): string {
  const guid = requiredString(value, label).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(guid)) {
    throw new ArkansasCfisClientError("bad_response", `Invalid Arkansas CFIS ${label}`);
  }
  return guid;
}

function parseTransactionRow(value: unknown): ArkansasTransactionRow {
  const row = objectValue(value, "transaction row");
  return {
    guid: requireGuidResponse(row.guid, "transaction guid"),
    filerName: requiredString(row.filerName, "transaction filerName"),
    filerRegistrationGuid: requireGuidResponse(row.filerRegistrationGuid, "transaction filerRegistrationGuid"),
    transactionAmount: requiredNumber(row.transactionAmount, "transaction transactionAmount"),
    transactionDate: requiredString(row.transactionDate, "transaction transactionDate"),
    sourceName: nullableString(row.sourceName),
    employerName: nullableString(row.employerName),
    occupation: nullableString(row.occupation),
    transactionSource: nullableString(row.transactionSource),
    reportName: nullableString(row.reportName),
    transactionSubTypeDescription: nullableString(row.transactionSubTypeDesc),
    transactionCategory: nullableString(row.transactionCategory),
    hasChild: booleanValue(row.hasChild),
  };
}

function parseFiledReportRow(value: unknown): ArkansasFiledReportRow {
  const row = objectValue(value, "filed-report row");
  return {
    reportName: requiredString(row.reportName, "filed-report reportName"),
    reportType: nullableString(row.reportType),
    reportStatus: nullableString(row.reportStatus),
    reportVersion: requiredString(row.reportVersion, "filed-report reportVersion"),
    filerReportVersionId: requiredInteger(row.filerReportVersionId, "filed-report filerReportVersionId"),
    filerReportGuid: requireGuidResponse(row.filerReportGuid, "filed-report filerReportGuid"),
    filerRegistrationGuid: requireGuidResponse(row.filerRegistrationGuid, "filed-report filerRegistrationGuid"),
    filerEntityId: requiredInteger(row.filerEntityID, "filed-report filerEntityID"),
    filerName: requiredString(row.filerName, "filed-report filerName"),
    filerType: nullableString(row.filerType),
    officeName: nullableString(row.officeName),
    jurisdictionName: nullableString(row.jurisdictionName),
    startDate: nullableString(row.startDate),
    endDate: nullableString(row.endDate),
    dueDate: nullableString(row.dueDate),
    filedDate: nullableString(row.filedDate),
    isPaperFile: booleanValue(row.isPaperFile),
  };
}

export async function getArkansasNextElectionYear(
  options?: ArkansasCfisClientOptions
): Promise<number> {
  const envelope = await requestJson({
    endpoint: ARKANSAS_CFIS_ENDPOINTS.nextElectionYear,
    method: "GET",
    options,
  });
  const year = nullableYear(envelope.data, "next election year");
  if (year === null) {
    throw new ArkansasCfisClientError("bad_response", "Invalid Arkansas CFIS next election year");
  }
  return year;
}

export async function getArkansasOfficeLookup(
  options?: ArkansasCfisClientOptions
): Promise<ArkansasOfficeLookupEntry[]> {
  const envelope = await requestJson({
    endpoint: ARKANSAS_CFIS_ENDPOINTS.officeLookup,
    method: "GET",
    options,
  });
  if (!Array.isArray(envelope.data)) {
    throw new ArkansasCfisClientError("bad_response", "Invalid Arkansas CFIS office-lookup data");
  }
  return envelope.data.map((value) => {
    const row = objectValue(value, "office-lookup row");
    return {
      value: requiredString(row.value, "office-lookup value"),
      name: requiredString(row.name, "office-lookup name"),
    };
  });
}

export async function getArkansasFilerRegistrationPage(
  input: ArkansasFilerRegistrationSearchInput,
  options?: ArkansasCfisClientOptions
): Promise<ArkansasCfisPage<ArkansasFilerRegistrationRow>> {
  const filerName = input.filerName?.trim();
  const envelope = await requestJson({
    endpoint: ARKANSAS_CFIS_ENDPOINTS.filerRegistrations,
    method: "POST",
    body: {
      pageNumber: requirePositiveInteger(input.pageNumber ?? 1, "Arkansas CFIS page number"),
      pageSize: requirePageSize(input.pageSize),
      ...(filerName ? { filerName } : {}),
    },
    options,
  });
  return parsePage(envelope, parseFilerRegistrationRow);
}

const US_DATE_PATTERN = /^(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])\/\d{4}$/;

function requireUsDate(value: string, label: string): string {
  const trimmed = value.trim();
  if (!US_DATE_PATTERN.test(trimmed)) {
    throw new ArkansasCfisClientError("invalid_request", `Invalid ${label} (MM/DD/YYYY): ${value}`);
  }
  return trimmed;
}

export async function getArkansasTransactionPage(
  input: ArkansasTransactionSearchInput,
  options?: ArkansasCfisClientOptions
): Promise<ArkansasCfisPage<ArkansasTransactionRow>> {
  if ((input.fromDate === undefined) !== (input.toDate === undefined)) {
    throw new ArkansasCfisClientError("invalid_request", "Arkansas CFIS date window needs both fromDate and toDate");
  }
  if ((input.sortBy === undefined) !== (input.sortType === undefined)) {
    throw new ArkansasCfisClientError("invalid_request", "Arkansas CFIS sort needs both sortBy and sortType");
  }
  const envelope = await requestJson({
    endpoint: ARKANSAS_CFIS_ENDPOINTS.transactions,
    method: "POST",
    body: {
      pageNumber: requirePositiveInteger(input.pageNumber ?? 1, "Arkansas CFIS page number"),
      pageSize: requirePageSize(input.pageSize),
      filerRegistrationGuid: requireGuid(input.filerRegistrationGuid, "Arkansas CFIS registration guid"),
      transactionTypeCode: input.transactionTypeCode,
      ...(input.fromDate !== undefined && input.toDate !== undefined
        ? {
            fromDate: requireUsDate(input.fromDate, "Arkansas CFIS fromDate"),
            toDate: requireUsDate(input.toDate, "Arkansas CFIS toDate"),
          }
        : {}),
      ...(input.sortBy !== undefined && input.sortType !== undefined
        ? { sortBy: input.sortBy, sortType: input.sortType }
        : {}),
    },
    options,
  });
  return parsePage(envelope, parseTransactionRow);
}

export async function getArkansasFiledReportPage(
  input: ArkansasFiledReportSearchInput,
  options?: ArkansasCfisClientOptions
): Promise<ArkansasCfisPage<ArkansasFiledReportRow>> {
  const filerName = input.filerName.trim();
  if (!filerName) {
    throw new ArkansasCfisClientError("invalid_request", "Arkansas filed-report search requires filerName");
  }
  const envelope = await requestJson({
    endpoint: ARKANSAS_CFIS_ENDPOINTS.filedReports,
    method: "POST",
    body: {
      pageNumber: requirePositiveInteger(input.pageNumber ?? 1, "Arkansas CFIS page number"),
      pageSize: requirePageSize(input.pageSize),
      filerName,
    },
    options,
  });
  return parsePage(envelope, parseFiledReportRow);
}

async function getAllPages<T>(input: {
  pageSize: number;
  getPage: (pageNumber: number) => Promise<ArkansasCfisPage<T>>;
}): Promise<T[]> {
  const items: T[] = [];
  let expectedTotal: number | null = null;
  for (let pageNumber = 1; ; pageNumber += 1) {
    const page = await input.getPage(pageNumber);
    if (expectedTotal === null) {
      expectedTotal = page.totalItems;
    } else if (page.totalItems !== expectedTotal) {
      throw new ArkansasCfisClientError(
        "bad_response",
        `Arkansas CFIS totalItems changed during pagination: ${expectedTotal} -> ${page.totalItems}`
      );
    }
    items.push(...page.items);
    if (items.length >= expectedTotal) {
      if (items.length !== expectedTotal) {
        throw new ArkansasCfisClientError(
          "bad_response",
          `Arkansas CFIS pagination returned ${items.length} rows for totalItems ${expectedTotal}`
        );
      }
      return items;
    }
    if (page.items.length === 0 || page.items.length > input.pageSize) {
      throw new ArkansasCfisClientError("bad_response", "Arkansas CFIS pagination ended inconsistently");
    }
  }
}

export async function getAllArkansasFilerRegistrations(
  input: Omit<ArkansasFilerRegistrationSearchInput, "pageNumber">,
  options?: ArkansasCfisClientOptions
): Promise<ArkansasFilerRegistrationRow[]> {
  const pageSize = requirePageSize(input.pageSize);
  return getAllPages({
    pageSize,
    getPage: (pageNumber) => getArkansasFilerRegistrationPage({ ...input, pageNumber, pageSize }, options),
  });
}

function rejectDuplicateTransactionGuids(rows: readonly ArkansasTransactionRow[], context: string): void {
  const seen = new Set<string>();
  let duplicates = 0;
  for (const row of rows) {
    if (seen.has(row.guid)) duplicates += 1;
    seen.add(row.guid);
  }
  if (duplicates > 0) {
    throw new ArkansasCfisClientError(
      "bad_response",
      `Arkansas CFIS transaction pull returned ${duplicates} duplicate guids (${context}); ` +
        "server paging is unstable, so the pull is incomplete"
    );
  }
}

function usDateToUtc(value: string): number {
  const [month, day, year] = value.split("/").map(Number);
  return Date.UTC(year!, month! - 1, day!);
}

function utcToUsDate(ms: number): string {
  const date = new Date(ms);
  const pad = (n: number): string => String(n).padStart(2, "0");
  return `${pad(date.getUTCMonth() + 1)}/${pad(date.getUTCDate())}/${date.getUTCFullYear()}`;
}

const DAY_MS = 24 * 60 * 60 * 1000;

export type ArkansasTransactionPullInput = Omit<
  ArkansasTransactionSearchInput,
  "pageNumber" | "fromDate" | "toDate" | "sortBy" | "sortType"
>;

// Complete per-registration pull. Server paging is unstable across pages
// (see ArkansasTransactionSortBy), so: find the date bounds with two sorted
// single-row requests, then bisect inclusive date windows until every window
// fits in one page. A single day that still overflows a page falls back to
// multi-page reads. Every level checks child totals against the parent and
// the assembled set rejects duplicate guids — the pull fails closed instead
// of returning silently incomplete rows.
export async function getAllArkansasTransactions(
  input: ArkansasTransactionPullInput,
  options?: ArkansasCfisClientOptions
): Promise<ArkansasTransactionRow[]> {
  const pageSize = requirePageSize(input.pageSize);
  const base = { ...input, pageSize };

  const first = await getArkansasTransactionPage({ ...base, pageNumber: 1 }, options);
  if (first.totalItems <= pageSize) {
    if (first.items.length !== first.totalItems) {
      throw new ArkansasCfisClientError(
        "bad_response",
        `Arkansas CFIS pagination returned ${first.items.length} rows for totalItems ${first.totalItems}`
      );
    }
    rejectDuplicateTransactionGuids(first.items, "single page");
    return first.items;
  }

  const earliest = await getArkansasTransactionPage(
    { ...base, pageNumber: 1, pageSize: 1, sortBy: "TransactionDate", sortType: "asc" },
    options
  );
  const latest = await getArkansasTransactionPage(
    { ...base, pageNumber: 1, pageSize: 1, sortBy: "TransactionDate", sortType: "desc" },
    options
  );
  const earliestDate = earliest.items[0]?.transactionDate;
  const latestDate = latest.items[0]?.transactionDate;
  if (!earliestDate || !latestDate || !US_DATE_PATTERN.test(earliestDate) || !US_DATE_PATTERN.test(latestDate)) {
    throw new ArkansasCfisClientError("bad_response", "Arkansas CFIS transaction date bounds unavailable");
  }

  const pullWindow = async (fromMs: number, toMs: number): Promise<ArkansasTransactionRow[]> => {
    const fromDate = utcToUsDate(fromMs);
    const toDate = utcToUsDate(toMs);
    const page = await getArkansasTransactionPage({ ...base, pageNumber: 1, fromDate, toDate }, options);
    if (page.totalItems <= pageSize) {
      if (page.items.length !== page.totalItems) {
        throw new ArkansasCfisClientError(
          "bad_response",
          `Arkansas CFIS window ${fromDate}-${toDate} returned ${page.items.length} rows for totalItems ${page.totalItems}`
        );
      }
      return page.items;
    }
    if (fromMs >= toMs) {
      // One day overflowing a page: the only remaining option is multi-page
      // reading; duplicate rejection below decides whether it was complete.
      const rows = await getAllPages({
        pageSize,
        getPage: (pageNumber) => getArkansasTransactionPage({ ...base, pageNumber, fromDate, toDate }, options),
      });
      rejectDuplicateTransactionGuids(rows, `single day ${fromDate}`);
      return rows;
    }
    const days = Math.round((toMs - fromMs) / DAY_MS);
    const midMs = fromMs + Math.floor(days / 2) * DAY_MS;
    const left = await pullWindow(fromMs, midMs);
    const right = await pullWindow(midMs + DAY_MS, toMs);
    if (left.length + right.length !== page.totalItems) {
      throw new ArkansasCfisClientError(
        "bad_response",
        `Arkansas CFIS window ${fromDate}-${toDate} split returned ${left.length + right.length} rows for totalItems ${page.totalItems}`
      );
    }
    return [...left, ...right];
  };

  const rows = await pullWindow(usDateToUtc(earliestDate), usDateToUtc(latestDate));
  if (rows.length !== first.totalItems) {
    throw new ArkansasCfisClientError(
      "bad_response",
      `Arkansas CFIS windowed pull returned ${rows.length} rows for totalItems ${first.totalItems}`
    );
  }
  rejectDuplicateTransactionGuids(rows, "windowed pull");
  return rows;
}

export async function getAllArkansasFiledReports(
  input: Omit<ArkansasFiledReportSearchInput, "pageNumber">,
  options?: ArkansasCfisClientOptions
): Promise<ArkansasFiledReportRow[]> {
  const pageSize = requirePageSize(input.pageSize);
  return getAllPages({
    pageSize,
    getPage: (pageNumber) => getArkansasFiledReportPage({ ...input, pageNumber, pageSize }, options),
  });
}

export async function downloadArkansasCfisBulkCsvToFile(
  input: {
    filingYear: number;
    transactionTypeCode: ArkansasCfisTransactionTypeCode;
    outputPath: string;
  },
  options?: ArkansasCfisClientOptions
): Promise<ArkansasCfisBulkDownloadResult> {
  const requestBody = {
    type: "CSV" as const,
    filingYear: requireFilingYear(input.filingYear),
    transactionTypeCode: input.transactionTypeCode,
  };
  const response = await request({
    endpoint: ARKANSAS_CFIS_ENDPOINTS.bulkExport,
    method: "POST",
    body: requestBody,
    expectedContentType: "csv",
    options,
  });
  if (!response.body) {
    throw new ArkansasCfisClientError(
      "bad_response",
      "Arkansas CFIS bulk export response did not include a body"
    );
  }

  const outputPath = resolve(input.outputPath);
  await mkdir(dirname(outputPath), { recursive: true, mode: 0o700 });
  const hash = createHash("sha256");
  let bytesWritten = 0;
  const meter = new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      bytesWritten += chunk.byteLength;
      if (bytesWritten > MAX_CSV_RESPONSE_BYTES) {
        callback(
          new ArkansasCfisClientError(
            "bad_response",
            `Arkansas CFIS ${ARKANSAS_CFIS_ENDPOINTS.bulkExport} exceeded ${MAX_CSV_RESPONSE_BYTES} bytes`
          )
        );
        return;
      }
      hash.update(chunk);
      callback(null, chunk);
    },
  });
  const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
  const timeoutMs = options?.timeoutMs ?? ARKANSAS_CFIS_FETCH_TIMEOUT_MS;
  const bodyTimeout = setTimeout(() => {
    source.destroy(
      new ArkansasCfisClientError(
        "network_error",
        `Arkansas CFIS bulk export body timed out after ${timeoutMs}ms`
      )
    );
  }, timeoutMs);

  try {
    await pipeline(source, meter, createWriteStream(outputPath, { mode: 0o600 }));
    await chmod(outputPath, 0o600);
    const outputStat = await stat(outputPath);
    if (!outputStat.isFile() || outputStat.size === 0 || outputStat.size !== bytesWritten) {
      throw new ArkansasCfisClientError(
        "bad_response",
        `Arkansas CFIS bulk export wrote an invalid artifact: ${bytesWritten} bytes`
      );
    }
  } catch (error) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw error;
  } finally {
    clearTimeout(bodyTimeout);
  }

  return {
    url: buildUrl(ARKANSAS_CFIS_ENDPOINTS.bulkExport),
    requestBody,
    contentType: response.headers.get("content-type"),
    contentDisposition: response.headers.get("content-disposition"),
    responseDate: response.headers.get("date"),
    outputPath,
    bytesWritten,
    sha256: hash.digest("hex"),
  };
}
