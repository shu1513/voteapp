// Read-only client for the Alabama FCPA public portal (Tyler entellitrak app
// at fcpa.alabamavotes.gov). Phase 0 scope: race summaries, committee search,
// bulk-extract catalog/download, filings list, filing-detail pages. See
// plan-alabama-finance.md and backend/docs/alabama-campaign-finance.md.

import { unzipSync } from "fflate";

import { createAlabamaFcpaDispatcher } from "./alabamaFcpaTls.js";

export const ALABAMA_FCPA_BASE_URL = "https://fcpa.alabamavotes.gov";

export type AlabamaFcpaClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class AlabamaFcpaClientError extends Error {
  constructor(
    public readonly code: AlabamaFcpaClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "AlabamaFcpaClientError";
  }
}

export type AlabamaFcpaClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  userAgent?: string;
};

const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_USER_AGENT = "VoteApp Alabama campaign finance research";
const MAX_PAGES = 200;

let sharedDispatcher: ReturnType<typeof createAlabamaFcpaDispatcher> | null = null;

function defaultFetch(url: string, init: RequestInit): Promise<Response> {
  sharedDispatcher ??= createAlabamaFcpaDispatcher();
  return fetch(url, { ...init, dispatcher: sharedDispatcher } as RequestInit);
}

async function requestRaw(
  path: string,
  params: Record<string, string>,
  options: AlabamaFcpaClientOptions
): Promise<Response> {
  const url = new URL(path, ALABAMA_FCPA_BASE_URL);
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
  const fetchImpl = options.fetchImpl ?? defaultFetch;
  let response: Response;
  try {
    response = await fetchImpl(url.toString(), {
      signal: AbortSignal.timeout(options.timeoutMs ?? DEFAULT_TIMEOUT_MS),
      headers: { "user-agent": options.userAgent ?? DEFAULT_USER_AGENT },
      redirect: "follow",
    });
  } catch (error) {
    throw new AlabamaFcpaClientError(
      "network_error",
      `Alabama FCPA request failed: ${url} (${error instanceof Error ? error.message : String(error)})`
    );
  }
  if (!response.ok) {
    throw new AlabamaFcpaClientError(
      "http_error",
      `Alabama FCPA request returned HTTP ${response.status}: ${url}`,
      response.status
    );
  }
  return response;
}

type AlabamaListEnvelope<TRow> = {
  data?: { totalRecords?: number; list?: TRow[] };
  success?: boolean;
};

async function requestListPage<TRow>(
  page: string,
  params: Record<string, string>,
  options: AlabamaFcpaClientOptions
): Promise<{ totalRecords: number; list: TRow[] }> {
  const response = await requestRaw("/page.request.do", { page, ...params }, options);
  let body: AlabamaListEnvelope<TRow>;
  try {
    body = (await response.json()) as AlabamaListEnvelope<TRow>;
  } catch {
    throw new AlabamaFcpaClientError("bad_response", `Alabama FCPA page ${page} did not return JSON`);
  }
  const totalRecords = body.data?.totalRecords;
  const list = body.data?.list;
  if (body.success !== true || typeof totalRecords !== "number" || !Array.isArray(list)) {
    throw new AlabamaFcpaClientError(
      "bad_response",
      `Alabama FCPA page ${page} returned an unexpected envelope`
    );
  }
  return { totalRecords, list };
}

async function requestAllPages<TRow>(
  page: string,
  params: Record<string, string>,
  pageSize: number,
  options: AlabamaFcpaClientOptions
): Promise<TRow[]> {
  const rows: TRow[] = [];
  let expected: number | null = null;
  for (let pageNumber = 1; pageNumber <= MAX_PAGES; pageNumber += 1) {
    const result = await requestListPage<TRow>(
      page,
      { ...params, pageNumber: String(pageNumber), pageSize: String(pageSize) },
      options
    );
    expected ??= result.totalRecords;
    if (result.totalRecords !== expected) {
      throw new AlabamaFcpaClientError(
        "bad_response",
        `Alabama FCPA page ${page} totalRecords changed mid-pagination (${expected} -> ${result.totalRecords})`
      );
    }
    rows.push(...result.list);
    if (rows.length >= expected || result.list.length === 0) break;
  }
  if (expected !== null && rows.length < expected) {
    throw new AlabamaFcpaClientError(
      "bad_response",
      `Alabama FCPA page ${page} returned ${rows.length} of ${expected} rows`
    );
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Race search dropdown id maps

export type AlabamaSelectOption = { id: string; label: string };

export type AlabamaRaceSearchIds = {
  elections: AlabamaSelectOption[];
  offices: AlabamaSelectOption[];
};

function parseSelectOptions(html: string, selectId: string): AlabamaSelectOption[] {
  const selectMatch = new RegExp(`<select[^>]*id="${selectId}"([\\s\\S]*?)</select>`).exec(html);
  if (!selectMatch) {
    throw new AlabamaFcpaClientError("bad_response", `Race search page has no <select id="${selectId}">`);
  }
  const options: AlabamaSelectOption[] = [];
  const optionPattern = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)/g;
  for (let match = optionPattern.exec(selectMatch[1]!); match; match = optionPattern.exec(selectMatch[1]!)) {
    const id = match[1]!.trim();
    const label = match[2]!.trim();
    if (id) options.push({ id, label });
  }
  if (options.length === 0) {
    throw new AlabamaFcpaClientError("bad_response", `Race search <select id="${selectId}"> has no options`);
  }
  return options;
}

export async function getAlabamaRaceSearchIds(
  options: AlabamaFcpaClientOptions = {}
): Promise<AlabamaRaceSearchIds> {
  const response = await requestRaw(
    "/page.request.do",
    { page: "page.acfPublicPoliticalRaceSearch" },
    options
  );
  const html = await response.text();
  return {
    elections: parseSelectOptions(html, "election"),
    offices: parseSelectOptions(html, "office"),
  };
}

// ---------------------------------------------------------------------------
// Political race search (cycle summaries per candidate committee)

export type AlabamaRaceRow = {
  COMMITTEEID: number;
  CANDIDATE: string;
  CANDIDATESTATUS: string | null;
  BEGINNINGFUNDS: number;
  MONETARYCONTRIB: number;
  MONETARYEXP: number;
  NONMONETARYCONTRIB: number;
  OTHERSOURCES: number;
  ENDINGFUNDS: number;
  YEAR: number | null;
};

export async function getAlabamaRaceRows(
  input: { electionId: string; officeId: string; pageSize?: number },
  options: AlabamaFcpaClientOptions = {}
): Promise<AlabamaRaceRow[]> {
  // Never pass the year param: it takes internal option ids with a different
  // year attribution than the extracts. The no-year response is the cycle
  // aggregate (plan-alabama-finance.md, gotcha 1).
  return requestAllPages<AlabamaRaceRow>(
    "com.acf.common.page.politicalracesearchresults",
    {
      sortDirection: "ASC",
      sortBy: "candidate",
      election: input.electionId,
      office: input.officeId,
    },
    input.pageSize ?? 100,
    options
  );
}

// ---------------------------------------------------------------------------
// Principal campaign committee search (internal id <-> FCPA committee number)

export type AlabamaCommitteeSearchRow = {
  id: number;
  committeeId: string;
  candidateFirstName?: string | null;
  candidateMiddleName?: string | null;
  candidateLastName?: string | null;
  city?: string | null;
  committeeStatus?: string | null;
  office?: string | null;
  party?: string | null;
  jurisdiction?: string | null;
  place?: string | null;
  committeeState?: string | null;
  registeredDate?: string | null;
};

export type AlabamaCommitteeSearchCriteria = {
  candidateLastName?: string;
  committeeId?: string;
  officeId?: string;
  statusId?: string;
};

export async function searchAlabamaPrincipalCampaignCommittees(
  criteria: AlabamaCommitteeSearchCriteria,
  options: AlabamaFcpaClientOptions = {}
): Promise<AlabamaCommitteeSearchRow[]> {
  const fields: Array<{ field_key: string; comparison_type: string; comparison_value_1: string }> = [];
  if (criteria.candidateLastName) {
    fields.push({
      field_key: "candidateLastName",
      comparison_type: "contains",
      comparison_value_1: criteria.candidateLastName,
    });
  }
  if (criteria.committeeId) {
    fields.push({ field_key: "committeeId", comparison_type: "equalTo", comparison_value_1: criteria.committeeId });
  }
  if (criteria.officeId) {
    fields.push({ field_key: "office", comparison_type: "equalTo", comparison_value_1: criteria.officeId });
  }
  if (criteria.statusId) {
    fields.push({ field_key: "committeeStatus", comparison_type: "equalTo", comparison_value_1: criteria.statusId });
  }
  if (fields.length === 0) {
    throw new AlabamaFcpaClientError("invalid_request", "Committee search needs at least one criterion");
  }
  // The portal requires the committee-type criterion; 1 = principal campaign
  // committee (candidate committees).
  fields.push({ field_key: "committeeType", comparison_type: "equalTo", comparison_value_1: "1" });
  return requestAllPages<AlabamaCommitteeSearchRow>(
    "com.acf.common.page.committeesearchresults",
    {
      sortDirection: "ASC",
      sortBy: "candidateLastName",
      criteria: JSON.stringify(fields),
    },
    50,
    options
  );
}

// ---------------------------------------------------------------------------
// Bulk-extract catalog + download

export type AlabamaExtractCatalogRow = {
  DATATYPE: string;
  YEAR: number;
  LASTUPDATED: string;
  LASTUPDATEDRAW: string;
  DOWNLOAD: number;
};

export async function getAlabamaExtractCatalog(
  options: AlabamaFcpaClientOptions = {}
): Promise<AlabamaExtractCatalogRow[]> {
  return requestAllPages<AlabamaExtractCatalogRow>(
    "com.acf.common.page.transactiondatadownloadsresults",
    { sortDirection: "ASC", sortBy: "state" },
    100,
    options
  );
}

export type AlabamaExtractDownload = {
  fileName: string;
  csvText: string;
  zipByteCount: number;
};

export async function downloadAlabamaExtract(
  downloadId: number,
  options: AlabamaFcpaClientOptions = {}
): Promise<AlabamaExtractDownload> {
  const response = await requestRaw(
    "/page.request.do",
    { page: "getTransactionData", id: String(downloadId) },
    options
  );
  const bytes = new Uint8Array(await response.arrayBuffer());
  let entries: Record<string, Uint8Array>;
  try {
    entries = unzipSync(bytes);
  } catch {
    throw new AlabamaFcpaClientError("bad_response", `Extract download ${downloadId} is not a zip archive`);
  }
  const names = Object.keys(entries).filter((name) => name.toLowerCase().endsWith(".csv"));
  if (names.length !== 1) {
    throw new AlabamaFcpaClientError(
      "bad_response",
      `Extract download ${downloadId} contains ${names.length} CSV entries; expected exactly 1`
    );
  }
  return {
    fileName: names[0]!,
    csvText: Buffer.from(entries[names[0]!]!).toString("utf8"),
    zipByteCount: bytes.byteLength,
  };
}

// ---------------------------------------------------------------------------
// Committee filings list + filing detail page

export type AlabamaCommitteeFilingRow = {
  ID: number;
  DESCRIPTION: string;
  PERIODBEGIN: string | null;
  PERIODEND: string | null;
  FILEDDATE: string | null;
  AMENDED: string | null;
};

export async function getAlabamaCommitteeFilings(
  internalCommitteeId: number,
  options: AlabamaFcpaClientOptions = {}
): Promise<AlabamaCommitteeFilingRow[]> {
  return requestAllPages<AlabamaCommitteeFilingRow>(
    "com.acf.common.page.committeeelectronicfilingsresults",
    {
      sortDirection: "DESC",
      sortBy: "dueDate",
      committeeId: String(internalCommitteeId),
    },
    50,
    options
  );
}

export async function getAlabamaFilingDetailHtml(
  filingId: number,
  options: AlabamaFcpaClientOptions = {}
): Promise<string> {
  const response = await requestRaw(
    "/page.request.do",
    { page: "page.acfPublicFilingDetail", filingId: String(filingId) },
    options
  );
  const html = await response.text();
  if (html.includes("System Exception")) {
    throw new AlabamaFcpaClientError("bad_response", `Filing detail ${filingId} returned a System Exception page`);
  }
  return html;
}
