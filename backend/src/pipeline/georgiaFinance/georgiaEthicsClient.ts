// HTTP client for the two Georgia ethics-commission systems (georgia_plan.md
// D1/D8, spike results A1–A4). Every route lives here and nowhere else.
//
// Two hosts serve the same endpoint shapes with disjoint vocabularies:
// PeachFile (2025-07-01 onward, nightly extract) and the frozen EFile archive
// (2022–2025-06-30 by report). Source selection is per registration and per
// report — never a global date (A1) — via the report-inventory union below.
//
// Transport rules are pinned from the spike (A4/A7): one request in flight,
// 2 s spacing, pageSize hard cap 100 (the WAF rejects more), sortBy pinned to
// "Transaction Date" (any other value silently returns zero rows), offset
// paging drifts under date-sort ties so transaction fetches dedup by
// transactionId and offer a stability loop that fails closed.

export const GEORGIA_ETHICS_HOSTS = {
  peachfile: "https://api-peachfile.ethics.ga.gov",
  efile_archive: "https://api-recordsearch.ethics.ga.gov",
} as const;

export type GeorgiaEthicsHost = keyof typeof GEORGIA_ETHICS_HOSTS;

export const GEORGIA_ETHICS_RECORDS_SEARCH_URL = "https://ethics.ga.gov/records-search-all/";

export const DEFAULT_GEORGIA_ETHICS_USER_AGENT =
  "VoteApp election research (https://electionssimplified.com)";

// The spike ran ~200 requests at 2 s spacing with zero blocks (A7).
export const DEFAULT_GEORGIA_ETHICS_REQUEST_SPACING_MS = 2_000;

export const DEFAULT_GEORGIA_ETHICS_REQUEST_TIMEOUT_MS = 90_000;

// Hard cap: pageSize > 100 is rejected by the WAF with
// {"message":"Potentially harmful payload detected!"} (F2a).
export const GEORGIA_ETHICS_PAGE_SIZE = 100;

// Sanity ceiling on rows fetched for one filter. The largest per-filer store
// the spike saw was ~3.5k rows; the full archive store is ~2.56M — a filter
// that silently stopped applying (A3) must fail long before that.
export const GEORGIA_ETHICS_MAX_ROWS = 100_000;

// Timed-pending TCON rows carry the zero GUID instead of null in
// filerReportGuid (D8 — the IE endpoint writes null; both mean "no CCDR
// yet"). Grouping must treat both as absent.
export const GEORGIA_ZERO_GUID = "00000000-0000-0000-0000-000000000000";

// --- Pinned per-host vocabularies (D8/A8: every vocabulary is per host) ----

export const GEORGIA_TRANSACTION_STATUS_CODES_BY_HOST = {
  // TFIL disclosed on a filed CCDR; TPEN disclosed on a timed report (in
  // official totals); TAMD amended-current; TPAMD amended while timed-pending.
  peachfile: ["TFIL", "TPEN", "TAMD", "TPAMD"],
  // F filed; A amended-current.
  efile_archive: ["F", "A"],
} as const satisfies Record<GeorgiaEthicsHost, readonly string[]>;

export function isGeorgiaRecognizedTransactionStatus(host: GeorgiaEthicsHost, code: string | null | undefined): boolean {
  if (typeof code !== "string") {
    return false;
  }
  return (GEORGIA_TRANSACTION_STATUS_CODES_BY_HOST[host] as readonly string[]).includes(code.trim());
}

// D5's individuals-only occupation gate keys on the per-host individual code
// (A8): pinning only the PeachFile code would silently drop every archive
// individual contribution from the occupation breakdown.
export const GEORGIA_INDIVIDUAL_SOURCE_TYPE_CODE_BY_HOST = {
  peachfile: "TIND",
  efile_archive: "IND",
} as const satisfies Record<GeorgiaEthicsHost, string>;

// Report-family normalization (D8). Cross-host report identity is
// (registration, family, period start, period end) — NEVER the raw
// reportTypeCode (disjoint code sets) and never report status (version state,
// not identity). Codes population-checked across 30 probed inventories.
export const GEORGIA_REPORT_FAMILY_BY_HOST_CODE = {
  peachfile: {
    FPCFDR: "ccdr",
    FPTBDR: "two_business_day",
    FPICTBDR: "independent_committee_two_business_day",
  },
  efile_archive: {
    "103": "ccdr",
    "104": "two_business_day",
    "107": "independent_committee_two_business_day",
  },
} as const;

export type GeorgiaReportFamily =
  (typeof GEORGIA_REPORT_FAMILY_BY_HOST_CODE)["peachfile"][keyof (typeof GEORGIA_REPORT_FAMILY_BY_HOST_CODE)["peachfile"]];

export function normalizeGeorgiaReportFamily(host: GeorgiaEthicsHost, reportTypeCode: string): GeorgiaReportFamily {
  const familyByCode: Record<string, GeorgiaReportFamily> = GEORGIA_REPORT_FAMILY_BY_HOST_CODE[host];
  const family = familyByCode[reportTypeCode.trim()];
  if (!family) {
    // Fail closed on unknown codes (D8) — an unmapped code must never let the
    // same report survive the inventory union twice.
    throw new GeorgiaEthicsClientError(
      "bad_response",
      `Unknown Georgia ${host} reportTypeCode: ${JSON.stringify(reportTypeCode)}`
    );
  }
  return family;
}

// --- Errors ----------------------------------------------------------------

export type GeorgiaEthicsClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response"
  | "filter_ineffective"
  | "unstable_result";

export class GeorgiaEthicsClientError extends Error {
  constructor(
    public readonly code: GeorgiaEthicsClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "GeorgiaEthicsClientError";
  }
}

// --- Request bodies (pinned byte-for-byte from spike-verified requests) ----

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new GeorgiaEthicsClientError("invalid_request", `${fieldName} is required`);
  }
  return trimmed;
}

function requirePageNumber(value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new GeorgiaEthicsClientError("invalid_request", `Invalid Georgia page number: ${value}`);
  }
  return value;
}

// Filter dates are rendered in each host's own date dialect (spike probes:
// archive accepted ISO, PeachFile accepted MM/DD/YYYY).
export function formatGeorgiaFilterDate(host: GeorgiaEthicsHost, isoDate: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(isoDate.trim());
  if (!match) {
    throw new GeorgiaEthicsClientError("invalid_request", `Invalid Georgia filter date (want YYYY-MM-DD): ${JSON.stringify(isoDate)}`);
  }
  const [, year, month, day] = match;
  return host === "efile_archive" ? `${year}-${month}-${day}` : `${month}/${day}/${year}`;
}

export function buildGeorgiaCandidateIndexRequestBody(input: {
  filerName: string;
  pageNumber: number;
  pageSize?: number;
}): string {
  return JSON.stringify({
    pageNumber: requirePageNumber(input.pageNumber),
    pageSize: input.pageSize ?? GEORGIA_ETHICS_PAGE_SIZE,
    filerTypeCode: "RC",
    filerName: requireNonEmpty(input.filerName, "Georgia candidate index filerName"),
    politicalPartyCode: null,
    OfficeSought: null,
    totalRaisedMax: null,
    totalRaisedMin: null,
    totalSpentMax: null,
    totalSpentMin: null,
    balanceFundsMax: null,
    balanceFundsMin: null,
    accountStatus: null,
    election: null,
    electionCycle: null,
    transactionSourceTypeCode: null,
    treasurerName: null,
    jurisdictionId: null,
    campaignName: null,
    cityDistrictId: null,
    districtTypeId: null,
    jurisdictionIsStateOrIsCounty: null,
  });
}

export function buildGeorgiaFilerReportRequestBody(input: {
  filerName: string;
  pageNumber: number;
  pageSize?: number;
}): string {
  return JSON.stringify({
    pageNumber: requirePageNumber(input.pageNumber),
    pageSize: input.pageSize ?? GEORGIA_ETHICS_PAGE_SIZE,
    filerTypeCode: "",
    officeId: "",
    reportType: "",
    reportStatus: null,
    filedDateFromDate: null,
    filedDateToDate: null,
    reportName: "",
    electionID: "",
    filerName: requireNonEmpty(input.filerName, "Georgia filed-report filerName"),
    campaignCommittee: "",
    districtTypeId: "",
    districtId: "",
    jurisdictionName: "",
    jurisdictionTypeId: "",
    dueDateFromDate: null,
    dueDateToDate: null,
    partyAffiliationCode: "",
    filingCycleId: "",
    reportVersion: "",
  });
}

export type GeorgiaTransactionFilter = {
  // Per-host name form (A3): archive keys candidate filers by person display
  // name, PeachFile by committee name. Substring, case-insensitive.
  filerName: string;
  // ISO dates; rendered per host. Windows bound pagination drift (A4) but
  // must never lose rows — callers finish with an unbounded sweep pass.
  fromDate?: string | null;
  toDate?: string | null;
};

export function buildGeorgiaTransactionRequestBody(
  host: GeorgiaEthicsHost,
  filter: GeorgiaTransactionFilter,
  pageNumber: number
): string {
  return JSON.stringify({
    pageNumber: requirePageNumber(pageNumber),
    pageSize: GEORGIA_ETHICS_PAGE_SIZE,
    // Pinned (A4): any other sortBy silently returns zero rows.
    sortBy: "Transaction Date",
    sortType: "desc",
    transactionTypeCode: "TCON",
    filerName: requireNonEmpty(filter.filerName, "Georgia transaction filerName"),
    sourceName: "",
    transactionAmountMax: null,
    sourceTypeCode: "",
    committeeType: "",
    electionID: "",
    reportName: "",
    toDate: filter.toDate ? formatGeorgiaFilterDate(host, filter.toDate) : null,
    fromDate: filter.fromDate ? formatGeorgiaFilterDate(host, filter.fromDate) : null,
    byState: "",
    electionType: "",
    electionYear: "",
  });
}

// --- Transport -------------------------------------------------------------

export type GeorgiaEthicsHttpResponse = {
  status: number;
  body: string;
};

export type GeorgiaEthicsFetchFn = (url: string, body: string) => Promise<GeorgiaEthicsHttpResponse>;

export type GeorgiaEthicsTransport = {
  postJson: (url: string, body: string) => Promise<unknown>;
};

function defaultGeorgiaEthicsFetch(userAgent: string, timeoutMs: number): GeorgiaEthicsFetchFn {
  return async (url: string, body: string) => {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": userAgent,
        Accept: "application/json",
      },
      body,
      signal: AbortSignal.timeout(timeoutMs),
    });
    return { status: response.status, body: await response.text() };
  };
}

// One request in flight, spacing before every request after the first,
// bounded retries with linear backoff on transient failures only (network,
// 429, 5xx). Non-transient statuses fail immediately.
export function createGeorgiaEthicsTransport(
  options: {
    fetch?: GeorgiaEthicsFetchFn;
    sleep?: (ms: number) => Promise<void>;
    spacingMs?: number;
    maxAttempts?: number;
    retryBackoffMs?: number;
    userAgent?: string;
    timeoutMs?: number;
    log?: (message: string) => void;
  } = {}
): GeorgiaEthicsTransport {
  const spacingMs = options.spacingMs ?? DEFAULT_GEORGIA_ETHICS_REQUEST_SPACING_MS;
  const maxAttempts = options.maxAttempts ?? 3;
  const retryBackoffMs = options.retryBackoffMs ?? 5_000;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const fetchFn =
    options.fetch ??
    defaultGeorgiaEthicsFetch(
      options.userAgent ?? DEFAULT_GEORGIA_ETHICS_USER_AGENT,
      options.timeoutMs ?? DEFAULT_GEORGIA_ETHICS_REQUEST_TIMEOUT_MS
    );

  let queue: Promise<unknown> = Promise.resolve();
  let anyRequestStarted = false;

  return {
    postJson: (url: string, body: string) => {
      const run = queue.then(async () => {
        let lastError: Error | null = null;
        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
          if (anyRequestStarted) {
            await sleep(attempt === 1 ? spacingMs : attempt * retryBackoffMs);
          }
          anyRequestStarted = true;
          try {
            const response = await fetchFn(url, body);
            if (response.status === 200) {
              return parseGeorgiaEthicsEnvelope(url, response.body);
            }
            const failure = new GeorgiaEthicsClientError(
              "http_error",
              `Georgia ethics request failed with HTTP ${response.status}: ${url}`,
              response.status
            );
            if (response.status !== 429 && response.status < 500) {
              throw failure;
            }
            lastError = failure;
          } catch (error) {
            if (error instanceof GeorgiaEthicsClientError && error.code !== "network_error") {
              throw error;
            }
            lastError = error as Error;
          }
          options.log?.(`Georgia ethics attempt ${attempt}/${maxAttempts} failed for ${url}: ${lastError.message}`);
        }
        throw new GeorgiaEthicsClientError(
          "network_error",
          `Georgia ethics request failed after ${maxAttempts} attempts: ${url} — ${lastError?.message}`
        );
      });
      // Later requests wait for this one to settle, success or failure.
      queue = run.catch(() => {});
      return run;
    },
  };
}

// Both hosts wrap responses in {data, succeeded, error}. Fail closed on
// non-JSON, envelope errors, and the WAF's "harmful payload" message.
function parseGeorgiaEthicsEnvelope(url: string, body: string): unknown {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics response is not JSON: ${url}`);
  }
  const envelope = parsed as { data?: unknown; succeeded?: unknown; error?: unknown; message?: unknown };
  if (typeof envelope.message === "string" && /harmful payload/i.test(envelope.message)) {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics WAF rejected the request: ${url}`);
  }
  if (envelope.succeeded === false || (envelope.error !== undefined && envelope.error !== null)) {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics response reported failure: ${url}`);
  }
  if (envelope.data === undefined || envelope.data === null) {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics response has no data: ${url}`);
  }
  return envelope.data;
}

function requireItemsPage(url: string, data: unknown): { items: unknown[] } {
  const page = data as { items?: unknown };
  if (!Array.isArray(page.items)) {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics response has no items array: ${url}`);
  }
  return { items: page.items };
}

// --- Row types (fields the pipeline reads; extra response fields ignored) --

export type GeorgiaCandidateIndexRow = {
  filerEntityId: number;
  filerRegistrationId: number | null;
  guid: string;
  filerName: string;
  committeeName: string | null;
  candidateFirstName: string | null;
  candidateLastName: string | null;
  ballotFullName: string | null;
  office: string | null;
  districtName: string | null;
  filerStatusCode: string | null;
  filingCycleName: string | null;
  electionCycleName: string | null;
  totalContributions: number | null;
  totalExpenditures: number | null;
  cashOnHand: number | null;
};

export type GeorgiaFiledReportVersion = {
  filerReportGuid: string;
  filerReportVersionId: number;
  reportStatus: string | null;
  filedDate: string | null;
  filePath: string | null;
};

export type GeorgiaFiledReportRow = {
  filerReportId: number;
  filerReportGuid: string;
  filerRegistrationGuid: string;
  filerEntityId: number;
  reportTypeCode: string;
  reportName: string | null;
  reportStatus: string | null;
  reportVersionId: number | null;
  startDate: string | null;
  endDate: string | null;
  filedDate: string | null;
  hasChild: boolean;
  childVersions: GeorgiaFiledReportVersion[];
};

export type GeorgiaTransactionRow = {
  guid: string;
  transactionId: number;
  transactionAmount: number;
  filerEntityId: number;
  filerRegistrationGuid: string | null;
  filerReportGuid: string | null;
  timedFiledReportGuid: string | null;
  filerReportId: number | null;
  filerReportVersionId: number | null;
  transactionDate: string | null;
  sourceName: string | null;
  payeeOccupation: string | null;
  payeeEmployer: string | null;
  transactionTypeCode: string | null;
  transactionSubTypeCode: string | null;
  transactionSubTypeDesc: string | null;
  transactionSourceTypeCode: string | null;
  transactionStatusCode: string | null;
  reportName: string | null;
  electionYear: number | null;
};

function asOptionalString(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function asOptionalNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function requireRowNumber(url: string, value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics row is missing ${fieldName}: ${url}`);
  }
  return value;
}

function requireRowString(url: string, value: unknown, fieldName: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new GeorgiaEthicsClientError("bad_response", `Georgia ethics row is missing ${fieldName}: ${url}`);
  }
  return value;
}

function parseCandidateIndexRow(url: string, raw: unknown): GeorgiaCandidateIndexRow {
  const row = raw as Record<string, unknown>;
  return {
    filerEntityId: requireRowNumber(url, row.filerEntityId, "filerEntityId"),
    filerRegistrationId: asOptionalNumber(row.filerRegistrationId),
    guid: requireRowString(url, row.guid, "guid"),
    filerName: requireRowString(url, row.filerName, "filerName"),
    committeeName: asOptionalString(row.committeeName),
    candidateFirstName: asOptionalString(row.candidateFirstName),
    candidateLastName: asOptionalString(row.candidateLastName),
    ballotFullName: asOptionalString(row.ballotFullName),
    office: asOptionalString(row.office),
    districtName: asOptionalString(row.districtName),
    filerStatusCode: asOptionalString(row.filerStatusCode),
    filingCycleName: asOptionalString(row.filingCycleName),
    electionCycleName: asOptionalString(row.electionCycleName),
    totalContributions: asOptionalNumber(row.totalContributions),
    totalExpenditures: asOptionalNumber(row.totalExpenditures),
    cashOnHand: asOptionalNumber(row.cashOnHand),
  };
}

function parseFiledReportRow(url: string, raw: unknown): GeorgiaFiledReportRow {
  const row = raw as Record<string, unknown>;
  const childResults = row.childResults as { items?: unknown } | null | undefined;
  const childItems = Array.isArray(childResults?.items) ? childResults.items : [];
  return {
    filerReportId: requireRowNumber(url, row.filerReportId, "filerReportId"),
    filerReportGuid: requireRowString(url, row.filerReportGuid, "filerReportGuid"),
    filerRegistrationGuid: requireRowString(url, row.filerRegistrationGuid, "filerRegistrationGuid"),
    filerEntityId: requireRowNumber(url, row.filerEntityId, "filerEntityId"),
    reportTypeCode: requireRowString(url, row.reportTypeCode, "reportTypeCode"),
    reportName: asOptionalString(row.reportName),
    reportStatus: asOptionalString(row.reportStatus),
    reportVersionId: asOptionalNumber(row.reportVersionId),
    startDate: asOptionalString(row.startDate),
    endDate: asOptionalString(row.endDate),
    filedDate: asOptionalString(row.filedDate),
    hasChild: row.hasChild === true,
    childVersions: childItems.map((item) => {
      const version = item as Record<string, unknown>;
      return {
        filerReportGuid: requireRowString(url, version.filerReportGuid, "childResults filerReportGuid"),
        filerReportVersionId: requireRowNumber(url, version.filerReportVersionID, "childResults filerReportVersionID"),
        reportStatus: asOptionalString(version.reportStatus),
        filedDate: asOptionalString(version.filedDate),
        filePath: asOptionalString(version.filePath),
      };
    }),
  };
}

function parseTransactionRow(url: string, raw: unknown): GeorgiaTransactionRow {
  const row = raw as Record<string, unknown>;
  return {
    guid: requireRowString(url, row.guid, "guid"),
    transactionId: requireRowNumber(url, row.transactionId, "transactionId"),
    transactionAmount: requireRowNumber(url, row.transactionAmount, "transactionAmount"),
    filerEntityId: requireRowNumber(url, row.filerEntityId, "filerEntityId"),
    filerRegistrationGuid: asOptionalString(row.filerRegistrationGuid),
    filerReportGuid: asOptionalString(row.filerReportGuid),
    timedFiledReportGuid: asOptionalString(row.timedFiledReportGuid),
    filerReportId: asOptionalNumber(row.filerReportId),
    filerReportVersionId: asOptionalNumber(row.filerReportVersionId),
    transactionDate: asOptionalString(row.transactionDate),
    sourceName: asOptionalString(row.sourceName),
    // Archive values are right-padded with spaces (F9) — trim at the source.
    payeeOccupation: asOptionalString(row.payeeOccupation)?.trim() ?? null,
    payeeEmployer: asOptionalString(row.payeeEmployer)?.trim() ?? null,
    transactionTypeCode: asOptionalString(row.transactionTypeCode),
    transactionSubTypeCode: asOptionalString(row.transactionSubTypeCode),
    transactionSubTypeDesc: asOptionalString(row.transactionSubTypeDesc),
    transactionSourceTypeCode: asOptionalString(row.transactionSourceTypeCode),
    transactionStatusCode: asOptionalString(row.transactionStatusCode),
    reportName: asOptionalString(row.reportName),
    electionYear: asOptionalNumber(row.electionYear),
  };
}

// --- Paged fetches ---------------------------------------------------------

function endpointUrl(host: GeorgiaEthicsHost, path: string): string {
  return `${GEORGIA_ETHICS_HOSTS[host]}${path}`;
}

// Pages until a short page. Both hosts accept pageNumber starting at 1;
// totalItems is unreliable across hosts (PeachFile returns totalRows: null on
// the transaction endpoint — F2b), so the short page is the only loop bound,
// with the row ceiling as the runaway backstop.
async function fetchAllPages<Row>(
  transport: GeorgiaEthicsTransport,
  url: string,
  buildBody: (pageNumber: number) => string,
  parseRow: (url: string, raw: unknown) => Row
): Promise<Row[]> {
  const rows: Row[] = [];
  for (let pageNumber = 1; ; pageNumber += 1) {
    const data = await transport.postJson(url, buildBody(pageNumber));
    const { items } = requireItemsPage(url, data);
    if (items.length > GEORGIA_ETHICS_PAGE_SIZE) {
      throw new GeorgiaEthicsClientError(
        "bad_response",
        `Georgia ethics page ${pageNumber} returned ${items.length} rows, above the pinned page size: ${url}`
      );
    }
    for (const item of items) {
      rows.push(parseRow(url, item));
    }
    if (rows.length > GEORGIA_ETHICS_MAX_ROWS) {
      throw new GeorgiaEthicsClientError(
        "bad_response",
        `Georgia ethics fetch exceeded the ${GEORGIA_ETHICS_MAX_ROWS}-row sanity ceiling — ` +
          `the name filter likely stopped applying: ${url}`
      );
    }
    if (items.length < GEORGIA_ETHICS_PAGE_SIZE) {
      return rows;
    }
  }
}

export async function fetchGeorgiaCandidateIndexRows(
  transport: GeorgiaEthicsTransport,
  host: GeorgiaEthicsHost,
  input: { filerName: string }
): Promise<GeorgiaCandidateIndexRow[]> {
  const url = endpointUrl(host, "/api/PublicFilerDetails/GetCandidateDetails");
  return fetchAllPages(
    transport,
    url,
    (pageNumber) => buildGeorgiaCandidateIndexRequestBody({ filerName: input.filerName, pageNumber }),
    parseCandidateIndexRow
  );
}

export async function fetchGeorgiaFiledReportRows(
  transport: GeorgiaEthicsTransport,
  host: GeorgiaEthicsHost,
  input: { filerName: string }
): Promise<GeorgiaFiledReportRow[]> {
  const url = endpointUrl(host, "/api/PublicFiledReportAndDownload/GetFilerReport");
  return fetchAllPages(
    transport,
    url,
    (pageNumber) => buildGeorgiaFilerReportRequestBody({ filerName: input.filerName, pageNumber }),
    parseFiledReportRow
  );
}

export type GeorgiaTransactionFetchResult = {
  rows: GeorgiaTransactionRow[];
  fetchedRowCount: number;
  duplicateRowCount: number;
  foreignRowCount: number;
};

// Single-pass transaction fetch implementing the A4 paging rules:
// pinned sortBy, page-until-short-page, dedup by transactionId (offset
// paging drifts under date-sort ties), and the A3 filter-effectiveness
// check — id filter params are silently ignored by both hosts, so rows are
// post-filtered by filerEntityId and a result where the name filter matched
// only foreign filers is a hard error, because an ineffective filter looks
// identical to a small result set.
export async function fetchGeorgiaTransactionRows(
  transport: GeorgiaEthicsTransport,
  host: GeorgiaEthicsHost,
  filter: GeorgiaTransactionFilter,
  input: { expectedFilerEntityIds: readonly number[] }
): Promise<GeorgiaTransactionFetchResult> {
  if (input.expectedFilerEntityIds.length === 0) {
    throw new GeorgiaEthicsClientError("invalid_request", "Georgia transaction fetch needs expected filer entity ids");
  }
  const url = endpointUrl(host, "/api/PublicTransactionDetails/GetTransactionDetails");
  const fetched = await fetchAllPages(
    transport,
    url,
    (pageNumber) => buildGeorgiaTransactionRequestBody(host, filter, pageNumber),
    parseTransactionRow
  );

  const expected = new Set(input.expectedFilerEntityIds);
  const seenTransactionIds = new Set<number>();
  const rows: GeorgiaTransactionRow[] = [];
  let duplicateRowCount = 0;
  let foreignRowCount = 0;
  for (const row of fetched) {
    if (!expected.has(row.filerEntityId)) {
      foreignRowCount += 1;
      continue;
    }
    if (seenTransactionIds.has(row.transactionId)) {
      duplicateRowCount += 1;
      continue;
    }
    seenTransactionIds.add(row.transactionId);
    rows.push(row);
  }

  if (rows.length === 0 && foreignRowCount > 0) {
    throw new GeorgiaEthicsClientError(
      "filter_ineffective",
      `Georgia ${host} transaction filter ${JSON.stringify(filter.filerName)} returned ${foreignRowCount} rows, ` +
        "none for the expected filer — the filter had no effect or matched the wrong filer"
    );
  }

  return { rows, fetchedRowCount: fetched.length, duplicateRowCount, foreignRowCount };
}

export type GeorgiaStableTransactionFetchResult = GeorgiaTransactionFetchResult & {
  passCount: number;
};

// Bounded-retry stability loop (A4 residual risk): date filters cannot
// subdivide below one day, so a deadline day with more than one page of tied
// rows rides on dedup alone. Re-pull until the unique transaction-id set is
// identical across two consecutive passes; a window that never stabilizes
// fails closed rather than caching a row set that silently lost rows.
export async function fetchGeorgiaTransactionRowsStable(
  transport: GeorgiaEthicsTransport,
  host: GeorgiaEthicsHost,
  filter: GeorgiaTransactionFilter,
  input: { expectedFilerEntityIds: readonly number[]; maxPasses?: number }
): Promise<GeorgiaStableTransactionFetchResult> {
  const maxPasses = input.maxPasses ?? 4;
  if (!Number.isInteger(maxPasses) || maxPasses < 2) {
    throw new GeorgiaEthicsClientError("invalid_request", `Georgia stability passes must be at least 2: ${maxPasses}`);
  }

  let previousIds: Set<number> | null = null;
  let lastResult: GeorgiaTransactionFetchResult | null = null;
  for (let pass = 1; pass <= maxPasses; pass += 1) {
    const result = await fetchGeorgiaTransactionRows(transport, host, filter, input);
    const ids = new Set(result.rows.map((row) => row.transactionId));
    if (previousIds && ids.size === previousIds.size && [...ids].every((id) => previousIds!.has(id))) {
      return { ...result, passCount: pass };
    }
    previousIds = ids;
    lastResult = result;
  }
  throw new GeorgiaEthicsClientError(
    "unstable_result",
    `Georgia ${host} transaction fetch for ${JSON.stringify(filter.filerName)} did not stabilize in ${maxPasses} passes ` +
      `(last pass: ${lastResult?.rows.length ?? 0} unique rows, ${lastResult?.duplicateRowCount ?? 0} duplicates)`
  );
}

// --- Timed-report grouping (D8) -------------------------------------------

// Report membership comes from report GUIDs, never from transaction dates
// (the store holds garbage dates on valid rows — A4). Timed-pending rows
// carry no real filerReportGuid: the TCON endpoint writes the zero GUID, the
// IE endpoint writes null — both group by timedFiledReportGuid.
export function georgiaTransactionReportGroupGuid(row: {
  filerReportGuid: string | null;
  timedFiledReportGuid: string | null;
}): string | null {
  const reportGuid = row.filerReportGuid?.trim().toLowerCase();
  if (reportGuid && reportGuid !== GEORGIA_ZERO_GUID) {
    return reportGuid;
  }
  const timedGuid = row.timedFiledReportGuid?.trim().toLowerCase();
  return timedGuid || null;
}

// --- Report-inventory union (D8 source selection) --------------------------

export type GeorgiaReportInventoryEntry = {
  source: GeorgiaEthicsHost;
  family: GeorgiaReportFamily;
  // Null on old archive filings whose inventory rows carry no period (seen on
  // 2022-era archive reports) — those entries stand alone and never merge.
  periodStart: string | null;
  periodEnd: string | null;
  report: GeorgiaFiledReportRow;
  // Set when both hosts hold the report and PeachFile won the union — the
  // archive copy is kept for reconciliation diagnostics (index totals track
  // the archive store for pre-cutover reports; A6/D4 drift note).
  supersededArchiveReport?: GeorgiaFiledReportRow;
};

function reportPeriodDate(report: GeorgiaFiledReportRow, field: "startDate" | "endDate"): string | null {
  const value = report[field];
  const match = value ? /^(\d{4}-\d{2}-\d{2})/.exec(value.trim()) : null;
  return match ? match[1]! : null;
}

// Builds the per-registration-chain report inventory as the union of both
// hosts' filed-report rows (A1: the date partition is dead — PeachFile
// migrated some filers' full pre-cutover history, other filers are
// archive-only, and the archive accepted special-election reports into late
// 2025). Callers pass rows already scoped to ONE registration chain via the
// D3 identity map. The cross-host match key is (family, period start, period
// end); where both hosts hold the same report PeachFile wins — it carries
// current amendment state. Reports with no recorded period (2022-era archive
// rows) cannot be identity-matched, so each stands alone under its own
// report guid. Duplicate full-period keys within one host fail closed: the
// cross-host merge would be ambiguous, and every probed inventory shows the
// full-period key unique per registration and host.
export function buildGeorgiaReportInventory(input: {
  peachfileReports: readonly GeorgiaFiledReportRow[];
  archiveReports: readonly GeorgiaFiledReportRow[];
}): GeorgiaReportInventoryEntry[] {
  const entries = new Map<string, GeorgiaReportInventoryEntry>();
  const standalone: GeorgiaReportInventoryEntry[] = [];

  function addReports(source: GeorgiaEthicsHost, reports: readonly GeorgiaFiledReportRow[]): void {
    for (const report of reports) {
      const family = normalizeGeorgiaReportFamily(source, report.reportTypeCode);
      const periodStart = reportPeriodDate(report, "startDate");
      const periodEnd = reportPeriodDate(report, "endDate");
      if (periodStart === null || periodEnd === null) {
        standalone.push({ source, family, periodStart, periodEnd, report });
        continue;
      }
      const key = `${family}|${periodStart}|${periodEnd}`;
      const existing = entries.get(key);
      if (!existing) {
        entries.set(key, { source, family, periodStart, periodEnd, report });
        continue;
      }
      if (existing.source === source) {
        throw new GeorgiaEthicsClientError(
          "bad_response",
          `Georgia ${source} inventory holds two reports with the same identity key ${key} ` +
            `(${existing.report.filerReportGuid} and ${report.filerReportGuid})`
        );
      }
      // Cross-host duplicate: PeachFile wins regardless of insertion order.
      if (source === "peachfile") {
        entries.set(key, {
          source,
          family,
          periodStart,
          periodEnd,
          report,
          supersededArchiveReport: existing.report,
        });
      } else {
        entries.set(key, { ...existing, supersededArchiveReport: report });
      }
    }
  }

  addReports("peachfile", input.peachfileReports);
  addReports("efile_archive", input.archiveReports);

  return [...entries.values(), ...standalone].sort((a, b) => {
    const aStart = a.periodStart ?? "";
    const bStart = b.periodStart ?? "";
    if (aStart !== bStart) {
      return aStart < bStart ? -1 : 1;
    }
    const aEnd = a.periodEnd ?? "";
    const bEnd = b.periodEnd ?? "";
    if (aEnd !== bEnd) {
      return aEnd < bEnd ? -1 : 1;
    }
    if (a.family !== b.family) {
      return a.family.localeCompare(b.family);
    }
    return a.report.filerReportGuid.localeCompare(b.report.filerReportGuid);
  });
}
