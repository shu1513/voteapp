// City of Austin campaign-finance client over the city's Socrata portal
// (plan-austin-finance.md). The City Clerk publishes every schedule of the
// e-filing system as datasets on data.austintexas.gov; this module reads the
// four the module needs through narrow SoQL queries (filtered $where, stable
// $order, bounded paging) — never a bulk download.
//
// Modeled on newYorkSodaClient.ts (same Socrata conventions). Typed rows drop
// every address/zip/geometry field at the mapping boundary so PII cannot reach
// logs, fixtures, or tables. Amounts arrive as decimal strings and are parsed
// to integer cents exactly — a value with more than two decimals or a
// non-numeric shape is a bad response, not a rounding case.

export const AUSTIN_SOCRATA_BASE_URL = "https://data.austintexas.gov/resource";
export const AUSTIN_SOCRATA_DATASET_PAGE_BASE_URL = "https://data.austintexas.gov/d";
export const AUSTIN_SOCRATA_REPORT_DETAIL_DATASET = "b2pc-2s8n";
export const AUSTIN_SOCRATA_CONTRIBUTIONS_DATASET = "3kfv-biw6";
export const AUSTIN_SOCRATA_DIRECT_CAMPAIGN_EXPENDITURES_DATASET = "8p2b-ewep";
export const AUSTIN_SOCRATA_COMMITTEE_PURPOSE_DATASET = "u3cd-iecr";
export const AUSTIN_SOCRATA_DEFAULT_TIMEOUT_MS = 30_000;
export const AUSTIN_SOCRATA_DEFAULT_PAGE_LIMIT = 1_000;
export const AUSTIN_SOCRATA_MAX_PAGE_LIMIT = 50_000;
export const AUSTIN_SOCRATA_DEFAULT_MAX_PAGES = 25;

export type AustinSocrataClientErrorCode = "invalid_request" | "network_error" | "http_error" | "bad_response";

export class AustinSocrataClientError extends Error {
  constructor(
    public readonly code: AustinSocrataClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "AustinSocrataClientError";
  }
}

export type AustinSocrataClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  appToken?: string;
  pageLimit?: number;
  maxPages?: number;
};

// Socrata rate-limits unauthenticated calls by shared IP pool; production
// should set AUSTIN_SOCRATA_APP_TOKEN (free, not a user credential).
export function defaultAustinSocrataClientOptions(): AustinSocrataClientOptions {
  const appToken = process.env.AUSTIN_SOCRATA_APP_TOKEN?.trim();
  return appToken ? { appToken } : {};
}

/** Report Detail (`b2pc-2s8n`): one row per filed report with cover totals. */
export type AustinReportDetailRow = {
  reportId: string;
  /** Null on ~170 live rows (mostly committee filers); a caller keying by filer must skip those. */
  filerName: string | null;
  /** Leading code of `form_type`, e.g. `COH`, `CORCOH`, `COHATX7`, `GPAC`, `SPAC`. */
  formTypeCode: string;
  formType: string;
  reportType: string | null;
  dateFiled: string;
  periodFrom: string | null;
  periodTo: string | null;
  electionDate: string | null;
  electionType: string | null;
  officeSought: string | null;
  officeHeld: string | null;
  contribTotalCents: number | null;
  expendTotalCents: number | null;
  contribBalanceCents: number | null;
  outstandingLoanCents: number | null;
  reportUrl: string | null;
};

/** Contributions (`3kfv-biw6`): itemized contributions/pledges to any filer. */
export type AustinContributionRow = {
  transactionId: string;
  /** Report the row was filed on — the `R…` prefix of `transaction_id`. */
  reportId: string;
  recipient: string;
  donor: string;
  donorType: string | null;
  contributionType: string | null;
  amountCents: number;
  contributionDate: string | null;
  occupation: string | null;
  employer: string | null;
  reportFiled: string | null;
  correction: boolean;
  reportUrl: string | null;
};

/** Direct Campaign Expenditures (`8p2b-ewep`): outside spending naming a target. */
export type AustinDirectCampaignExpenditureRow = {
  dceId: string;
  parentTransaction: string;
  /** Report the row was filed on — the `R…` prefix of `parent_transaction`. */
  reportId: string;
  /** Spender as reported; one live row (2024, "Vela, Chito") has none — callers must quarantine such rows. */
  paidBy: string | null;
  payee: string | null;
  paymentDate: string | null;
  amountCents: number;
  candidateOrMeasure: string;
  officeSoughtInfo: string | null;
  officeHeldInfo: string | null;
  correction: boolean;
  reportUrl: string | null;
};

/** Committee Purpose (`u3cd-iecr`): a committee's declared SUPPORT/OPPOSE/ASSIST per report. */
export type AustinCommitteePurposeRow = {
  /** `committee_purp_id` — `<report_id>-C0000n`; the bare `purpose_id` repeats across reports. */
  committeePurposeId: string;
  reportId: string | null;
  /** Null on ~45 live rows; such rows cannot be attributed to a spender. */
  filerName: string | null;
  committeeActivity: string;
  purposeType: string;
  recipient: string | null;
  officeSought: string | null;
  officeHeld: string | null;
  electionDate: string | null;
  measureDescription: string | null;
  correction: boolean;
  reportUrl: string | null;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function normalizePageLimit(value: number | undefined): number {
  const normalized = value ?? AUSTIN_SOCRATA_DEFAULT_PAGE_LIMIT;
  if (!Number.isInteger(normalized) || normalized <= 0 || normalized > AUSTIN_SOCRATA_MAX_PAGE_LIMIT) {
    throw new AustinSocrataClientError(
      "invalid_request",
      `Austin Socrata page limit must be an integer between 1 and ${AUSTIN_SOCRATA_MAX_PAGE_LIMIT}`
    );
  }
  return normalized;
}

function normalizeMaxPages(value: number | undefined): number {
  const normalized = value ?? AUSTIN_SOCRATA_DEFAULT_MAX_PAGES;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new AustinSocrataClientError("invalid_request", "Austin Socrata maxPages must be a positive integer");
  }
  return normalized;
}

// SoQL string literals escape single quotes by doubling them.
export function soqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new AustinSocrataClientError("invalid_request", `Austin Socrata ${fieldName} must not be empty`);
  }
  return trimmed;
}

/** `YYYY-MM-DD` only — the value is interpolated into a SoQL literal. */
export function requireIsoDate(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    throw new AustinSocrataClientError("invalid_request", `Austin Socrata ${fieldName} must be YYYY-MM-DD, got ${value}`);
  }
  return trimmed;
}

/** Socrata floating timestamps look like `2024-01-19T00:00:00.000`; keep the date part. */
function toIsoDate(value: unknown, fieldName: string): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${fieldName} is not a string`);
  }
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (!/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?)?$/.test(trimmed)) {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${fieldName} is not a date: ${trimmed}`);
  }
  return trimmed.slice(0, 10);
}

function getString(row: Record<string, unknown>, key: string): string | null {
  const value = row[key];
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${key} is not a string`);
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function requireString(row: Record<string, unknown>, key: string): string {
  const value = getString(row, key);
  if (value === null) {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${key} is missing or empty`);
  }
  return value;
}

/**
 * Socrata serialises numbers as decimal strings (`"710580.84"`). Parse them
 * exactly: sign, digits, at most two decimals. Anything else is a bad response
 * — never `Number(value) * 100`.
 */
export function parseAustinMoneyCents(value: unknown, fieldName: string): number | null {
  if (value === undefined || value === null) return null;
  const text = typeof value === "number" ? String(value) : typeof value === "string" ? value.trim() : "";
  if (!text) return null;
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(text);
  if (!match) {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${fieldName} is not a money value: ${text}`);
  }
  const [, sign, whole, fraction = ""] = match;
  const cents = Number(whole) * 100 + Number(fraction.padEnd(2, "0"));
  if (!Number.isSafeInteger(cents)) {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${fieldName} is out of range: ${text}`);
  }
  return sign === "-" ? -cents : cents;
}

function requireMoneyCents(row: Record<string, unknown>, key: string): number {
  const cents = parseAustinMoneyCents(row[key], key);
  if (cents === null) {
    throw new AustinSocrataClientError("bad_response", `Austin Socrata ${key} is missing`);
  }
  return cents;
}

/** Socrata URL columns arrive as `{ url, description }` objects. */
function getUrl(row: Record<string, unknown>, key: string): string | null {
  const value = row[key];
  if (value === undefined || value === null) return null;
  if (isRecord(value)) {
    const url = value.url;
    return typeof url === "string" && url.trim() ? url.trim() : null;
  }
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

/** Correction flags are `"X"` when set and absent otherwise. */
function getFlag(row: Record<string, unknown>, key: string): boolean {
  return getString(row, key) !== null;
}

/** Every transaction/parent id is `<report_id>-<suffix>`; the report id is the `R…` prefix. */
export function austinReportIdFromTransactionId(transactionId: string): string {
  const match = /^(R\d+)-/.exec(transactionId.trim());
  if (!match) {
    throw new AustinSocrataClientError("bad_response", `Austin transaction id has no report prefix: ${transactionId}`);
  }
  return match[1]!;
}

/** `"COH - Candidate /Officeholder Campaign Finance Report"` → `"COH"`. */
export function austinFormTypeCode(formType: string): string {
  const code = formType.split(" - ")[0]?.trim() ?? "";
  if (!/^[A-Z0-9]+$/.test(code)) {
    throw new AustinSocrataClientError("bad_response", `Austin form_type has no leading code: ${formType}`);
  }
  return code;
}

export function buildAustinSocrataDatasetUrl(
  datasetId: string,
  params: Record<string, string | number | undefined>
): string {
  if (!/^[a-z0-9]{4}-[a-z0-9]{4}$/i.test(datasetId)) {
    throw new AustinSocrataClientError("invalid_request", `Invalid Austin Socrata dataset ID: ${datasetId}`);
  }
  const url = new URL(`${AUSTIN_SOCRATA_BASE_URL}/${datasetId}.json`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

async function fetchAustinSocrataJson(url: string, options: AustinSocrataClientOptions): Promise<unknown> {
  const timeoutMs = options.timeoutMs ?? AUSTIN_SOCRATA_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers({ accept: "application/json" });
  if (options.appToken?.trim()) {
    headers.set("X-App-Token", options.appToken.trim());
  }
  // The timeout covers the body read too; it is cleared only after
  // response.json() settles.
  try {
    let response: Response;
    try {
      response = await (options.fetchImpl ?? fetch)(url, { headers, signal: controller.signal });
    } catch (error) {
      if (isAbortError(error)) {
        throw new AustinSocrataClientError("network_error", `Austin Socrata request timed out after ${timeoutMs}ms for ${url}`);
      }
      throw new AustinSocrataClientError(
        "network_error",
        `Austin Socrata request failed for ${url}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
    if (!response.ok) {
      throw new AustinSocrataClientError(
        "http_error",
        `Austin Socrata request failed: ${response.status} ${response.statusText}`,
        response.status
      );
    }
    try {
      return await response.json();
    } catch (error) {
      if (isAbortError(error)) {
        throw new AustinSocrataClientError("network_error", `Austin Socrata request timed out after ${timeoutMs}ms for ${url}`);
      }
      throw new AustinSocrataClientError(
        "bad_response",
        `Austin Socrata response was not valid JSON: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAustinSocrataRows(
  datasetId: string,
  params: Record<string, string | number | undefined>,
  options: AustinSocrataClientOptions
): Promise<Record<string, unknown>[]> {
  const payload = await fetchAustinSocrataJson(buildAustinSocrataDatasetUrl(datasetId, params), options);
  if (!Array.isArray(payload)) {
    throw new AustinSocrataClientError("bad_response", "Austin Socrata response is missing the result array");
  }
  if (!payload.every(isRecord)) {
    throw new AustinSocrataClientError("bad_response", "Austin Socrata response contains a non-object row");
  }
  return payload;
}

// Socrata paging requires a stable $order; callers must pass one.
async function fetchAustinSocrataPagedRows(
  datasetId: string,
  params: Record<string, string | number | undefined> & { $order: string },
  options: AustinSocrataClientOptions
): Promise<Record<string, unknown>[]> {
  const limit = normalizePageLimit(options.pageLimit);
  const maxPages = normalizeMaxPages(options.maxPages);
  const rows: Record<string, unknown>[] = [];
  for (let pageIndex = 0; pageIndex < maxPages; pageIndex += 1) {
    const page = await fetchAustinSocrataRows(datasetId, { ...params, $limit: limit, $offset: pageIndex * limit }, options);
    rows.push(...page);
    if (page.length < limit) {
      return rows;
    }
  }
  throw new AustinSocrataClientError(
    "bad_response",
    `Austin Socrata paged query exceeded ${maxPages} pages for dataset ${datasetId}`
  );
}

export function austinReportDetailRowFromRecord(row: Record<string, unknown>): AustinReportDetailRow {
  const formType = requireString(row, "form_type");
  const dateFiled = toIsoDate(row.date_filed, "date_filed");
  if (dateFiled === null) {
    throw new AustinSocrataClientError("bad_response", "Austin Socrata date_filed is missing");
  }
  return {
    reportId: requireString(row, "report_id"),
    filerName: getString(row, "filer_name"),
    formTypeCode: austinFormTypeCode(formType),
    formType,
    reportType: getString(row, "report_type"),
    dateFiled,
    periodFrom: toIsoDate(row.period_from, "period_from"),
    periodTo: toIsoDate(row.period_to, "period_to"),
    electionDate: toIsoDate(row.election_date, "election_date"),
    electionType: getString(row, "election_type"),
    officeSought: getString(row, "office_sought"),
    officeHeld: getString(row, "office_held"),
    contribTotalCents: parseAustinMoneyCents(row.contrib_total, "contrib_total"),
    expendTotalCents: parseAustinMoneyCents(row.expend_total, "expend_total"),
    contribBalanceCents: parseAustinMoneyCents(row.contrib_balance, "contrib_balance"),
    outstandingLoanCents: parseAustinMoneyCents(row.outstand_loan, "outstand_loan"),
    reportUrl: getUrl(row, "link_to_report"),
  };
}

export function austinContributionRowFromRecord(row: Record<string, unknown>): AustinContributionRow {
  const transactionId = requireString(row, "transaction_id");
  return {
    transactionId,
    reportId: austinReportIdFromTransactionId(transactionId),
    recipient: requireString(row, "recipient"),
    donor: requireString(row, "donor"),
    donorType: getString(row, "donor_type"),
    contributionType: getString(row, "contribution_type"),
    amountCents: requireMoneyCents(row, "contribution_amount"),
    contributionDate: toIsoDate(row.contribution_date, "contribution_date"),
    occupation: getString(row, "donor_reported_occupation"),
    employer: getString(row, "donor_reported_employer"),
    reportFiled: getString(row, "report_filed"),
    correction: getFlag(row, "correction"),
    reportUrl: getUrl(row, "view_report"),
  };
}

export function austinDirectCampaignExpenditureRowFromRecord(
  row: Record<string, unknown>
): AustinDirectCampaignExpenditureRow {
  const parentTransaction = requireString(row, "parent_transaction");
  return {
    dceId: requireString(row, "dce_id"),
    parentTransaction,
    reportId: austinReportIdFromTransactionId(parentTransaction),
    paidBy: getString(row, "paid_by"),
    payee: getString(row, "payee"),
    paymentDate: toIsoDate(row.payment_date, "payment_date"),
    amountCents: requireMoneyCents(row, "payment_amount"),
    candidateOrMeasure: requireString(row, "candidate_or_measure"),
    officeSoughtInfo: getString(row, "office_sought_info"),
    officeHeldInfo: getString(row, "office_held_info"),
    correction: getFlag(row, "correction"),
    reportUrl: getUrl(row, "view_report"),
  };
}

export function austinCommitteePurposeRowFromRecord(row: Record<string, unknown>): AustinCommitteePurposeRow {
  return {
    committeePurposeId: requireString(row, "committee_purp_id"),
    reportId: getString(row, "report"),
    filerName: getString(row, "filer_name"),
    committeeActivity: requireString(row, "committee_activity"),
    purposeType: requireString(row, "purpose_type"),
    recipient: getString(row, "recipient"),
    officeSought: getString(row, "office_sought"),
    officeHeld: getString(row, "office_held"),
    electionDate: toIsoDate(row.election_date, "election_date"),
    measureDescription: getString(row, "measure_description"),
    correction: getFlag(row, "cor_flag"),
    reportUrl: getUrl(row, "link_to_report"),
  };
}

/** All Report Detail rows for one filer name (exact match on `filer_name`). */
export async function getAustinReportDetailRowsByFiler(
  filerName: string,
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions()
): Promise<AustinReportDetailRow[]> {
  const rows = await fetchAustinSocrataPagedRows(
    AUSTIN_SOCRATA_REPORT_DETAIL_DATASET,
    { $where: `filer_name = ${soqlString(requireNonEmpty(filerName, "filer name"))}`, $order: "report_id, date_filed" },
    options
  );
  return rows.map(austinReportDetailRowFromRecord);
}

/** Report Detail rows tagged with one election date, optionally narrowed to form-type codes. */
export async function getAustinReportDetailRowsByElection(
  input: { electionDate: string; formTypeCodes?: readonly string[] },
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions()
): Promise<AustinReportDetailRow[]> {
  const electionDate = requireIsoDate(input.electionDate, "election date");
  const rows = await fetchAustinSocrataPagedRows(
    AUSTIN_SOCRATA_REPORT_DETAIL_DATASET,
    { $where: `election_date = ${soqlString(`${electionDate}T00:00:00.000`)}`, $order: "report_id, date_filed" },
    options
  );
  const typed = rows.map(austinReportDetailRowFromRecord);
  if (!input.formTypeCodes) return typed;
  const codes = new Set(input.formTypeCodes);
  return typed.filter((row) => codes.has(row.formTypeCode));
}

/** Total rows vs distinct report ids — Report Detail carries exact duplicate rows. */
export async function getAustinReportDetailRowCounts(
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions()
): Promise<{ totalRows: number; distinctReportIds: number }> {
  const rows = await fetchAustinSocrataRows(
    AUSTIN_SOCRATA_REPORT_DETAIL_DATASET,
    { $select: "count(*) as total_rows, count(distinct report_id) as distinct_report_ids" },
    options
  );
  const row = rows[0];
  if (rows.length !== 1 || !row) {
    throw new AustinSocrataClientError("bad_response", "Austin Socrata count query did not return one row");
  }
  const totalRows = Number(requireString(row, "total_rows"));
  const distinctReportIds = Number(requireString(row, "distinct_report_ids"));
  if (!Number.isInteger(totalRows) || !Number.isInteger(distinctReportIds)) {
    throw new AustinSocrataClientError("bad_response", "Austin Socrata count query returned non-integers");
  }
  return { totalRows, distinctReportIds };
}

/** All itemized contribution rows received by one filer name (exact match on `recipient`). */
export async function getAustinContributionRowsByRecipient(
  recipient: string,
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions()
): Promise<AustinContributionRow[]> {
  const rows = await fetchAustinSocrataPagedRows(
    AUSTIN_SOCRATA_CONTRIBUTIONS_DATASET,
    { $where: `recipient = ${soqlString(requireNonEmpty(recipient, "recipient"))}`, $order: "transaction_id" },
    options
  );
  return rows.map(austinContributionRowFromRecord);
}

/** Every Direct Campaign Expenditure row (the dataset is small: hundreds of rows). */
export async function getAustinDirectCampaignExpenditureRows(
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions()
): Promise<AustinDirectCampaignExpenditureRow[]> {
  const rows = await fetchAustinSocrataPagedRows(
    AUSTIN_SOCRATA_DIRECT_CAMPAIGN_EXPENDITURES_DATASET,
    { $order: "dce_id" },
    options
  );
  return rows.map(austinDirectCampaignExpenditureRowFromRecord);
}

/** Every Committee Purpose row (the dataset is small: hundreds of rows). */
export async function getAustinCommitteePurposeRows(
  options: AustinSocrataClientOptions = defaultAustinSocrataClientOptions()
): Promise<AustinCommitteePurposeRow[]> {
  const rows = await fetchAustinSocrataPagedRows(
    AUSTIN_SOCRATA_COMMITTEE_PURPOSE_DATASET,
    { $order: "committee_purp_id" },
    options
  );
  return rows.map(austinCommitteePurposeRowFromRecord);
}

// --- Effective-report selection (plan gotchas 1 and 2) -----------------------

/** Regular candidate/officeholder reports; a correction (`CORCOH`) re-files one of these. */
export const AUSTIN_CANDIDATE_REGULAR_FORM_CODES: ReadonlySet<string> = new Set(["COH", "COHFR"]);
export const AUSTIN_CANDIDATE_CORRECTION_FORM_CODES: ReadonlySet<string> = new Set(["CORCOH"]);
/** Pre-election special reports whose rows are re-reported on the next regular report. */
export const AUSTIN_CANDIDATE_SPECIAL_FORM_CODES: ReadonlySet<string> = new Set(["COHATX7"]);

export type AustinEffectiveReportSelection = {
  /** One report per (period_from, period_to): the latest-filed regular/correction report. */
  effective: AustinReportDetailRow[];
  /** Regular/correction reports superseded by a later filing for the same period. */
  superseded: AustinReportDetailRow[];
  /** Special reports whose period lies inside an effective report's period (re-reported). */
  droppedSpecial: AustinReportDetailRow[];
  /** Special reports NOT covered by any effective report — the only special rows a caller may count. */
  keptSpecial: AustinReportDetailRow[];
  /** Rows of other form types (PAC reports, dissolutions, …) — never candidate finance. */
  ignored: AustinReportDetailRow[];
  /** Exact duplicate rows removed by report id before selection. */
  duplicateRowCount: number;
};

/**
 * Selects the reports whose cover totals and itemized rows may be counted for
 * one candidate filer:
 *   - rows are deduplicated by `report_id` (the dataset repeats some reports);
 *   - regular + correction reports are grouped by exact reporting period and
 *     the latest `date_filed` wins (a correction re-lists everything, so the
 *     original must not also count); ties prefer the correction, then the
 *     larger report id;
 *   - a special (ATX.7) report is dropped when its period lies inside an
 *     effective report's period, because the regular report re-reports it;
 *   - reports without a reporting period cannot be reconciled and are ignored.
 */
export function selectAustinEffectiveReports(rows: readonly AustinReportDetailRow[]): AustinEffectiveReportSelection {
  const byId = new Map<string, AustinReportDetailRow>();
  let duplicateRowCount = 0;
  for (const row of rows) {
    if (byId.has(row.reportId)) {
      duplicateRowCount += 1;
      continue;
    }
    byId.set(row.reportId, row);
  }
  const groups = new Map<string, AustinReportDetailRow[]>();
  const specials: AustinReportDetailRow[] = [];
  const ignored: AustinReportDetailRow[] = [];
  for (const row of byId.values()) {
    const isRegular = AUSTIN_CANDIDATE_REGULAR_FORM_CODES.has(row.formTypeCode);
    const isCorrection = AUSTIN_CANDIDATE_CORRECTION_FORM_CODES.has(row.formTypeCode);
    const isSpecial = AUSTIN_CANDIDATE_SPECIAL_FORM_CODES.has(row.formTypeCode);
    if (!(isRegular || isCorrection || isSpecial) || row.periodFrom === null || row.periodTo === null) {
      ignored.push(row);
      continue;
    }
    if (isSpecial) {
      specials.push(row);
      continue;
    }
    const key = `${row.periodFrom} ${row.periodTo}`;
    const group = groups.get(key) ?? [];
    group.push(row);
    groups.set(key, group);
  }
  const effective: AustinReportDetailRow[] = [];
  const superseded: AustinReportDetailRow[] = [];
  for (const group of groups.values()) {
    group.sort((left, right) => {
      if (left.dateFiled !== right.dateFiled) return left.dateFiled < right.dateFiled ? -1 : 1;
      const leftCorrection = AUSTIN_CANDIDATE_CORRECTION_FORM_CODES.has(left.formTypeCode) ? 1 : 0;
      const rightCorrection = AUSTIN_CANDIDATE_CORRECTION_FORM_CODES.has(right.formTypeCode) ? 1 : 0;
      if (leftCorrection !== rightCorrection) return leftCorrection - rightCorrection;
      return left.reportId < right.reportId ? -1 : left.reportId > right.reportId ? 1 : 0;
    });
    const winner = group[group.length - 1]!;
    effective.push(winner);
    superseded.push(...group.slice(0, -1));
  }
  const droppedSpecial: AustinReportDetailRow[] = [];
  const keptSpecial: AustinReportDetailRow[] = [];
  for (const special of specials) {
    const covered = effective.some(
      (row) => row.periodFrom! <= special.periodFrom! && special.periodTo! <= row.periodTo!
    );
    (covered ? droppedSpecial : keptSpecial).push(special);
  }
  const byPeriod = (left: AustinReportDetailRow, right: AustinReportDetailRow) =>
    left.periodFrom! < right.periodFrom! ? -1 : left.periodFrom! > right.periodFrom! ? 1 : 0;
  effective.sort(byPeriod);
  return { effective, superseded, droppedSpecial, keptSpecial, ignored, duplicateRowCount };
}
