export const NEW_JERSEY_ELEC_BASE_URL = "https://www.njelecefilesearch.com";
export const NEW_JERSEY_ELEC_SOURCE_URL = `${NEW_JERSEY_ELEC_BASE_URL}/`;
export const NEW_JERSEY_ELEC_DEFAULT_TIMEOUT_MS = 30_000;
export const NEW_JERSEY_ELEC_DEFAULT_CONTRIBUTION_ROW_LIMIT = 100_000;
export const NEW_JERSEY_ELEC_MAX_CONTRIBUTION_ROW_LIMIT = 100_000;

const NEW_JERSEY_ELEC_ENTITY_LIST_PATH = "/api/VWEntity/GetEntityList";
const NEW_JERSEY_ELEC_CONTRIBUTION_DETAIL_PATH = "/api/VWContributionDetail/GetContBitsDataByObject";
const NEW_JERSEY_ELEC_ENTITY_FILING_DATA_PATH = "/api/VWEntity/GetEntityFilingData";
const NEW_JERSEY_ELEC_REPORT_DOWNLOAD_PATH = "/SearchIndExpReports/";

export type NewJerseyElecClientErrorCode = "invalid_request" | "network_error" | "http_error" | "bad_response";

export class NewJerseyElecClientError extends Error {
  constructor(
    public readonly code: NewJerseyElecClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "NewJerseyElecClientError";
  }
}

export type NewJerseyElecClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type NewJerseyElecEntitySearchInput = {
  firstName?: string | null;
  lastName?: string | null;
  nonIndividualName?: string | null;
  pacName?: string | null;
  nonPacOnly?: boolean;
};

export type NewJerseyElecIndependentExpenditureEntitiesInput = {
  electionYear: number;
  electionTypeCode?: string | null;
};

export type NewJerseyElecEntity = {
  entityS: number;
  entityName: string;
  firstName: string | null;
  middleInitial: string | null;
  lastName: string | null;
  suffix: string | null;
  nonIndividualName: string | null;
  pacName: string | null;
  electionYear: number;
  sequenceNumber: number | null;
  officeCode: string | null;
  office: string | null;
  partyCode: string | null;
  party: string | null;
  locationCode: number | null;
  location: string | null;
  electionTypeCode: string | null;
  electionType: string | null;
  entityType: string | null;
  sourceUrl: string;
};

export type NewJerseyElecContributionRowsInput = {
  entityS: number;
  electionYear: number;
  firstName?: string | null;
  lastName?: string | null;
  nonIndividualName?: string | null;
  officeCode?: string | null;
  partyCode?: string | null;
  locationCode?: string | number | null;
  electionTypeCode?: string | null;
  nonPacOnly?: boolean;
  rowLimit?: number;
};

export type NewJerseyElecContributionRow = {
  contribS: number;
  entityS: number;
  electionYear: number | null;
  recipientName: string | null;
  contributorName: string;
  contributorFirstName: string | null;
  contributorLastName: string | null;
  contributorNonIndividualName: string | null;
  isIndividual: boolean;
  contributorType: string | null;
  contributionType: string | null;
  contributionDate: string | null;
  amount: number;
  employerName: string | null;
  occupationCode: string | null;
  occupationName: string | null;
  sourceUrl: string;
};

export type NewJerseyElecContributionRowsResult = {
  recordsTotal: number;
  recordsFiltered: number;
  rows: NewJerseyElecContributionRow[];
  sourceUrl: string;
};

export type NewJerseyElecEntityFilingsInput = {
  entityS: number;
};

export type NewJerseyElecFiling = {
  entityS: number;
  docId: number;
  period: number | null;
  amendmentNumber: number | null;
  dateReceived: string | null;
  publicAccess: boolean;
  hour48NoticePublicAccess: boolean;
  amountReceived: number | null;
  amountDisbursed: number | null;
  filingStatusFormCode: string | null;
  linkTabFormType: string | null;
  formName: string | null;
  sortSequence: number | null;
  reportDownloadUrl: string;
  sourceUrl: string;
};

export type NewJerseyElecReportDownload = {
  docId: number;
  fileNameWithSas: string;
  sourceUrl: string;
};

type NewJerseyElecEntityApiRow = {
  ENTITY_S?: unknown;
  ENTITYNAME?: unknown;
  FIRST_NAME?: unknown;
  MIDDLE_INITIAL?: unknown;
  LAST_NAME?: unknown;
  SUFFIX?: unknown;
  NON_IND_NAME?: unknown;
  PACNAME?: unknown;
  ELECTIONYEAR?: unknown;
  SEQ_NUM?: unknown;
  OFFICECODE?: unknown;
  OFFICE?: unknown;
  PARTYCODE?: unknown;
  PARTY?: unknown;
  LOCATION_CODE?: unknown;
  LOCATION?: unknown;
  ELECTIONTYPECODE?: unknown;
  ELECTIONTYPE?: unknown;
  ENTITY_TYPE?: unknown;
};

type NewJerseyElecContributionApiRow = {
  CONTRIB_S?: unknown;
  ENTITY_S?: unknown;
  ELECTIONYEAR?: unknown;
  CAND_NAME?: unknown;
  CONTRIBUTOR?: unknown;
  FIRST_NAME?: unknown;
  LAST_NAME?: unknown;
  NON_IND_NAME?: unknown;
  IsIndividual?: unknown;
  ContributorType?: unknown;
  ContributionType?: unknown;
  CONT_DATE?: unknown;
  CONT_AMT?: unknown;
  EMP_NAME?: unknown;
  OccupationCode?: unknown;
  OCCUPATION?: unknown;
  OccupationName?: unknown;
};

type NewJerseyElecContributionApiResponse = {
  recordsTotal?: unknown;
  recordsFiltered?: unknown;
  data?: unknown;
};

type NewJerseyElecFilingApiRow = {
  ENTITY_S?: unknown;
  PERIOD?: unknown;
  AMEND_NO?: unknown;
  DATE_RECEIVED?: unknown;
  DOCID?: unknown;
  PUBLIC_ACCESS?: unknown;
  HOUR_48_NOTICE_PUBLIC_ACCESS?: unknown;
  AMT_REC?: unknown;
  AMT_DISB?: unknown;
  FilingStatusFormCode?: unknown;
  LinkTabFormType?: unknown;
  FormName?: unknown;
  SORT_SEQ?: unknown;
};

type NewJerseyElecReportDownloadApiResponse = {
  FileNameWithSAS?: unknown;
};

const CONTRIBUTION_COLUMNS = [
  ["CONTRIBUTOR", "CONTRIBUTOR"],
  ["Address", "STREET1"],
  ["EMP_NAME", "EMP_NAME"],
  ["EmployerAddress", "EMP_STREET1"],
  ["OccupationName", "OccupationName"],
  ["CAND_NAME", "CAND_NAME"],
  ["ContributorType", "ContributorType"],
  ["ContributionType", "ContributionType"],
  ["CONT_DATE", "CONT_DATE"],
  ["CONT_AMT", "CONT_AMT"],
  ["CONTRIB_S", "CONTRIB_S"],
] as const;

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function absoluteNewJerseyElecUrl(path: string): string {
  return new URL(path, NEW_JERSEY_ELEC_BASE_URL).toString();
}

function normalizeOptionalText(value: string | null | undefined): string {
  return value?.trim().replace(/\s+/g, " ") ?? "";
}

function requireSearchTerm(input: NewJerseyElecEntitySearchInput): void {
  const values = [input.firstName, input.lastName, input.nonIndividualName, input.pacName].map(normalizeOptionalText);
  if (!values.some((value) => value.length > 0)) {
    throw new NewJerseyElecClientError("invalid_request", "At least one NJ ELEC entity search term is required");
  }
}

function normalizeEntityS(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new NewJerseyElecClientError("invalid_request", `Invalid NJ ELEC ENTITY_S: ${value}`);
  }
  return value;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new NewJerseyElecClientError("invalid_request", `Invalid NJ ELEC election year: ${value}`);
  }
  return value;
}

function normalizeContributionRowLimit(value: number | undefined): number {
  const normalized = value ?? NEW_JERSEY_ELEC_DEFAULT_CONTRIBUTION_ROW_LIMIT;
  if (
    !Number.isInteger(normalized) ||
    normalized <= 0 ||
    normalized > NEW_JERSEY_ELEC_MAX_CONTRIBUTION_ROW_LIMIT
  ) {
    throw new NewJerseyElecClientError(
      "invalid_request",
      `NJ ELEC contribution rowLimit must be an integer between 1 and ${NEW_JERSEY_ELEC_MAX_CONTRIBUTION_ROW_LIMIT}`
    );
  }
  return normalized;
}

function getString(row: Record<string, unknown>, key: string): string | null {
  const value = row[key];
  if (typeof value === "string") {
    const trimmed = value.trim().replace(/\s+/g, " ");
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}

function getNumber(row: Record<string, unknown>, key: string): number | null {
  const value = row[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value.replace(/[$,]/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function getInteger(row: Record<string, unknown>, key: string): number | null {
  const value = getNumber(row, key);
  return value !== null && Number.isInteger(value) ? value : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isIndividualValue(value: unknown): boolean {
  return typeof value === "string" && value.trim().toUpperCase() === "Y";
}

function getBooleanFromFlag(row: Record<string, unknown>, key: string): boolean {
  const value = row[key];
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value === 1;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toUpperCase();
    return normalized === "1" || normalized === "Y" || normalized === "TRUE";
  }
  return false;
}

async function fetchNewJerseyElecJson<T>(
  url: string,
  init: RequestInit,
  options: NewJerseyElecClientOptions = {}
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? NEW_JERSEY_ELEC_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  let response: Response;
  try {
    response = await (options.fetchImpl ?? fetch)(url, {
      ...init,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new NewJerseyElecClientError("network_error", `NJ ELEC request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw new NewJerseyElecClientError(
      "network_error",
      `NJ ELEC request failed for ${url}: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    throw new NewJerseyElecClientError(
      "http_error",
      `NJ ELEC request failed with HTTP ${response.status} for ${url}`,
      response.status
    );
  }

  try {
    return (await response.json()) as T;
  } catch (error) {
    throw new NewJerseyElecClientError(
      "bad_response",
      `NJ ELEC response was not valid JSON for ${url}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

export function buildNewJerseyElecEntityListUrl(input: NewJerseyElecEntitySearchInput): string {
  requireSearchTerm(input);
  return buildNewJerseyElecEntityListUrlUnchecked(input);
}

function buildNewJerseyElecEntityListUrlUnchecked(input: NewJerseyElecEntitySearchInput): string {
  const url = new URL(NEW_JERSEY_ELEC_ENTITY_LIST_PATH, NEW_JERSEY_ELEC_BASE_URL);
  url.searchParams.set("NONPACOnly", input.nonPacOnly === false ? "false" : "true");
  url.searchParams.set("LastName", normalizeOptionalText(input.lastName));
  url.searchParams.set("FirstName", normalizeOptionalText(input.firstName));
  url.searchParams.set("NonIndName", normalizeOptionalText(input.nonIndividualName));
  url.searchParams.set("PACName", normalizeOptionalText(input.pacName));
  return url.toString();
}

export function buildNewJerseyElecIndependentExpenditureEntitiesUrl(): string {
  return buildNewJerseyElecEntityListUrlUnchecked({
    nonPacOnly: false,
  });
}

export function buildNewJerseyElecContributionRowsSourceUrl(entityS: number): string {
  return `${NEW_JERSEY_ELEC_BASE_URL}/SearchContributionToEntity?eid=${normalizeEntityS(entityS)}`;
}

export function buildNewJerseyElecEntityFilingsUrl(input: NewJerseyElecEntityFilingsInput): string {
  const url = new URL(NEW_JERSEY_ELEC_ENTITY_FILING_DATA_PATH, NEW_JERSEY_ELEC_BASE_URL);
  url.searchParams.set("ENTITY_S", String(normalizeEntityS(input.entityS)));
  return url.toString();
}

export function buildNewJerseyElecReportDownloadUrl(docId: number): string {
  if (!Number.isSafeInteger(docId) || docId <= 0) {
    throw new NewJerseyElecClientError("invalid_request", `Invalid NJ ELEC DOCID: ${docId}`);
  }
  const url = new URL(NEW_JERSEY_ELEC_REPORT_DOWNLOAD_PATH, NEW_JERSEY_ELEC_BASE_URL);
  url.searchParams.set("handler", "DownloadReport");
  url.searchParams.set("DocId", String(docId));
  return url.toString();
}

export function buildNewJerseyElecContributionRowsForm(input: NewJerseyElecContributionRowsInput): URLSearchParams {
  const entityS = normalizeEntityS(input.entityS);
  const electionYear = normalizeElectionYear(input.electionYear);
  const rowLimit = normalizeContributionRowLimit(input.rowLimit);
  const body = new URLSearchParams();

  body.set("ENTITY_S", String(entityS));
  body.set("NONPACOnly", input.nonPacOnly === false ? "false" : "true");
  body.set("FirstName", normalizeOptionalText(input.firstName));
  body.set("LastName", normalizeOptionalText(input.lastName));
  body.set("NonIndName", normalizeOptionalText(input.nonIndividualName));
  body.set("OfficeCodes", normalizeOptionalText(input.officeCode));
  body.set("PartyCodes", normalizeOptionalText(input.partyCode));
  body.set("LocationCodes", normalizeOptionalText(input.locationCode == null ? null : String(input.locationCode)));
  body.set("ElectionTypeCodes", normalizeOptionalText(input.electionTypeCode));
  body.set("ElectionYears", String(electionYear));
  body.set("ContributorFirstName", "");
  body.set("ContributorLastName", "");
  body.set("ContributorMI", "");
  body.set("ContributorSuffix", "");
  body.set("ContributorNonIndName", "");
  body.set("ContributorTypeCodes", "");
  body.set("EMP_NAME", "");
  body.set("OccupationCodes", "");
  body.set("DateFrom", "");
  body.set("DateTo", "");
  body.set("AmountFrom", "");
  body.set("AmountTo", "");
  body.set("draw", "1");
  body.set("start", "0");
  body.set("length", String(rowLimit));
  body.set("order[0][column]", "0");
  body.set("order[0][dir]", "asc");

  CONTRIBUTION_COLUMNS.forEach(([data, name], index) => {
    body.set(`columns[${index}][data]`, data);
    body.set(`columns[${index}][name]`, name);
  });

  return body;
}

export function mapNewJerseyElecEntity(row: NewJerseyElecEntityApiRow, sourceUrl: string): NewJerseyElecEntity | null {
  if (!isRecord(row)) {
    return null;
  }
  const entityS = getInteger(row, "ENTITY_S");
  const entityName = getString(row, "ENTITYNAME");
  const electionYear = getInteger(row, "ELECTIONYEAR");
  if (entityS === null || !entityName || electionYear === null) {
    return null;
  }

  return {
    entityS,
    entityName,
    firstName: getString(row, "FIRST_NAME"),
    middleInitial: getString(row, "MIDDLE_INITIAL"),
    lastName: getString(row, "LAST_NAME"),
    suffix: getString(row, "SUFFIX"),
    nonIndividualName: getString(row, "NON_IND_NAME"),
    pacName: getString(row, "PACNAME"),
    electionYear,
    sequenceNumber: getInteger(row, "SEQ_NUM"),
    officeCode: getString(row, "OFFICECODE"),
    office: getString(row, "OFFICE"),
    partyCode: getString(row, "PARTYCODE"),
    party: getString(row, "PARTY"),
    locationCode: getInteger(row, "LOCATION_CODE"),
    location: getString(row, "LOCATION"),
    electionTypeCode: getString(row, "ELECTIONTYPECODE"),
    electionType: getString(row, "ELECTIONTYPE"),
    entityType: getString(row, "ENTITY_TYPE"),
    sourceUrl,
  };
}

export function mapNewJerseyElecContributionRow(
  row: NewJerseyElecContributionApiRow,
  sourceUrl: string
): NewJerseyElecContributionRow | null {
  if (!isRecord(row)) {
    return null;
  }
  const contribS = getInteger(row, "CONTRIB_S");
  const entityS = getInteger(row, "ENTITY_S");
  const contributorName = getString(row, "CONTRIBUTOR");
  const amount = getNumber(row, "CONT_AMT");
  if (contribS === null || entityS === null || !contributorName || amount === null) {
    return null;
  }

  return {
    contribS,
    entityS,
    electionYear: getInteger(row, "ELECTIONYEAR"),
    recipientName: getString(row, "CAND_NAME"),
    contributorName,
    contributorFirstName: getString(row, "FIRST_NAME"),
    contributorLastName: getString(row, "LAST_NAME"),
    contributorNonIndividualName: getString(row, "NON_IND_NAME"),
    isIndividual: isIndividualValue(row.IsIndividual),
    contributorType: getString(row, "ContributorType"),
    contributionType: getString(row, "ContributionType"),
    contributionDate: getString(row, "CONT_DATE"),
    amount,
    employerName: getString(row, "EMP_NAME"),
    occupationCode: getString(row, "OccupationCode") ?? getString(row, "OCCUPATION"),
    occupationName: getString(row, "OccupationName"),
    sourceUrl,
  };
}

export function mapNewJerseyElecFiling(row: NewJerseyElecFilingApiRow, sourceUrl: string): NewJerseyElecFiling | null {
  if (!isRecord(row)) {
    return null;
  }
  const entityS = getInteger(row, "ENTITY_S");
  const docId = getInteger(row, "DOCID");
  if (entityS === null || docId === null) {
    return null;
  }

  return {
    entityS,
    docId,
    period: getInteger(row, "PERIOD"),
    amendmentNumber: getInteger(row, "AMEND_NO"),
    dateReceived: getString(row, "DATE_RECEIVED"),
    publicAccess: getBooleanFromFlag(row, "PUBLIC_ACCESS"),
    hour48NoticePublicAccess: getBooleanFromFlag(row, "HOUR_48_NOTICE_PUBLIC_ACCESS"),
    amountReceived: getNumber(row, "AMT_REC"),
    amountDisbursed: getNumber(row, "AMT_DISB"),
    filingStatusFormCode: getString(row, "FilingStatusFormCode"),
    linkTabFormType: getString(row, "LinkTabFormType"),
    formName: getString(row, "FormName"),
    sortSequence: getInteger(row, "SORT_SEQ"),
    reportDownloadUrl: buildNewJerseyElecReportDownloadUrl(docId),
    sourceUrl,
  };
}

export async function searchNewJerseyElecEntities(
  input: NewJerseyElecEntitySearchInput,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyElecEntity[]> {
  const url = buildNewJerseyElecEntityListUrl(input);
  const payload = await fetchNewJerseyElecJson<unknown>(url, { headers: { accept: "application/json" } }, options);
  if (!Array.isArray(payload)) {
    throw new NewJerseyElecClientError("bad_response", "NJ ELEC entity response is missing an array payload");
  }
  return payload.flatMap((row) => {
    const mapped = mapNewJerseyElecEntity(row as NewJerseyElecEntityApiRow, url);
    return mapped ? [mapped] : [];
  });
}

export async function listNewJerseyElecIndependentExpenditureEntities(
  input: NewJerseyElecIndependentExpenditureEntitiesInput,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyElecEntity[]> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const electionTypeCode = normalizeOptionalText(input.electionTypeCode).toUpperCase();
  const url = buildNewJerseyElecIndependentExpenditureEntitiesUrl();
  const payload = await fetchNewJerseyElecJson<unknown>(url, { headers: { accept: "application/json" } }, options);
  if (!Array.isArray(payload)) {
    throw new NewJerseyElecClientError("bad_response", "NJ ELEC entity response is missing an array payload");
  }
  return payload
    .flatMap((row) => {
      const mapped = mapNewJerseyElecEntity(row as NewJerseyElecEntityApiRow, url);
      return mapped ? [mapped] : [];
    })
    .filter((entity) => {
      if (entity.electionYear !== electionYear) {
        return false;
      }
      if (entity.officeCode !== "Z") {
        return false;
      }
      return !electionTypeCode || entity.electionTypeCode?.toUpperCase() === electionTypeCode;
    });
}

export async function getNewJerseyElecContributionRows(
  input: NewJerseyElecContributionRowsInput,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyElecContributionRowsResult> {
  const sourceUrl = buildNewJerseyElecContributionRowsSourceUrl(input.entityS);
  const payload = await fetchNewJerseyElecJson<NewJerseyElecContributionApiResponse>(
    absoluteNewJerseyElecUrl(NEW_JERSEY_ELEC_CONTRIBUTION_DETAIL_PATH),
    {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
      },
      body: buildNewJerseyElecContributionRowsForm(input),
    },
    options
  );

  if (!Array.isArray(payload.data)) {
    throw new NewJerseyElecClientError("bad_response", "NJ ELEC contribution response is missing data rows");
  }

  return {
    recordsTotal: getNumber({ recordsTotal: payload.recordsTotal }, "recordsTotal") ?? 0,
    recordsFiltered: getNumber({ recordsFiltered: payload.recordsFiltered }, "recordsFiltered") ?? 0,
    rows: payload.data.flatMap((row) => {
      const mapped = mapNewJerseyElecContributionRow(row as NewJerseyElecContributionApiRow, sourceUrl);
      return mapped ? [mapped] : [];
    }),
    sourceUrl,
  };
}

export async function getNewJerseyElecEntityFilings(
  input: NewJerseyElecEntityFilingsInput,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyElecFiling[]> {
  const url = buildNewJerseyElecEntityFilingsUrl(input);
  const payload = await fetchNewJerseyElecJson<unknown>(url, { headers: { accept: "application/json" } }, options);
  if (!Array.isArray(payload)) {
    throw new NewJerseyElecClientError("bad_response", "NJ ELEC entity filing response is missing an array payload");
  }
  return payload.flatMap((row) => {
    const mapped = mapNewJerseyElecFiling(row as NewJerseyElecFilingApiRow, url);
    return mapped ? [mapped] : [];
  });
}

export async function getNewJerseyElecFilingRows(
  input: NewJerseyElecEntityFilingsInput,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyElecFiling[]> {
  return await getNewJerseyElecEntityFilings(input, options);
}

export async function getNewJerseyElecReportDownload(
  docId: number,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyElecReportDownload> {
  const url = buildNewJerseyElecReportDownloadUrl(docId);
  const payload = await fetchNewJerseyElecJson<NewJerseyElecReportDownloadApiResponse>(
    url,
    { headers: { accept: "application/json" } },
    options
  );
  const fileNameWithSas =
    typeof payload.FileNameWithSAS === "string" && payload.FileNameWithSAS.trim().length > 0
      ? payload.FileNameWithSAS
      : null;
  if (!fileNameWithSas) {
    throw new NewJerseyElecClientError("bad_response", "NJ ELEC report download response is missing FileNameWithSAS");
  }
  return {
    docId,
    fileNameWithSas,
    sourceUrl: url,
  };
}

export async function downloadNewJerseyElecReportUrl(
  docId: number,
  options: NewJerseyElecClientOptions = {}
): Promise<string> {
  return (await getNewJerseyElecReportDownload(docId, options)).fileNameWithSas;
}
