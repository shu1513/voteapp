export const KENTUCKY_KREF_PUBLIC_SEARCH_BASE_URL = "https://secure.kentucky.gov/kref/publicsearch";
export const KENTUCKY_KREF_DEFAULT_TIMEOUT_MS = 30_000;
export const KENTUCKY_KREF_IE_ONLY_ORGANIZATION_TYPE = "UnauthorizedCampaignCommittee";

export type KentuckyKrefClientErrorCode = "invalid_request" | "network_error" | "http_error" | "bad_response";

export class KentuckyKrefClientError extends Error {
  constructor(
    public readonly code: KentuckyKrefClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "KentuckyKrefClientError";
  }
}

export type KentuckyKrefClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type KentuckyKrefPublicSearchPage = "candidate_contributions" | "organization_contributions" | "independent_expenditures";

export type KentuckyKrefDropdownOption = {
  value: string;
  label: string;
};

export type KentuckyKrefContributionExportInput = {
  contributionSearchType: "Candidate" | "Organization";
  candidateFirstName?: string | null;
  candidateLastName?: string | null;
  electionDate?: string | null;
  electionType?: string | null;
  officeSought?: string | null;
  location?: string | null;
  organizationName?: string | null;
  organizationType?: string | null;
  firstName?: string | null;
  lastName?: string | null;
  fromOrganizationName?: string | null;
  city?: string | null;
  state?: string | null;
  zip?: string | null;
  employer?: string | null;
  occupation?: string | null;
  otherOccupation?: string | null;
  minAmount?: number | string | null;
  maxAmount?: number | string | null;
  minimalDate?: string | null;
  maximalDate?: string | null;
  contributionTypes?: readonly string[];
  contributionMode?: string | null;
  paymentCodes?: readonly string[];
};

export type KentuckyKrefIndependentExpenditureExportInput = {
  firstName?: string | null;
  lastName?: string | null;
  organizationName?: string | null;
  toWhomMade?: string | null;
  minimalDate?: string | null;
  maximalDate?: string | null;
  minAmount?: number | string | null;
  maxAmount?: number | string | null;
  candidateFirstName?: string | null;
  candidateLastName?: string | null;
  isSupported?: boolean | null;
  electionDate?: string | null;
  electionType?: string | null;
  officeIssue?: string | null;
  jurisdiction?: string | null;
  minimalElectionDate?: string | null;
  maximalElectionDate?: string | null;
};

export type KentuckyKrefCsvRow = Record<string, string>;

export type KentuckyKrefContributionRecord = {
  recipientName?: string;
  toOrganizationName?: string;
  candidateName?: string;
  candidateFirstName?: string;
  candidateLastName?: string;
  office?: string;
  location?: string;
  electionDate?: string;
  electionYear?: number;
  electionType?: string;
  contributorName?: string;
  contributorType?: string;
  contributionMode?: string;
  occupation?: string;
  otherOccupation?: string;
  employer?: string;
  city?: string;
  state?: string;
  zip?: string;
  amount: number;
  receiptDate?: string;
  statementType?: string;
};

export type KentuckyKrefIndependentExpenditureRecord = {
  toWhomMade?: string;
  spenderName?: string;
  date?: string;
  candidateName?: string;
  supportOppose?: "support" | "oppose";
  officeOrBallotMeasure?: string;
  electionDate?: string;
  electionYear?: number;
  amount: number;
};

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function appendOptionalParam(params: URLSearchParams, key: string, value: string | number | boolean | null | undefined): void {
  if (value === undefined || value === null) {
    return;
  }
  const normalized = String(value).trim();
  if (normalized) {
    params.append(key, normalized);
  }
}

function appendOptionalList(params: URLSearchParams, key: string, values: readonly string[] | undefined): void {
  for (const value of values ?? []) {
    appendOptionalParam(params, key, value);
  }
}

function requireContributionSearchType(value: KentuckyKrefContributionExportInput["contributionSearchType"]): void {
  if (value !== "Candidate" && value !== "Organization") {
    throw new KentuckyKrefClientError("invalid_request", `Unsupported Kentucky KREF contribution search type: ${value}`);
  }
}

function normalizeAmountInput(value: number | string | null | undefined, fieldName: string): string | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const normalized = typeof value === "number" ? value : Number(String(value).replace(/[$,]/g, "").trim());
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new KentuckyKrefClientError("invalid_request", `${fieldName} must be a nonnegative amount`);
  }
  return String(normalized);
}

export function buildKentuckyKrefPublicSearchPageUrl(page: KentuckyKrefPublicSearchPage): string {
  switch (page) {
    case "candidate_contributions":
      return `${KENTUCKY_KREF_PUBLIC_SEARCH_BASE_URL}/ToCandidateSearch`;
    case "organization_contributions":
      return `${KENTUCKY_KREF_PUBLIC_SEARCH_BASE_URL}/ToOrganizationSearch`;
    case "independent_expenditures":
      return `${KENTUCKY_KREF_PUBLIC_SEARCH_BASE_URL}/IndependentExpenditureSearch`;
    default: {
      const exhaustive: never = page;
      throw new KentuckyKrefClientError("invalid_request", `Unsupported Kentucky KREF public search page: ${exhaustive}`);
    }
  }
}

export function buildKentuckyKrefContributionExportUrl(input: KentuckyKrefContributionExportInput): string {
  requireContributionSearchType(input.contributionSearchType);

  const url = new URL(`${KENTUCKY_KREF_PUBLIC_SEARCH_BASE_URL}/ExportContributors`);
  const params = url.searchParams;
  appendOptionalParam(params, "ContributionSearchType", input.contributionSearchType);
  appendOptionalParam(params, "CandidateFirstName", input.candidateFirstName);
  appendOptionalParam(params, "CandidateLastName", input.candidateLastName);
  appendOptionalParam(params, "ElectionDate", input.electionDate);
  appendOptionalParam(params, "ElectionType", input.electionType);
  appendOptionalParam(params, "OfficeSought", input.officeSought);
  appendOptionalParam(params, "Location", input.location);
  appendOptionalParam(params, "OrganizationName", input.organizationName);
  appendOptionalParam(params, "OrganizationType", input.organizationType);
  appendOptionalParam(params, "FirstName", input.firstName);
  appendOptionalParam(params, "LastName", input.lastName);
  appendOptionalParam(params, "FromOrganizationName", input.fromOrganizationName);
  appendOptionalParam(params, "City", input.city);
  appendOptionalParam(params, "State", input.state);
  appendOptionalParam(params, "Zip", input.zip);
  appendOptionalParam(params, "Employer", input.employer);
  appendOptionalParam(params, "Occupation", input.occupation);
  appendOptionalParam(params, "OtherOccupation", input.otherOccupation);
  appendOptionalParam(params, "MinAmount", normalizeAmountInput(input.minAmount, "Kentucky KREF minAmount"));
  appendOptionalParam(params, "MaxAmount", normalizeAmountInput(input.maxAmount, "Kentucky KREF maxAmount"));
  appendOptionalParam(params, "MinimalDate", input.minimalDate);
  appendOptionalParam(params, "MaximalDate", input.maximalDate);
  appendOptionalList(params, "ContributionTypes", input.contributionTypes);
  appendOptionalParam(params, "ContributionMode", input.contributionMode);
  appendOptionalList(params, "PaymentCodes", input.paymentCodes);
  return url.toString();
}

export function buildKentuckyKrefIndependentExpenditureExportUrl(
  input: KentuckyKrefIndependentExpenditureExportInput
): string {
  const url = new URL(`${KENTUCKY_KREF_PUBLIC_SEARCH_BASE_URL}/IndependentExpenditureSearch/ExportIndependentExpenditures`);
  const params = url.searchParams;
  appendOptionalParam(params, "FirstName", input.firstName);
  appendOptionalParam(params, "LastName", input.lastName);
  appendOptionalParam(params, "OrganizationName", input.organizationName);
  appendOptionalParam(params, "ToWhomMade", input.toWhomMade);
  appendOptionalParam(params, "MinimalDate", input.minimalDate);
  appendOptionalParam(params, "MaximalDate", input.maximalDate);
  appendOptionalParam(params, "MinAmount", normalizeAmountInput(input.minAmount, "Kentucky KREF minAmount"));
  appendOptionalParam(params, "MaxAmount", normalizeAmountInput(input.maxAmount, "Kentucky KREF maxAmount"));
  appendOptionalParam(params, "CandidateFirstName", input.candidateFirstName);
  appendOptionalParam(params, "CandidateLastName", input.candidateLastName);
  appendOptionalParam(params, "IsSupported", input.isSupported);
  appendOptionalParam(params, "ElectionDate", input.electionDate);
  appendOptionalParam(params, "ElectionType", input.electionType);
  appendOptionalParam(params, "OfficeIssue", input.officeIssue);
  appendOptionalParam(params, "Jurisdiction", input.jurisdiction);
  appendOptionalParam(params, "MinimalElectionDate", input.minimalElectionDate);
  appendOptionalParam(params, "MaximalElectionDate", input.maximalElectionDate);
  return url.toString();
}

async function fetchKentuckyKrefText(
  url: string,
  options: KentuckyKrefClientOptions,
  accept = "text/csv,text/plain;q=0.9,*/*;q=0.1"
): Promise<string> {
  const timeoutMs = options.timeoutMs ?? KENTUCKY_KREF_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  let response: Response;
  try {
    response = await (options.fetchImpl ?? fetch)(url, {
      headers: { accept },
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new KentuckyKrefClientError("network_error", `Kentucky KREF request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw new KentuckyKrefClientError(
      "network_error",
      `Kentucky KREF request failed for ${url}: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    throw new KentuckyKrefClientError(
      "http_error",
      `Kentucky KREF request failed: ${response.status} ${response.statusText}`,
      response.status
    );
  }

  return response.text();
}

function normalizeHeader(value: string): string {
  return value
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    const next = csv[index + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    if (char === "\r") {
      continue;
    }
    field += char;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((fields) => fields.some((fieldValue) => fieldValue.trim().length > 0));
}

export function parseKentuckyKrefCsvRows(csv: string): KentuckyKrefCsvRow[] {
  const rows = parseCsvRows(csv);
  const headerRow = rows[0];
  if (!headerRow || headerRow.length === 0) {
    return [];
  }
  const headers = headerRow.map(normalizeHeader);
  return rows.slice(1).map((fields) => {
    const row: KentuckyKrefCsvRow = {};
    for (let index = 0; index < headers.length; index += 1) {
      const header = headers[index];
      if (header) {
        row[header] = fields[index]?.trim() ?? "";
      }
    }
    return row;
  });
}

function decodeHtmlEntity(entity: string): string {
  const namedEntities: Record<string, string> = {
    amp: "&",
    apos: "'",
    gt: ">",
    lt: "<",
    nbsp: " ",
    quot: '"',
  };
  if (entity.startsWith("#x") || entity.startsWith("#X")) {
    const parsed = Number.parseInt(entity.slice(2), 16);
    return Number.isFinite(parsed) ? String.fromCodePoint(parsed) : `&${entity};`;
  }
  if (entity.startsWith("#")) {
    const parsed = Number.parseInt(entity.slice(1), 10);
    return Number.isFinite(parsed) ? String.fromCodePoint(parsed) : `&${entity};`;
  }
  return namedEntities[entity] ?? `&${entity};`;
}

function decodeHtmlText(value: string): string {
  return value
    .replace(/<[^>]*>/g, " ")
    .replace(/&([a-zA-Z]+|#[0-9]+|#x[0-9a-fA-F]+);/g, (_match, entity: string) => decodeHtmlEntity(entity))
    .replace(/\s+/g, " ")
    .trim();
}

function getHtmlAttribute(tag: string, attributeName: string): string | null {
  const pattern = new RegExp(`${attributeName}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, "i");
  const match = pattern.exec(tag);
  return decodeHtmlText(match?.[1] ?? match?.[2] ?? match?.[3] ?? "");
}

export function parseKentuckyKrefDropdownOptions(html: string, selectName: string): KentuckyKrefDropdownOption[] {
  const normalizedSelectName = selectName.trim().toLowerCase();
  if (!normalizedSelectName) {
    throw new KentuckyKrefClientError("invalid_request", "Kentucky KREF dropdown select name is required");
  }

  const options: KentuckyKrefDropdownOption[] = [];
  const selectPattern = /<select\b[^>]*>[\s\S]*?<\/select>/gi;
  let selectMatch: RegExpExecArray | null;
  while ((selectMatch = selectPattern.exec(html)) !== null) {
    const selectHtml = selectMatch[0] ?? "";
    const selectOpenTag = /<select\b[^>]*>/i.exec(selectHtml)?.[0] ?? "";
    const nameOrId = getHtmlAttribute(selectOpenTag, "name") ?? getHtmlAttribute(selectOpenTag, "id") ?? "";
    if (nameOrId.trim().toLowerCase() !== normalizedSelectName) {
      continue;
    }

    const optionPattern = /<option\b[^>]*>[\s\S]*?<\/option>/gi;
    let optionMatch: RegExpExecArray | null;
    while ((optionMatch = optionPattern.exec(selectHtml)) !== null) {
      const optionHtml = optionMatch[0] ?? "";
      const optionOpenTag = /<option\b[^>]*>/i.exec(optionHtml)?.[0] ?? "";
      const value = getHtmlAttribute(optionOpenTag, "value") ?? "";
      const label = decodeHtmlText(optionHtml.replace(/^<option\b[^>]*>/i, "").replace(/<\/option>$/i, ""));
      if (value.trim() || label.trim()) {
        options.push({
          value: value.trim(),
          label: label.trim(),
        });
      }
    }
  }
  return options;
}

export function parseKentuckyKrefElectionDateOptions(html: string, selectName = "ElectionDate"): KentuckyKrefDropdownOption[] {
  return parseKentuckyKrefDropdownOptions(html, selectName).filter((option) => option.value || !/^select election date/i.test(option.label));
}

function getString(row: KentuckyKrefCsvRow, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = row[normalizeHeader(key)] ?? row[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim().replace(/\s+/g, " ");
    }
  }
  return undefined;
}

function parseAmount(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Number(value.replace(/[$,()]/g, "").trim());
  if (!Number.isFinite(parsed)) {
    return undefined;
  }
  return Math.abs(Math.round(parsed * 100) / 100);
}

function parseElectionYear(value: string | undefined): number | undefined {
  const match = /(?:^|\/)(\d{4})(?:\D|$)/.exec(value ?? "");
  if (!match?.[1]) {
    return undefined;
  }
  const parsed = Number.parseInt(match[1], 10);
  return Number.isInteger(parsed) ? parsed : undefined;
}

function combineNameParts(parts: Array<string | undefined>): string | undefined {
  const normalized = parts.filter((part): part is string => Boolean(part)).join(" ").replace(/\s+/g, " ").trim();
  return normalized || undefined;
}

function contributorNameFromRow(row: KentuckyKrefCsvRow): string | undefined {
  return (
    getString(row, "from_organization_name") ??
    combineNameParts([getString(row, "contributor_first_name"), getString(row, "contributor_last_name")])
  );
}

export function kentuckyKrefContributionRecordFromRow(row: KentuckyKrefCsvRow): KentuckyKrefContributionRecord | null {
  const amount = parseAmount(getString(row, "amount"));
  if (amount === undefined || amount <= 0) {
    return null;
  }
  const candidateFirstName = getString(row, "recipient_first_name");
  const candidateLastName = getString(row, "recipient_last_name");
  const candidateName = combineNameParts([candidateFirstName, candidateLastName]);
  const toOrganizationName = getString(row, "to_organization");
  const electionDate = getString(row, "election_date");

  return {
    ...(candidateName ? { candidateName } : {}),
    ...(candidateFirstName ? { candidateFirstName } : {}),
    ...(candidateLastName ? { candidateLastName } : {}),
    ...(toOrganizationName ? { toOrganizationName } : {}),
    ...(candidateName || toOrganizationName ? { recipientName: candidateName ?? toOrganizationName } : {}),
    ...(getString(row, "office_sought") ? { office: getString(row, "office_sought") } : {}),
    ...(getString(row, "location") ? { location: getString(row, "location") } : {}),
    ...(electionDate ? { electionDate } : {}),
    ...(parseElectionYear(electionDate) !== undefined ? { electionYear: parseElectionYear(electionDate) } : {}),
    ...(getString(row, "election_type") ? { electionType: getString(row, "election_type") } : {}),
    ...(contributorNameFromRow(row) ? { contributorName: contributorNameFromRow(row) } : {}),
    ...(getString(row, "contribution_type") ? { contributorType: getString(row, "contribution_type") } : {}),
    ...(getString(row, "contribution_mode") ? { contributionMode: getString(row, "contribution_mode") } : {}),
    ...(getString(row, "occupation") ? { occupation: getString(row, "occupation") } : {}),
    ...(getString(row, "other_occupation") ? { otherOccupation: getString(row, "other_occupation") } : {}),
    ...(getString(row, "employer") ? { employer: getString(row, "employer") } : {}),
    ...(getString(row, "city") ? { city: getString(row, "city") } : {}),
    ...(getString(row, "state") ? { state: getString(row, "state") } : {}),
    ...(getString(row, "zip") ? { zip: getString(row, "zip") } : {}),
    amount,
    ...(getString(row, "receipt_date") ? { receiptDate: getString(row, "receipt_date") } : {}),
    ...(getString(row, "statement_type") ? { statementType: getString(row, "statement_type") } : {}),
  };
}

function supportOpposeFromKref(value: string | undefined): "support" | "oppose" | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }
  if (normalized.startsWith("support")) {
    return "support";
  }
  if (normalized.startsWith("oppose")) {
    return "oppose";
  }
  return undefined;
}

export function kentuckyKrefIndependentExpenditureRecordFromRow(
  row: KentuckyKrefCsvRow
): KentuckyKrefIndependentExpenditureRecord | null {
  const amount = parseAmount(getString(row, "amount"));
  if (amount === undefined || amount <= 0) {
    return null;
  }
  const electionDate = getString(row, "election_date");
  return {
    ...(getString(row, "to_whome_made", "to_whom_made") ? {
      toWhomMade: getString(row, "to_whome_made", "to_whom_made"),
    } : {}),
    ...(getString(row, "name") ? { spenderName: getString(row, "name") } : {}),
    ...(getString(row, "date") ? { date: getString(row, "date") } : {}),
    ...(getString(row, "candidate_name") ? { candidateName: getString(row, "candidate_name") } : {}),
    ...(supportOpposeFromKref(getString(row, "support_oppose")) ? {
      supportOppose: supportOpposeFromKref(getString(row, "support_oppose")),
    } : {}),
    ...(getString(row, "office_ballot_measure") ? {
      officeOrBallotMeasure: getString(row, "office_ballot_measure"),
    } : {}),
    ...(electionDate ? { electionDate } : {}),
    ...(parseElectionYear(electionDate) !== undefined ? { electionYear: parseElectionYear(electionDate) } : {}),
    amount,
  };
}

export async function downloadKentuckyKrefContributions(
  input: KentuckyKrefContributionExportInput,
  options: KentuckyKrefClientOptions = {}
): Promise<KentuckyKrefContributionRecord[]> {
  const csv = await fetchKentuckyKrefText(buildKentuckyKrefContributionExportUrl(input), options);
  return parseKentuckyKrefCsvRows(csv)
    .map(kentuckyKrefContributionRecordFromRow)
    .filter((row): row is KentuckyKrefContributionRecord => row !== null);
}

export function downloadKentuckyKrefCandidateContributions(
  input: Omit<KentuckyKrefContributionExportInput, "contributionSearchType">,
  options: KentuckyKrefClientOptions = {}
): Promise<KentuckyKrefContributionRecord[]> {
  return downloadKentuckyKrefContributions({ ...input, contributionSearchType: "Candidate" }, options);
}

export function downloadKentuckyKrefIeOnlyCommitteeContributions(
  input: Omit<KentuckyKrefContributionExportInput, "contributionSearchType" | "organizationType"> = {},
  options: KentuckyKrefClientOptions = {}
): Promise<KentuckyKrefContributionRecord[]> {
  return downloadKentuckyKrefContributions(
    {
      ...input,
      contributionSearchType: "Organization",
      organizationType: KENTUCKY_KREF_IE_ONLY_ORGANIZATION_TYPE,
    },
    options
  );
}

export async function downloadKentuckyKrefIndependentExpenditures(
  input: KentuckyKrefIndependentExpenditureExportInput,
  options: KentuckyKrefClientOptions = {}
): Promise<KentuckyKrefIndependentExpenditureRecord[]> {
  const csv = await fetchKentuckyKrefText(buildKentuckyKrefIndependentExpenditureExportUrl(input), options);
  return parseKentuckyKrefCsvRows(csv)
    .map(kentuckyKrefIndependentExpenditureRecordFromRow)
    .filter((row): row is KentuckyKrefIndependentExpenditureRecord => row !== null);
}

export async function fetchKentuckyKrefCandidateElectionDateOptions(
  options: KentuckyKrefClientOptions = {}
): Promise<KentuckyKrefDropdownOption[]> {
  const html = await fetchKentuckyKrefText(
    buildKentuckyKrefPublicSearchPageUrl("candidate_contributions"),
    options,
    "text/html,text/plain;q=0.9,*/*;q=0.1"
  );
  return parseKentuckyKrefElectionDateOptions(html);
}

export async function fetchKentuckyKrefIndependentExpenditureElectionDateOptions(
  options: KentuckyKrefClientOptions = {}
): Promise<KentuckyKrefDropdownOption[]> {
  const html = await fetchKentuckyKrefText(
    buildKentuckyKrefPublicSearchPageUrl("independent_expenditures"),
    options,
    "text/html,text/plain;q=0.9,*/*;q=0.1"
  );
  return parseKentuckyKrefElectionDateOptions(html);
}
