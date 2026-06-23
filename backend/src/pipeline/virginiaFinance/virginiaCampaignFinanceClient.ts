export const VIRGINIA_CAMPAIGN_FINANCE_BASE_URL = "https://cfreports.elections.virginia.gov";
export const VIRGINIA_CAMPAIGN_FINANCE_DEFAULT_TIMEOUT_MS = 30_000;
export const VIRGINIA_CANDIDATE_COMMITTEE_TYPE = "Candidate Campaign Committee";

export type VirginiaCampaignFinanceClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class VirginiaCampaignFinanceClientError extends Error {
  constructor(
    public readonly code: VirginiaCampaignFinanceClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "VirginiaCampaignFinanceClientError";
  }
}

export type VirginiaCampaignFinanceClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type VirginiaCommitteeSearchInput = {
  committeeName: string;
  committeeType?: string;
};

export type VirginiaCommitteeSearchResult = {
  committeeId: string;
  committeeName: string;
  candidateName: string | null;
  committeeType: string;
  reportsUrl: string;
  sourceUrl: string;
};

export type VirginiaCommitteeReportList = {
  committeeId: string;
  committeeName: string | null;
  committeeCode: string | null;
  statementOfOrganizationUrl: string | null;
  scheduledReportIds: number[];
  largeContributionReportIds: number[];
  sourceUrl: string;
};

export type VirginiaReportHeader = {
  committeeCode: string | null;
  committeeName: string | null;
  reportYear: number | null;
  reportType: string | null;
  filingDate: string | null;
  startDate: string | null;
  endDate: string | null;
  electionCycle: string | null;
  officeSought: string | null;
};

export type VirginiaScheduleAContribution = {
  contributorName: string | null;
  isIndividual: boolean | null;
  employer: string | null;
  occupationOrTypeOfBusiness: string | null;
  transactionDate: string | null;
  amount: number;
  totalToDate: number | null;
};

export type VirginiaCampaignFinanceReport = {
  header: VirginiaReportHeader;
  scheduleA: VirginiaScheduleAContribution[];
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new VirginiaCampaignFinanceClientError("invalid_request", `${fieldName} is required`);
  }
  return trimmed;
}

function normalizeReportId(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new VirginiaCampaignFinanceClientError("invalid_request", `Invalid Virginia report id: ${value}`);
  }
  return value;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function decodeNumericHtmlEntity(match: string, code: string, radix: number): string {
  const parsed = Number.parseInt(code, radix);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 0x10ffff) {
    return match;
  }
  try {
    return String.fromCodePoint(parsed);
  } catch {
    return match;
  }
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&#(\d+);/g, (match, code: string) => decodeNumericHtmlEntity(match, code, 10))
    .replace(/&#x([0-9a-f]+);/gi, (match, code: string) => decodeNumericHtmlEntity(match, code, 16))
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripHtml(value: string): string {
  return decodeHtmlEntities(value.replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function absoluteVirginiaUrl(value: string): string {
  return new URL(value, VIRGINIA_CAMPAIGN_FINANCE_BASE_URL).toString();
}

function decodeCommitteeId(value: string): string {
  try {
    return decodeURIComponent(value).trim();
  } catch {
    return value.trim();
  }
}

function parseAmount(raw: string | null): number | null {
  const normalized = (raw ?? "").replace(/[$,]/g, "").trim();
  if (!normalized || !/^-?\d+(?:\.\d+)?$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? Math.round(parsed * 100) / 100 : null;
}

function parseInteger(raw: string | null): number | null {
  const normalized = raw?.trim() ?? "";
  if (!/^\d+$/.test(normalized)) {
    return null;
  }
  const parsed = Number.parseInt(normalized, 10);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function xmlTagPattern(tagName: string): string {
  return `(?:[A-Za-z_][\\w.-]*:)?${tagName}`;
}

function firstXmlText(xml: string, tagName: string): string | null {
  const tag = xmlTagPattern(tagName);
  const match = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)</${tag}>`, "i").exec(xml);
  if (!match?.[1]) {
    return null;
  }
  return decodeHtmlEntities(match[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1"))
    .replace(/\s+/g, " ")
    .trim() || null;
}

function xmlBlocks(xml: string, tagName: string): string[] {
  const tag = xmlTagPattern(tagName);
  const blocks: string[] = [];
  const regex = new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*?</${tag}>`, "gi");
  let match: RegExpExecArray | null;
  while ((match = regex.exec(xml))) {
    blocks.push(match[0]);
  }
  return blocks;
}

function xmlBlock(xml: string, tagName: string): string | null {
  return xmlBlocks(xml, tagName)[0] ?? null;
}

function xmlAttribute(block: string, attributeName: string): string | null {
  const match = new RegExp(`\\b${attributeName}\\s*=\\s*["']([^"']*)["']`, "i").exec(block);
  return match?.[1] ? decodeHtmlEntities(match[1]).trim() : null;
}

function parseVirginiaBoolean(raw: string | null): boolean | null {
  const normalized = raw?.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  if (["true", "1", "yes"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "no"].includes(normalized)) {
    return false;
  }
  return null;
}

function combineNameParts(parts: Array<string | null>): string | null {
  const name = parts
    .map((part) => part?.trim() ?? "")
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  return name || null;
}

function contributorNameFromBlock(contributorBlock: string): string | null {
  return (
    firstXmlText(contributorBlock, "OrganizationName") ??
    combineNameParts([
      firstXmlText(contributorBlock, "FirstName"),
      firstXmlText(contributorBlock, "MiddleName"),
      firstXmlText(contributorBlock, "LastName"),
      firstXmlText(contributorBlock, "Suffix"),
    ]) ??
    firstXmlText(contributorBlock, "LastName")
  );
}

async function fetchVirginiaCampaignFinanceText(
  url: string,
  options: VirginiaCampaignFinanceClientOptions = {}
): Promise<string> {
  const timeoutMs = options.timeoutMs ?? VIRGINIA_CAMPAIGN_FINANCE_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  let response: Response;
  try {
    response = await (options.fetchImpl ?? fetch)(url, {
      headers: { accept: "text/html,application/xml,text/xml;q=0.9,*/*;q=0.1" },
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new VirginiaCampaignFinanceClientError(
        "network_error",
        `Virginia campaign finance request timed out after ${timeoutMs}ms for ${url}`
      );
    }
    throw new VirginiaCampaignFinanceClientError(
      "network_error",
      `Virginia campaign finance request failed for ${url}: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    throw new VirginiaCampaignFinanceClientError(
      "http_error",
      `Virginia campaign finance request failed: ${response.status} ${response.statusText}`,
      response.status
    );
  }

  return await response.text();
}

export function buildVirginiaCommitteeSearchUrl(input: VirginiaCommitteeSearchInput): string {
  const url = new URL(VIRGINIA_CAMPAIGN_FINANCE_BASE_URL);
  url.searchParams.set("CommitteeName", requireNonEmpty(input.committeeName, "Virginia committee search name"));
  url.searchParams.set("CommitteeType", input.committeeType?.trim() || VIRGINIA_CANDIDATE_COMMITTEE_TYPE);
  return url.toString();
}

export function buildVirginiaCommitteeReportsUrl(committeeId: string): string {
  const normalized = requireNonEmpty(committeeId, "Virginia committee id");
  return absoluteVirginiaUrl(`/Committee/Index/${encodeURIComponent(normalized)}`);
}

export function buildVirginiaReportPageUrl(reportId: number): string {
  return absoluteVirginiaUrl(`/Report/Index/${normalizeReportId(reportId)}`);
}

export function buildVirginiaReportXmlUrl(reportId: number): string {
  return absoluteVirginiaUrl(`/Report/ReportXML/${normalizeReportId(reportId)}`);
}

function parseCommitteeSearchRow(rowHtml: string, sourceUrl: string): VirginiaCommitteeSearchResult | null {
  const committeeNameMatch = /<td\b[^>]*class=["'][^"']*\bcommitteeName\b[^"']*["'][^>]*>([\s\S]*?)<\/td>/i.exec(
    rowHtml
  );
  const candidateNameMatch = /<td\b[^>]*class=["'][^"']*\bcandidateName\b[^"']*["'][^>]*>([\s\S]*?)<\/td>/i.exec(
    rowHtml
  );
  const committeeTypeMatch = /<td\b[^>]*class=["'][^"']*\bcommitteeType\b[^"']*["'][^>]*>([\s\S]*?)<\/td>/i.exec(
    rowHtml
  );
  const hrefMatch = /<a\b[^>]*href=["']([^"']*\/Committee\/Index\/([^"'/]+))["'][^>]*>/i.exec(rowHtml);
  if (!committeeNameMatch?.[1] || !committeeTypeMatch?.[1] || !hrefMatch?.[1] || !hrefMatch?.[2]) {
    return null;
  }

  const committeeName = stripHtml(committeeNameMatch[1]);
  const committeeType = stripHtml(committeeTypeMatch[1]);
  if (!committeeName || !committeeType) {
    return null;
  }
  const candidateName = candidateNameMatch?.[1] ? stripHtml(candidateNameMatch[1]) || null : null;
  const committeeId = decodeCommitteeId(hrefMatch[2]);
  if (!committeeId) {
    return null;
  }
  const reportsUrl = absoluteVirginiaUrl(hrefMatch[1]);
  return {
    committeeId,
    committeeName,
    candidateName,
    committeeType,
    reportsUrl,
    sourceUrl,
  };
}

export function parseVirginiaCommitteeSearchResults(
  html: string,
  sourceUrl = VIRGINIA_CAMPAIGN_FINANCE_BASE_URL
): VirginiaCommitteeSearchResult[] {
  const results: VirginiaCommitteeSearchResult[] = [];
  const rowRegex = /<tr\b[\s\S]*?<\/tr>/gi;
  let match: RegExpExecArray | null;
  while ((match = rowRegex.exec(html))) {
    const row = parseCommitteeSearchRow(match[0], sourceUrl);
    if (row) {
      results.push(row);
    }
  }
  return results;
}

function uniqueReportIds(sectionHtml: string): number[] {
  const ids: number[] = [];
  const seen = new Set<number>();
  const regex = /\/Report\/Index\/(\d+)/gi;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(sectionHtml))) {
    const reportId = Number.parseInt(match[1] ?? "", 10);
    if (Number.isSafeInteger(reportId) && reportId > 0 && !seen.has(reportId)) {
      ids.push(reportId);
      seen.add(reportId);
    }
  }
  return ids;
}

function htmlSectionById(html: string, id: string, nextId?: string): string {
  const startMatch = new RegExp(`<div\\b[^>]*id=["']${id}["'][^>]*>`, "i").exec(html);
  if (!startMatch) {
    return "";
  }
  const start = startMatch.index;
  const end = nextId
    ? new RegExp(`<div\\b[^>]*id=["']${nextId}["'][^>]*>`, "i").exec(html.slice(start + startMatch[0].length))
    : null;
  return end ? html.slice(start, start + startMatch[0].length + end.index) : html.slice(start);
}

export function parseVirginiaCommitteeReportList(
  html: string,
  input: { committeeId: string; sourceUrl?: string }
): VirginiaCommitteeReportList {
  const committeeId = requireNonEmpty(input.committeeId, "Virginia committee id");
  const sourceUrl = input.sourceUrl ?? buildVirginiaCommitteeReportsUrl(committeeId);
  const titleMatch = /<h2\b[^>]*(?:title=["']([^"']*)["'])?[^>]*>([\s\S]*?)<\/h2>/i.exec(html);
  const title = stripHtml(titleMatch?.[1] ?? titleMatch?.[2] ?? "");
  const reportsForMatch = /^Reports for\s+(.+?)(?:\s+\(([^()]+)\))?$/i.exec(title);
  const statementMatch = /<a\b[^>]*href=["']([^"']*\/Printable\/StatementOfOrganization\/[^"']+)["'][^>]*>/i.exec(
    html
  );
  const scheduledSection = htmlSectionById(html, "ScheduledReports", "LargeContributionReports");
  const largeContributionSection = htmlSectionById(html, "LargeContributionReports");

  return {
    committeeId,
    committeeName: reportsForMatch?.[1]?.trim() || null,
    committeeCode: reportsForMatch?.[2]?.trim() || null,
    statementOfOrganizationUrl: statementMatch?.[1] ? absoluteVirginiaUrl(statementMatch[1]) : null,
    scheduledReportIds: uniqueReportIds(scheduledSection),
    largeContributionReportIds: uniqueReportIds(largeContributionSection),
    sourceUrl,
  };
}

export function parseVirginiaCampaignFinanceReportXml(xml: string): VirginiaCampaignFinanceReport {
  const headerBlock = xmlBlock(xml, "ReportHeader") ?? xml;
  const scheduleA = xmlBlocks(xmlBlock(xml, "ScheduleA") ?? "", "LiA")
    .map((block): VirginiaScheduleAContribution | null => {
      const contributorBlock = xmlBlock(block, "Contributor");
      if (!contributorBlock) {
        return null;
      }
      const amount = parseAmount(firstXmlText(block, "Amount"));
      if (amount === null) {
        return null;
      }
      return {
        contributorName: contributorNameFromBlock(contributorBlock),
        isIndividual: parseVirginiaBoolean(xmlAttribute(contributorBlock, "IsIndividual")),
        employer: firstXmlText(contributorBlock, "NameOfEmployer"),
        occupationOrTypeOfBusiness: firstXmlText(contributorBlock, "OccupationOrTypeOfBusiness"),
        transactionDate: firstXmlText(block, "TransactionDate"),
        amount,
        totalToDate: parseAmount(firstXmlText(block, "TotalToDate")),
      };
    })
    .filter((row): row is VirginiaScheduleAContribution => row !== null);

  return {
    header: {
      committeeCode: firstXmlText(headerBlock, "CommitteeCode"),
      committeeName: firstXmlText(headerBlock, "CommitteeName"),
      reportYear: parseInteger(firstXmlText(headerBlock, "ReportYear")),
      reportType: firstXmlText(headerBlock, "ReportType"),
      filingDate: firstXmlText(headerBlock, "FilingDate"),
      startDate: firstXmlText(headerBlock, "StartDate"),
      endDate: firstXmlText(headerBlock, "EndDate"),
      electionCycle: firstXmlText(headerBlock, "ElectionCycle"),
      officeSought: firstXmlText(headerBlock, "OfficeSought"),
    },
    scheduleA,
  };
}

export async function searchVirginiaCandidateCommittees(
  input: VirginiaCommitteeSearchInput,
  options: VirginiaCampaignFinanceClientOptions = {}
): Promise<VirginiaCommitteeSearchResult[]> {
  const sourceUrl = buildVirginiaCommitteeSearchUrl(input);
  const html = await fetchVirginiaCampaignFinanceText(sourceUrl, options);
  return parseVirginiaCommitteeSearchResults(html, sourceUrl);
}

export async function fetchVirginiaCommitteeReportList(
  committeeId: string,
  options: VirginiaCampaignFinanceClientOptions = {}
): Promise<VirginiaCommitteeReportList> {
  const sourceUrl = buildVirginiaCommitteeReportsUrl(committeeId);
  const html = await fetchVirginiaCampaignFinanceText(sourceUrl, options);
  return parseVirginiaCommitteeReportList(html, { committeeId, sourceUrl });
}

export async function fetchVirginiaCampaignFinanceReport(
  reportId: number,
  options: VirginiaCampaignFinanceClientOptions = {}
): Promise<VirginiaCampaignFinanceReport> {
  const xml = await fetchVirginiaCampaignFinanceText(buildVirginiaReportXmlUrl(reportId), options);
  return parseVirginiaCampaignFinanceReportXml(xml);
}
