const OKLAHOMA_GUARDIAN_CANDIDATE_DETAIL_BASE_URL =
  "https://guardian.ok.gov/PublicSite/SearchPages/OrganizationDetail.aspx";
const DEFAULT_TIMEOUT_MS = 15_000;

export type OklahomaGuardianCandidateDetail = {
  organizationId: string;
  candidateName: string;
  officeName: string;
  district: string | null;
  electionYears: number[];
  sourceUrl: string;
};

function normalizeOrganizationId(value: string): string {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    throw new Error(`Invalid Oklahoma Guardian organization ID: ${value}`);
  }
  return normalized;
}

export function buildOklahomaGuardianCandidateDetailUrl(organizationId: string): string {
  const url = new URL(OKLAHOMA_GUARDIAN_CANDIDATE_DETAIL_BASE_URL);
  url.searchParams.set("OrganizationID", normalizeOrganizationId(organizationId));
  return url.toString();
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
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function htmlText(value: string): string {
  return decodeHtmlEntities(value.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

function readSpanText(html: string, id: string): string {
  const escapedId = id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = new RegExp(`<span\\b[^>]*\\bid=["']${escapedId}["'][^>]*>([\\s\\S]*?)<\\/span>`, "i").exec(html);
  return match ? htmlText(match[1] ?? "") : "";
}

function readElectionYears(html: string): number[] {
  const years = new Set<number>();
  const electionSpanPattern =
    /<span\b[^>]*\bid=["'][^"']*dgdCampaigns_[^"']*_lblElection["'][^>]*>([\s\S]*?)<\/span>/gi;
  for (const match of html.matchAll(electionSpanPattern)) {
    for (const yearMatch of htmlText(match[1] ?? "").matchAll(/\b(20\d{2})\b/g)) {
      const year = Number(yearMatch[1]);
      if (Number.isInteger(year)) {
        years.add(year);
      }
    }
  }
  return [...years].sort((left, right) => left - right);
}

export function parseOklahomaGuardianCandidateDetailHtml(input: {
  html: string;
  organizationId: string;
  sourceUrl?: string;
}): OklahomaGuardianCandidateDetail {
  const organizationId = normalizeOrganizationId(input.organizationId);
  const sourceUrl = input.sourceUrl ?? buildOklahomaGuardianCandidateDetailUrl(organizationId);
  const pageOrganizationId = readSpanText(input.html, "ctl00_Content_lblCandID");
  const candidateName = readSpanText(input.html, "ctl00_Content_lblCandName");
  const officeName = readSpanText(input.html, "ctl00_Content_lblCandOffice");
  const district = readSpanText(input.html, "ctl00_Content_lblCandDistrict") || null;
  const electionYears = readElectionYears(input.html);

  if (pageOrganizationId !== organizationId) {
    throw new Error(
      `Oklahoma Guardian candidate detail ID mismatch: requested ${organizationId}, received ${pageOrganizationId || "missing"}`
    );
  }
  if (!candidateName || !officeName || electionYears.length === 0) {
    throw new Error(`Incomplete Oklahoma Guardian candidate detail for organization ${organizationId}`);
  }

  return {
    organizationId,
    candidateName,
    officeName,
    district,
    electionYears,
    sourceUrl,
  };
}

export async function fetchOklahomaGuardianCandidateDetail(input: {
  organizationId: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<OklahomaGuardianCandidateDetail> {
  const organizationId = normalizeOrganizationId(input.organizationId);
  const sourceUrl = buildOklahomaGuardianCandidateDetailUrl(organizationId);
  const timeoutMs = input.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await (input.fetchImpl ?? fetch)(sourceUrl, {
      headers: { accept: "text/html,application/xhtml+xml" },
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Oklahoma Guardian candidate detail returned HTTP ${response.status} for ${organizationId}`);
    }
    return parseOklahomaGuardianCandidateDetailHtml({
      html: await response.text(),
      organizationId,
      sourceUrl,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`Oklahoma Guardian candidate detail timed out after ${timeoutMs}ms for ${organizationId}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}
