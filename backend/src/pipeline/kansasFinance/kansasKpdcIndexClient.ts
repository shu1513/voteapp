// KPDC scanned-archive client (plan-kansas-finance.md, Phase 1).
//
// The archive under kansas.gov/ethics/CFAScanned/ is plain static hosting:
// hand-authored HTML link trees per office family and cycle (e.g.
// House/2026ElecCycle/HLinks2026EC.htm) pointing at scanned-PDF filings.
// Facts verified live 2026-08-28:
// - Hrefs are absolute and mixed-host — the House tree links
//   `https://kansas.gov/...` while the IE tree links
//   `https://www.kansas.gov/...`; bare kansas.gov 302s to www.kansas.gov,
//   so URLs are normalized to www.kansas.gov and redirects followed.
// - Index rows are hand-written `<td>` runs (often unclosed): a district
//   cell, a filer-name cell ("Helwig, Dale"), then one link cell per filing
//   whose text is the period ("AT", "202601", "202607", ...).
// - Filename grammar over the live 2026 House tree (810 links):
//   `<filerCode>_<suffix>.pdf` with suffix AT (260), YYYYMM reports
//   (202607 x251, 202601 x125), 2026PLF last-minute (78), amend-prefixed
//   replacements (amend2607 x49, amend2601 x29, amendAT x5), AffYYMM (10),
//   TermYYMM (1). Filer codes may carry digits beyond the district
//   (H096JM2), and IE statements use IE_<code>_YYMM (IE_KC1_2607).
// - Across the 2024 House/Senate and 2022 statewide trees (2026-09-02): a
//   second and later replacement is numbered into the prefix (2amend2410,
//   3amend2407, 4amend2410, 2amendAT), the general-election last-minute
//   report is YYYYGLF beside the primary's YYYYPLF, and affidavits also
//   appear lowercase (aff2407). With those, all eight live trees classify
//   with zero unknowns.

import { KANSAS_CFR_OFFICE_CODES, KansasCfrClientError } from "./kansasCfrViewerClient.js";
import { decodeKansasHtmlText } from "./kansasCfrViewerParsers.js";
import type { KansasCfrOffice } from "./kansasFinanceEligibleOffices.js";

export const KANSAS_KPDC_SCAN_BASE_URL = "https://www.kansas.gov/ethics/CFAScanned/";
export const DEFAULT_KANSAS_KPDC_USER_AGENT =
  "Mozilla/5.0 (compatible; VoteApp election research; +https://electionssimplified.com)";
export const DEFAULT_KANSAS_KPDC_TIMEOUT_MS = 120_000;
// Largest live artifact class is a multi-page scan; the House index is 458 KB.
export const KANSAS_KPDC_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

const KANSAS_KPDC_HOSTS = new Set(["kansas.gov", "www.kansas.gov"]);
// 91 of the live House tree's 810 links (amendments, affidavits) still use
// absolute URLs on the DEAD ethics.ks.gov host with the /CFAScanned/ path
// at its root (http://ethics.ks.gov/CFAScanned/...). The same artifacts are
// served at www.kansas.gov/ethics/CFAScanned/... (verified live: the Perry
// amendment fixture), so those hosts are rewritten rather than dropped.
const KANSAS_KPDC_DEAD_HOSTS = new Set(["ethics.ks.gov", "ethics.kansas.gov"]);

export type KansasKpdcFetchOptions = {
  timeoutMs?: number;
  userAgent?: string;
  maxResponseBytes?: number;
  fetchImpl?: typeof fetch;
};

/**
 * Resolve a path against the CFAScanned base and pin it to the archive:
 * https, default port, no credentials, www.kansas.gov, and a path under
 * /ethics/CFAScanned/ (URL resolution normalizes `..`, so a
 * traversal-shaped path fails the prefix check rather than escaping it).
 */
export function buildKansasKpdcUrl(pathOrUrl: string): string {
  const url = new URL(pathOrUrl, KANSAS_KPDC_SCAN_BASE_URL);
  const refuse = (): never => {
    throw new KansasCfrClientError("invalid_request", `not a KPDC CFAScanned URL: ${pathOrUrl}`);
  };
  if (url.protocol !== "https:" && url.protocol !== "http:") refuse();
  if (url.port !== "" || url.username !== "" || url.password !== "") refuse();
  if (KANSAS_KPDC_DEAD_HOSTS.has(url.hostname) && url.pathname.startsWith("/CFAScanned/")) {
    url.pathname = `/ethics${url.pathname}`;
  } else if (!KANSAS_KPDC_HOSTS.has(url.hostname)) {
    refuse();
  }
  if (!url.pathname.startsWith("/ethics/CFAScanned/")) refuse();
  url.protocol = "https:";
  url.hostname = "www.kansas.gov";
  return url.toString();
}

async function fetchKansasKpdcResource(
  pathOrUrl: string,
  options: KansasKpdcFetchOptions
): Promise<{ url: string; body: Buffer; contentType: string | null }> {
  const url = buildKansasKpdcUrl(pathOrUrl);
  const timeoutMs = options.timeoutMs ?? DEFAULT_KANSAS_KPDC_TIMEOUT_MS;
  const maxResponseBytes = options.maxResponseBytes ?? KANSAS_KPDC_MAX_RESPONSE_BYTES;
  const fetchImpl = options.fetchImpl ?? fetch;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  // The timer stays armed through body consumption (same lesson as the
  // viewer client: headers can arrive and the body then stall).
  try {
    let response: Response;
    try {
      response = await fetchImpl(url, {
        headers: { "User-Agent": options.userAgent ?? DEFAULT_KANSAS_KPDC_USER_AGENT },
        redirect: "follow",
        signal: controller.signal,
      });
    } catch (error) {
      throw new KansasCfrClientError(
        "network_error",
        `GET ${url} failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
    if (!response.ok) {
      throw new KansasCfrClientError("http_error", `GET ${url} answered ${response.status}`, response.status);
    }
    const landed = new URL(response.url || url);
    if (!KANSAS_KPDC_HOSTS.has(landed.hostname) || !landed.pathname.startsWith("/ethics/CFAScanned/")) {
      throw new KansasCfrClientError(
        "bad_response",
        `GET ${url} redirected outside the archive to ${response.url}`
      );
    }
    const declaredLength = Number(response.headers.get("content-length") ?? Number.NaN);
    if (Number.isFinite(declaredLength) && declaredLength > maxResponseBytes) {
      throw new KansasCfrClientError(
        "bad_response",
        `GET ${url} declared ${declaredLength} bytes (limit ${maxResponseBytes})`
      );
    }
    let body: Buffer;
    try {
      body = Buffer.from(await response.arrayBuffer());
    } catch (error) {
      throw new KansasCfrClientError(
        "network_error",
        `GET ${url} body read failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
    if (body.byteLength > maxResponseBytes) {
      throw new KansasCfrClientError(
        "bad_response",
        `GET ${url} answered ${body.byteLength} bytes (limit ${maxResponseBytes})`
      );
    }
    return { url, body, contentType: response.headers.get("content-type") };
  } finally {
    clearTimeout(timer);
  }
}

/** GET one link-tree index page (HLinks2026EC.htm etc.) as UTF-8 HTML. */
export async function fetchKansasKpdcIndexPage(
  pathOrUrl: string,
  options: KansasKpdcFetchOptions = {}
): Promise<{ url: string; html: string }> {
  const resource = await fetchKansasKpdcResource(pathOrUrl, options);
  const html = resource.body.toString("utf8");
  if (!html.includes("<")) {
    throw new KansasCfrClientError("bad_response", `GET ${resource.url} did not answer HTML`);
  }
  return { url: resource.url, html };
}

/** GET one scanned filing; the body must carry the %PDF magic. */
export async function fetchKansasKpdcPdf(
  pathOrUrl: string,
  options: KansasKpdcFetchOptions = {}
): Promise<{ url: string; bytes: Uint8Array }> {
  const resource = await fetchKansasKpdcResource(pathOrUrl, options);
  const bytes = new Uint8Array(resource.body);
  if (bytes.length < 4 || String.fromCharCode(...bytes.slice(0, 4)) !== "%PDF") {
    throw new KansasCfrClientError("bad_response", `GET ${resource.url} did not answer a PDF`);
  }
  return { url: resource.url, bytes };
}

// ---------------------------------------------------------------------------
// Index-tree parsing.

export type KansasKpdcIndexPdfLink = {
  /** Absolute URL, normalized to https://www.kansas.gov. */
  url: string;
  /** Last path segment, e.g. "H001DH_202607.pdf". */
  fileName: string;
  /** The anchor's own text — the period column ("AT", "202607", ...). */
  linkText: string;
  /**
   * Whitespace-collapsed text of the containing hand-authored table row —
   * district and filer name plus the row's link texts. Interpretation is
   * per-tree (candidate trees: "01 Helwig, Dale ..."); callers regex what
   * they need.
   */
  rowText: string;
};

/**
 * Every PDF link of a CFAScanned index page, with its row context. The
 * markup is hand-authored (unclosed `<td>`s, inline styling), so rows are
 * segmented on `<tr` boundaries and reduced to text rather than modeled.
 */
export function parseKansasKpdcIndexPdfLinks(html: string, baseUrl: string): KansasKpdcIndexPdfLink[] {
  const links: KansasKpdcIndexPdfLink[] = [];
  const rowChunks = html.split(/<tr[\s>]/i);
  for (const chunk of rowChunks) {
    const anchors = [...chunk.matchAll(/<a[^>]+href="([^"]+\.pdf)"[^>]*>([\s\S]*?)<\/a>/gi)];
    if (anchors.length === 0) continue;
    const rowText = decodeKansasHtmlText(chunk.replace(/<[^>]*>/g, " "))
      .replace(/\s+/g, " ")
      .trim();
    for (const anchor of anchors) {
      let url: string;
      try {
        url = buildKansasKpdcUrl(new URL(decodeKansasHtmlText(anchor[1]!), baseUrl).toString());
      } catch {
        continue; // off-site or non-https link — not an archive artifact
      }
      const fileName = new URL(url).pathname.split("/").pop() ?? "";
      const linkText = decodeKansasHtmlText(anchor[2]!.replace(/<[^>]*>/g, " "))
        .replace(/\s+/g, " ")
        .trim();
      links.push({ url, fileName, linkText, rowText });
    }
  }
  return links;
}

// ---------------------------------------------------------------------------
// Filename grammar.

export type KansasKpdcFilingKind =
  | "appointment_of_treasurer"
  | "report"
  | "last_minute"
  | "affidavit"
  | "termination"
  | "unknown";

export type KansasKpdcFileNameInfo = {
  fileName: string;
  /** Everything before the LAST underscore (IE statements: "IE_KC1"). */
  filerCode: string;
  /** Everything after the last underscore, without ".pdf". */
  suffix: string;
  kind: KansasKpdcFilingKind;
  /**
   * Normalized YYYYMM when the suffix carries one (2607 -> 202607). For a
   * report it is the month the report was DUE (202601 = the 2025 annual
   * due 1/10/2026, 202607 = the pre-primary due 7/27/2026), not the period.
   */
  periodKey: string | null;
  /** amend-prefixed suffixes are full-replacement filings. */
  amendment: boolean;
  /** Replacement sequence from the prefix: amend = 1, 2amend = 2, ...; null on an original. */
  amendmentOrdinal: number | null;
};

/**
 * Classify a CFAScanned filename. Unrecognized suffixes come back as
 * `unknown` rather than throwing — the inventory layer fails closed on
 * them; this function only names what it can prove.
 */
export function parseKansasKpdcFileName(fileName: string): KansasKpdcFileNameInfo {
  const base = fileName.replace(/\.pdf$/i, "");
  const separator = base.lastIndexOf("_");
  const filerCode = separator > 0 ? base.slice(0, separator) : base;
  let suffix = separator > 0 ? base.slice(separator + 1) : "";
  const amend = /^(\d*)amend/i.exec(suffix);
  const amendmentOrdinal = amend === null ? null : amend[1] === "" ? 1 : Number.parseInt(amend[1]!, 10);
  const amendPrefix = amend?.[0] ?? "";
  if (amend !== null) suffix = suffix.slice(amendPrefix.length);

  const info = (kind: KansasKpdcFilingKind, periodKey: string | null): KansasKpdcFileNameInfo => ({
    fileName,
    filerCode,
    suffix: amendPrefix + suffix,
    kind,
    periodKey,
    amendment: amend !== null,
    amendmentOrdinal,
  });

  if (suffix === "AT") return info("appointment_of_treasurer", null);
  if (/^\d{6}$/.test(suffix)) return info("report", suffix);
  if (/^\d{4}$/.test(suffix)) return info("report", `20${suffix}`);
  if (/^\d{4}[PG]LF$/.test(suffix)) return info("last_minute", null);
  const affidavit = /^[Aa]ff(\d{4})$/.exec(suffix);
  if (affidavit) return info("affidavit", `20${affidavit[1]}`);
  const termination = /^Term(\d{4})$/.exec(suffix);
  if (termination) return info("termination", `20${termination[1]}`);
  return info("unknown", null);
}

// ---------------------------------------------------------------------------
// Candidate trees.

/**
 * Candidate link tree for an office family and election year, as the KPDC
 * "View Submitted Forms and Reports" page links them (verified 2026-09-02):
 * House/<y>ElecCycle/HLinks<y>EC.htm; Senate/<y>ElecCycle/SLinks<y>EC.htm in
 * presidential years and Senate/<y>SpecialElection/SLinks<y>SpecialElection.htm
 * for the other even years (2014, 2018, 2022, 2026 specials); one
 * StWide/<y>ElecCycle/SWLinks<y>EC.htm tree for all five statewide offices.
 */
export function kansasKpdcCandidateTreePath(office: KansasCfrOffice, electionYear: number): string {
  if (!Number.isSafeInteger(electionYear) || electionYear < 2000 || electionYear > 2100) {
    throw new Error(`Invalid Kansas election year: ${electionYear}`);
  }
  if (office.code === KANSAS_CFR_OFFICE_CODES.stateRepresentative) {
    return `House/${electionYear}ElecCycle/HLinks${electionYear}EC.htm`;
  }
  if (office.code === KANSAS_CFR_OFFICE_CODES.stateSenator) {
    if (electionYear % 4 === 0) return `Senate/${electionYear}ElecCycle/SLinks${electionYear}EC.htm`;
    if (electionYear % 2 === 0) return `Senate/${electionYear}SpecialElection/SLinks${electionYear}SpecialElection.htm`;
    throw new Error(`No Kansas State Senator KPDC tree for a ${electionYear} election`);
  }
  if (kansasKpdcStatewideFilerPrefix(office) === null) {
    throw new Error(`No KPDC candidate tree for Kansas office ${office.label}`);
  }
  return `StWide/${electionYear}ElecCycle/SWLinks${electionYear}EC.htm`;
}

/**
 * Statewide filer codes carry the office as "SW0n" (SW01 Governor, SW02
 * Attorney General, SW03 Insurance Commissioner, SW04 Secretary of State,
 * SW05 State Treasurer — read off the 2026 tree's section headings). These
 * are NOT the viewer's office codes (Attorney General is 3 there). Null for
 * districted offices, whose trees hold one office each.
 */
export function kansasKpdcStatewideFilerPrefix(office: KansasCfrOffice): string | null {
  switch (office.code) {
    case KANSAS_CFR_OFFICE_CODES.governor:
      return "SW01";
    case KANSAS_CFR_OFFICE_CODES.attorneyGeneral:
      return "SW02";
    case KANSAS_CFR_OFFICE_CODES.insuranceCommissioner:
      return "SW03";
    case KANSAS_CFR_OFFICE_CODES.secretaryOfState:
      return "SW04";
    case KANSAS_CFR_OFFICE_CODES.stateTreasurer:
      return "SW05";
    default:
      return null;
  }
}

export type KansasKpdcCandidateLink = Omit<KansasKpdcIndexPdfLink, "rowText">;

export type KansasKpdcCandidateRow = {
  /** District cell of the House/Senate trees; null on the statewide tree. */
  district: number | null;
  /** Name cell as written ("Helwig, Dale"); "" when a district cell had no name cell. */
  filedName: string;
  links: KansasKpdcCandidateLink[];
};

/**
 * Candidate rows of a link tree, walked CELL by cell rather than by <tr>:
 * the hand-authored markup drops <tr> tags between some candidates (2026
 * House: "33 Smith, Romona ... 33 Woody, Eli" and "111 Wasinger / 112
 * Brantley / 112 Froetschner" share one row), so a row-based read merges
 * filers. A cell of 1-3 digits starts a filer (its district); a cell whose
 * text is a "Last, First" name (a comma, no digits) names the current filer
 * or, when it already has one (statewide trees have no district cells),
 * starts the next; PDF links attach to the current filer; every other cell
 * (headings, "N/A", "---", notes such as "Candidate is now in District
 * #31") is ignored. Links before any filer are counted in `orphanLinks`.
 * Verified over all eight live trees (House 2024/2026, Senate 2024/2028 and
 * the 2022/2026 specials, statewide 2022/2026): no orphans, every name in
 * Last, First shape, 267 House-2026 filers against 263 <tr> rows.
 */
export function parseKansasKpdcCandidateRows(
  html: string,
  baseUrl: string
): { rows: KansasKpdcCandidateRow[]; orphanLinks: number } {
  const collapse = (value: string) => decodeKansasHtmlText(value.replace(/<[^>]*>/g, " ")).replace(/\s+/g, " ").trim();
  const rows: KansasKpdcCandidateRow[] = [];
  let current: KansasKpdcCandidateRow | null = null;
  let orphanLinks = 0;
  for (const cell of html.split(/<td\b[^>]*>/i).slice(1)) {
    const anchors = [...cell.matchAll(/<a[^>]+href="([^"]+\.pdf)"[^>]*>([\s\S]*?)<\/a>/gi)];
    if (anchors.length > 0) {
      for (const anchor of anchors) {
        let url: string;
        try {
          url = buildKansasKpdcUrl(new URL(decodeKansasHtmlText(anchor[1]!), baseUrl).toString());
        } catch {
          continue; // off-site or non-https link: not an archive artifact
        }
        if (current === null) {
          orphanLinks += 1;
          continue;
        }
        current.links.push({ url, fileName: new URL(url).pathname.split("/").pop() ?? "", linkText: collapse(anchor[2]!) });
      }
      continue;
    }
    const text = collapse(cell);
    if (/^\d{1,3}$/.test(text)) {
      current = { district: Number.parseInt(text, 10), filedName: "", links: [] };
      rows.push(current);
    } else if (text.includes(",") && !/\d/.test(text) && text.length < 60) {
      if (current !== null && current.filedName === "") {
        current.filedName = text;
      } else {
        current = { district: null, filedName: text, links: [] };
        rows.push(current);
      }
    }
  }
  return { rows, orphanLinks };
}
