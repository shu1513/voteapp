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

import { KansasCfrClientError } from "./kansasCfrViewerClient.js";
import { decodeKansasHtmlText } from "./kansasCfrViewerParsers.js";

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
  /** Normalized YYYYMM when the suffix carries one (2607 -> 202607). */
  periodKey: string | null;
  /** amend-prefixed suffixes are full-replacement filings. */
  amendment: boolean;
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
  const amendment = /^amend/i.test(suffix);
  if (amendment) suffix = suffix.slice("amend".length);

  const info = (kind: KansasKpdcFilingKind, periodKey: string | null): KansasKpdcFileNameInfo => ({
    fileName,
    filerCode,
    suffix: (amendment ? "amend" : "") + suffix,
    kind,
    periodKey,
    amendment,
  });

  if (suffix === "AT") return info("appointment_of_treasurer", null);
  if (/^\d{6}$/.test(suffix)) return info("report", suffix);
  if (/^\d{4}$/.test(suffix)) return info("report", `20${suffix}`);
  if (/^\d{4}PLF$/.test(suffix)) return info("last_minute", null);
  const affidavit = /^Aff(\d{4})$/.exec(suffix);
  if (affidavit) return info("affidavit", `20${affidavit[1]}`);
  const termination = /^Term(\d{4})$/.exec(suffix);
  if (termination) return info("termination", `20${termination[1]}`);
  return info("unknown", null);
}
