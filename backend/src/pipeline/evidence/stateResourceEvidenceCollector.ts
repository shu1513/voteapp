import { lookup as dnsLookup } from "node:dns/promises";
import { isIP } from "node:net";
import type { EvidenceSnippet } from "../../ai/types.js";
import type { StateResourceDraftPayload } from "../../types/stateResource.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";

type EvidenceCollectorOptions = {
  fetchImpl?: typeof fetch;
  dnsLookupImpl?: DnsLookupFn;
  enforceDnsResolution?: boolean;
  fetchTimeoutMs?: number;
  maxSeedUrls?: number;
  maxDiscoveredUrls?: number;
  maxEvidenceSnippets?: number;
  snippetMaxChars?: number;
};

type DnsLookupFn = (hostname: string) => Promise<string[]>;

type FetchPageResult = {
  url: string;
  title: string;
  snippet: string;
  discoveredUrls: string[];
  stateSpecificPollingUrl?: string;
};

type UrlSafetyOptions = {
  dnsLookupImpl: DnsLookupFn;
  enforceDnsResolution: boolean;
  hostSafetyCache: Map<string, boolean>;
};

const DEFAULT_FETCH_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_SEED_URLS = 5;
const DEFAULT_MAX_DISCOVERED_URLS = 5;
const DEFAULT_MAX_EVIDENCE_SNIPPETS = 8;
const DEFAULT_SNIPPET_MAX_CHARS = 800;
const DEFAULT_MAX_RESPONSE_BYTES = 1_000_000; // 1 MB cap for buffered page text.

/**
 * DNS lookup adapter used for hostname-to-IP safety checks.
 */
async function defaultDnsLookupImpl(hostname: string): Promise<string[]> {
  const records = await dnsLookup(hostname, { all: true, verbatim: true });
  return records.map((record) => record.address);
}

/**
 * Removes invalid UTF-16 surrogate usage so downstream JSON storage is safe.
 */
function stripInvalidUnicode(input: string): string {
  let output = "";

  for (let i = 0; i < input.length; i += 1) {
    const code = input.charCodeAt(i);

    // Preserve valid surrogate pairs and drop lone surrogates.
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = input.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        output += input[i];
        output += input[i + 1];
        i += 1;
      }
      continue;
    }

    if (code >= 0xdc00 && code <= 0xdfff) {
      continue;
    }

    output += input[i];
  }

  return output;
}

/**
 * Sanitizes text for compact snippet storage.
 */
function normalizeWhitespace(input: string): string {
  // PostgreSQL jsonb rejects some control chars (notably null); strip before storing.
  const sanitized = stripInvalidUnicode(input).replace(/[\u0000-\u001f\u007f]/g, " ");
  return sanitized.replace(/\s+/g, " ").trim();
}

/**
 * Produces a stable source name from URL host for fallback citations.
 */
function hostAsSourceName(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "source";
  }
}

/**
 * Returns true when a hostname is not eligible for external crawling.
 */
function isBlockedHostname(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");

  if (
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host.endsWith(".local") ||
    host.endsWith(".internal")
  ) {
    return true;
  }

  if (host === "metadata.google.internal" || host === "metadata") {
    return true;
  }

  const ipVersion = isIP(host);
  if (ipVersion === 4) {
    const octets = host.split(".").map((part) => Number.parseInt(part, 10));
    if (octets.length !== 4 || octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
      return true;
    }

    const [a, b] = octets;
    if (
      a === 10 ||
      a === 127 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      a === 0 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 198 && (b === 18 || b === 19)) ||
      a >= 224
    ) {
      return true;
    }

    return false;
  }

  if (ipVersion === 6) {
    const normalized = host.replace(/^\[|\]$/g, "").toLowerCase();
    if (normalized === "::1" || normalized === "::") {
      return true;
    }
    if (normalized.startsWith("fc") || normalized.startsWith("fd")) {
      return true;
    }
    if (
      normalized.startsWith("fe8") ||
      normalized.startsWith("fe9") ||
      normalized.startsWith("fea") ||
      normalized.startsWith("feb")
    ) {
      return true;
    }

    return false;
  }

  return false;
}

/**
 * Returns true if URL target is safe to fetch for evidence collection.
 */
async function isSafeFetchUrl(rawUrl: string, safetyOptions: UrlSafetyOptions): Promise<boolean> {
  try {
    const parsed = new URL(rawUrl);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return false;
    }

    const host = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, "");
    if (!host || isBlockedHostname(host)) {
      return false;
    }

    if (!safetyOptions.enforceDnsResolution) {
      return true;
    }

    if (isIP(host) !== 0) {
      return true;
    }

    const cached = safetyOptions.hostSafetyCache.get(host);
    if (typeof cached === "boolean") {
      return cached;
    }

    let addresses: string[];
    try {
      addresses = await safetyOptions.dnsLookupImpl(host);
    } catch {
      safetyOptions.hostSafetyCache.set(host, false);
      return false;
    }

    if (!Array.isArray(addresses) || addresses.length === 0) {
      safetyOptions.hostSafetyCache.set(host, false);
      return false;
    }

    const allAddressesSafe = addresses.every((address) => {
      const normalized = address.toLowerCase().replace(/^\[|\]$/g, "");
      return !isBlockedHostname(normalized);
    });

    safetyOptions.hostSafetyCache.set(host, allAddressesSafe);
    return allAddressesSafe;
  } catch {
    return false;
  }
}

/**
 * Returns true for text-like content types we allow reading into snippets.
 */
function isAllowedTextContentType(contentType: string, sourceUrl: string): boolean {
  if (!contentType || contentType.trim().length === 0) {
    console.warn(`state_resources evidence skipped due to missing content-type: ${sourceUrl}`);
    return false;
  }

  const lower = contentType.toLowerCase();
  return (
    lower.startsWith("text/") ||
    lower.includes("application/json") ||
    lower.includes("application/xml") ||
    lower.includes("application/xhtml+xml") ||
    lower.includes("application/ld+json")
  );
}

/**
 * Reads response body as text with a hard byte cap.
 */
async function readTextWithByteCap(response: Response, maxBytes: number): Promise<string | null> {
  const contentLength = Number.parseInt(response.headers.get("content-length") ?? "", 10);
  if (Number.isFinite(contentLength) && contentLength > maxBytes) {
    return null;
  }

  if (!response.body) {
    const text = await response.text();
    return Buffer.byteLength(text, "utf8") > maxBytes ? null : text;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }

      if (!value) {
        continue;
      }

      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        try {
          await reader.cancel();
        } catch {
          // Ignore reader cancellation errors; caller treats as no evidence.
        }
        return null;
      }

      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const merged = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return new TextDecoder().decode(merged);
}

/**
 * Extracts page title when present; otherwise falls back to source host.
 */
function extractTitle(html: string, url: string): string {
  const match = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  if (match && match[1]) {
    const title = normalizeWhitespace(match[1]);
    if (title.length > 0) {
      return title;
    }
  }

  return hostAsSourceName(url);
}

/**
 * Converts HTML to plain text for snippet generation.
 */
function htmlToText(html: string): string {
  const withoutScripts = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ");
  const withoutTags = withoutScripts.replace(/<[^>]+>/g, " ");
  const decoded = withoutTags
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
  return normalizeWhitespace(decoded);
}

/**
 * Extracts the state-specific polling link from Vote.org's polling-place-locator page when present.
 */
function extractVoteOrgStatePollingUrl(html: string, baseUrl: string, stateName: string): string | null {
  let parsedBase: URL;
  try {
    parsedBase = new URL(baseUrl);
  } catch {
    return null;
  }

  const host = parsedBase.hostname.toLowerCase();
  const path = parsedBase.pathname.toLowerCase();
  if (!host.endsWith("vote.org") || !path.includes("/polling-place-locator")) {
    return null;
  }

  const escapedStateName = escapeRegexLiteral(stateName.trim());
  const anchorPattern = new RegExp(
    `<a\\b[^>]*href\\s*=\\s*["']([^"']+)["'][^>]*>\\s*${escapedStateName}(?:\\s*<!--[\\s\\S]*?-->\\s*)?\\s*polling\\s*place\\s*locator\\s*<\\/a>`,
    "i"
  );
  const anchorMatch = anchorPattern.exec(html);
  if (anchorMatch?.[1]) {
    return normalizeHttpUrl(anchorMatch[1], { baseUrl });
  }

  const slug = stateName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const escapedSlug = escapeRegexLiteral(slug);
  const rowPattern = new RegExp(
    `<p\\b[^>]*id\\s*=\\s*["']${escapedSlug}["'][^>]*>[\\s\\S]*?<\\/p>[\\s\\S]{0,1400}?<a\\b[^>]*href\\s*=\\s*["']([^"']+)["']`,
    "i"
  );
  const rowMatch = rowPattern.exec(html);
  if (rowMatch?.[1]) {
    return normalizeHttpUrl(rowMatch[1], { baseUrl });
  }

  return null;
}

function isVoteOrgPollingLocatorUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.hostname.toLowerCase().endsWith("vote.org") && parsed.pathname.toLowerCase().includes("/polling-place-locator");
  } catch {
    return false;
  }
}

/**
 * Extracts and normalizes href links from a page, bounded by maxCount.
 */
function extractDiscoveredUrls(
  html: string,
  baseUrl: string,
  maxCount: number,
  stateName: string,
  stateAbbreviation: string
): string[] {
  const stateNameLower = stateName.trim().toLowerCase();
  const stateSlug = stateNameLower.replace(/\s+/g, "-");
  const stateAbbreviationLower = stateAbbreviation.trim().toLowerCase();
  const linkScores = new Map<string, number>();
  const linkOrder = new Map<string, number>();
  let order = 0;

  const voteOrgStateUrl = extractVoteOrgStatePollingUrl(html, baseUrl, stateName);
  if (voteOrgStateUrl) {
    linkScores.set(voteOrgStateUrl, 10_000);
    linkOrder.set(voteOrgStateUrl, -1);
  }

  const anchorRegex = /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let match: RegExpExecArray | null = anchorRegex.exec(html);

  while (match) {
    const normalized = normalizeHttpUrl(match[1], { baseUrl });
    if (!normalized) {
      match = anchorRegex.exec(html);
      continue;
    }

    const anchorText = normalizeWhitespace(match[2].replace(/<[^>]+>/g, " ")).toLowerCase();
    let score = 0;

    if (anchorText.includes(`${stateNameLower} polling place locator`)) {
      score += 100;
    }
    if (normalized.toLowerCase().includes(`.${stateAbbreviationLower}.`)) {
      score += 40;
    }
    if (normalized.toLowerCase().includes(`/${stateSlug}`)) {
      score += 25;
    }
    if (anchorText.includes("polling place locator")) {
      score += 35;
    }
    if (anchorText.includes(stateNameLower) && anchorText.includes("polling")) {
      score += 30;
    }
    if (/register|absentee|mail|id\b|identification/.test(anchorText)) {
      score -= 30;
    }
    if (/\.gov\b|\/elections?\b|sos\./i.test(normalized)) {
      score += 20;
    }

    const previousScore = linkScores.get(normalized) ?? Number.NEGATIVE_INFINITY;
    if (score > previousScore) {
      linkScores.set(normalized, score);
    }

    if (!linkOrder.has(normalized)) {
      linkOrder.set(normalized, order);
      order += 1;
    }

    match = anchorRegex.exec(html);
  }

  const urlLiteralRegex = /https?:\/\/[^\s"'<>\\]+|https?:\\\/\\\/[^\s"'<>]+/gi;
  let urlMatch: RegExpExecArray | null = urlLiteralRegex.exec(html);

  while (urlMatch) {
    const rawUrl = urlMatch[0].replace(/\\\//g, "/");
    const normalized = normalizeHttpUrl(rawUrl, { baseUrl });
    if (!normalized) {
      urlMatch = urlLiteralRegex.exec(html);
      continue;
    }

    const lower = normalized.toLowerCase();
    let score = 0;

    if (/polling-place|find-your-polling-place|pollfinder|voterlookup|locator/.test(lower)) {
      score += 60;
    }
    if (lower.includes(`.${stateAbbreviationLower}.`)) {
      score += 40;
    }
    if (lower.includes(`/${stateSlug}`)) {
      score += 25;
    }
    if (/sos\.|elections\./.test(lower)) {
      score += 20;
    }
    if (/register|registration|absentee|mail|id-laws|identification/.test(lower)) {
      score -= 35;
    }

    const previousScore = linkScores.get(normalized) ?? Number.NEGATIVE_INFINITY;
    if (score > previousScore) {
      linkScores.set(normalized, score);
    }
    if (!linkOrder.has(normalized)) {
      linkOrder.set(normalized, order);
      order += 1;
    }

    urlMatch = urlLiteralRegex.exec(html);
  }

  const hostPathRegex =
    /\b([a-z0-9.-]+\.[a-z]{2,}(?:\/[a-z0-9/_-]*(?:poll|locator|voterlookup|pollfinder)[a-z0-9/_-]*)+\/?)\b/gi;
  let hostPathMatch: RegExpExecArray | null = hostPathRegex.exec(html);

  while (hostPathMatch) {
    const raw = hostPathMatch[1];
    const withScheme = raw.startsWith("http://") || raw.startsWith("https://") ? raw : `https://${raw}`;
    const normalized = normalizeHttpUrl(withScheme, { baseUrl });
    if (!normalized) {
      hostPathMatch = hostPathRegex.exec(html);
      continue;
    }

    const lower = normalized.toLowerCase();
    let score = 0;

    if (/polling-place|poll|locator|voterlookup|pollfinder/.test(lower)) {
      score += 60;
    }
    if (lower.includes(`.${stateAbbreviationLower}.`)) {
      score += 40;
    }
    if (lower.includes(`/${stateSlug}`)) {
      score += 25;
    }
    if (/sos\.|elections\./.test(lower)) {
      score += 20;
    }
    if (/register|registration|absentee|mail|id-laws|identification/.test(lower)) {
      score -= 35;
    }

    const previousScore = linkScores.get(normalized) ?? Number.NEGATIVE_INFINITY;
    if (score > previousScore) {
      linkScores.set(normalized, score);
    }
    if (!linkOrder.has(normalized)) {
      linkOrder.set(normalized, order);
      order += 1;
    }

    hostPathMatch = hostPathRegex.exec(html);
  }

  const ranked = Array.from(linkScores.entries())
    .filter(([, score]) => score > 0)
    .sort((a, b) => {
      if (b[1] !== a[1]) {
        return b[1] - a[1];
      }

      return (linkOrder.get(a[0]) ?? 0) - (linkOrder.get(b[0]) ?? 0);
    })
    .slice(0, maxCount)
    .map(([url]) => url);

  return ranked;
}

/**
 * Escapes regex meta characters for safe literal matching.
 */
function escapeRegexLiteral(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Builds a bounded snippet, prioritizing text near state name/abbreviation.
 */
function buildSnippet(text: string, stateName: string, stateAbbreviation: string, maxChars: number): string {
  if (!text) {
    return "";
  }

  // Defensive guard: keep regex input bounded even if upstream validation changes.
  const safeAbbreviation = stateAbbreviation.slice(0, 10);

  const lowered = text.toLowerCase();
  const targetA = stateName.toLowerCase();
  const targetB = escapeRegexLiteral(safeAbbreviation.toLowerCase());
  const idx = lowered.indexOf(targetA);
  const altIdx = targetB.length > 0 ? new RegExp(`\\b${targetB}\\b`).exec(lowered)?.index ?? -1 : -1;
  const anchor = idx >= 0 ? idx : altIdx;

  if (anchor >= 0) {
    const start = Math.max(0, anchor - Math.floor(maxChars / 3));
    const end = Math.min(text.length, start + maxChars);
    return text.slice(start, end).trim();
  }

  return text.slice(0, maxChars).trim();
}

/**
 * Fetches one page, extracts a snippet, and returns newly discovered links.
 */
async function fetchPageEvidence(
  url: string,
  draft: StateResourceDraftPayload,
  fetchImpl: typeof fetch,
  fetchTimeoutMs: number,
  snippetMaxChars: number,
  maxDiscoveredUrls: number,
  allowOpenWebResearch: boolean,
  allowedSeedHosts: Set<string>,
  safetyOptions: UrlSafetyOptions
): Promise<FetchPageResult | null> {
  if (!(await isSafeFetchUrl(url, safetyOptions))) {
    return null;
  }

  const urlHost = new URL(url).hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (!allowOpenWebResearch && !allowedSeedHosts.has(urlHost)) {
    return null;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), fetchTimeoutMs);

  try {
    const response = await fetchImpl(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        "User-Agent": "voteapp-state-resources-evidence-bot/1.0",
      },
    });

    if (!response.ok) {
      return null;
    }

    const responseSourceUrl =
      normalizeHttpUrl(response.url) ??
      normalizeHttpUrl(url);
    if (!responseSourceUrl) {
      return null;
    }

    if (!(await isSafeFetchUrl(responseSourceUrl, safetyOptions))) {
      return null;
    }

    const finalHost = new URL(responseSourceUrl).hostname.toLowerCase().replace(/^\[|\]$/g, "");
    if (!allowOpenWebResearch && !allowedSeedHosts.has(finalHost)) {
      return null;
    }

    const contentType = (response.headers.get("content-type") ?? "").toLowerCase();
    if (!isAllowedTextContentType(contentType, responseSourceUrl)) {
      return null;
    }

    const raw = await readTextWithByteCap(response, DEFAULT_MAX_RESPONSE_BYTES);
    if (!raw) {
      return null;
    }

    const title = contentType.includes("html")
      ? extractTitle(raw, responseSourceUrl)
      : hostAsSourceName(responseSourceUrl);

    const text = contentType.includes("html")
      ? htmlToText(raw)
      : normalizeWhitespace(raw);

    const snippet = buildSnippet(text, draft.state_name, draft.state_abbreviation, snippetMaxChars);
    if (!snippet) {
      return null;
    }

    const discoveredUrls = contentType.includes("html")
      ? extractDiscoveredUrls(
          raw,
          responseSourceUrl,
          maxDiscoveredUrls,
          draft.state_name,
          draft.state_abbreviation
        )
      : [];
    const stateSpecificPollingUrl = contentType.includes("html")
      ? extractVoteOrgStatePollingUrl(raw, responseSourceUrl, draft.state_name) ?? undefined
      : undefined;

    return {
      url: responseSourceUrl,
      title,
      snippet,
      discoveredUrls,
      stateSpecificPollingUrl,
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Produces fallback evidence when live fetch is unavailable.
 */
function fallbackEvidence(url: string, draft: StateResourceDraftPayload): EvidenceSnippet {
  return {
    url,
    title: hostAsSourceName(url),
    snippet: `Seed source captured for ${draft.state_name} voting information. Live page fetch was unavailable during collection.`,
  };
}

/**
 * Collects evidence snippets starting from seed URLs and discovered links.
 * The collection algorithm is deterministic given consistent network responses.
 * Safe to run before AI enrichment.
 */
export async function collectStateResourceEvidence(
  draft: StateResourceDraftPayload,
  options: EvidenceCollectorOptions = {}
): Promise<EvidenceSnippet[]> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const dnsLookupImpl = options.dnsLookupImpl ?? defaultDnsLookupImpl;
  const enforceDnsResolution = options.enforceDnsResolution ?? options.fetchImpl === undefined;
  const safetyOptions: UrlSafetyOptions = {
    dnsLookupImpl,
    enforceDnsResolution,
    hostSafetyCache: new Map<string, boolean>(),
  };
  const fetchTimeoutMs = options.fetchTimeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS;
  const maxSeedUrls = options.maxSeedUrls ?? DEFAULT_MAX_SEED_URLS;
  const maxDiscoveredUrls = options.maxDiscoveredUrls ?? DEFAULT_MAX_DISCOVERED_URLS;
  const maxEvidenceSnippets = options.maxEvidenceSnippets ?? DEFAULT_MAX_EVIDENCE_SNIPPETS;
  const snippetMaxChars = options.snippetMaxChars ?? DEFAULT_SNIPPET_MAX_CHARS;

  const normalizedSeedCandidates = Array.from(
    new Set(
      draft.seed_sources
        .map((url) => normalizeHttpUrl(url))
        .filter((url): url is string => typeof url === "string")
    )
  );
  const seedUrls: string[] = [];
  for (const candidate of normalizedSeedCandidates) {
    if (seedUrls.length >= maxSeedUrls) {
      break;
    }

    if (await isSafeFetchUrl(candidate, safetyOptions)) {
      seedUrls.push(candidate);
    }
  }

  const allowedSeedHosts = new Set(
    seedUrls.map((url) => {
      try {
        return new URL(url).hostname.toLowerCase().replace(/^\[|\]$/g, "");
      } catch {
        return "";
      }
    }).filter((host) => host.length > 0)
  );

  const evidence: EvidenceSnippet[] = [];
  const seenUrls = new Set<string>();
  const discoveredQueue: string[] = [];

  for (const seedUrl of seedUrls) {
    const page = await fetchPageEvidence(
      seedUrl,
      draft,
      fetchImpl,
      fetchTimeoutMs,
      snippetMaxChars,
      maxDiscoveredUrls,
      draft.allow_open_web_research,
      allowedSeedHosts,
      safetyOptions
    );

    if (page) {
      if (!seenUrls.has(page.url)) {
        seenUrls.add(page.url);
        evidence.push({
          url: page.url,
          title: page.title,
          snippet: page.snippet,
        });
      }

      if (page.stateSpecificPollingUrl && !seenUrls.has(page.stateSpecificPollingUrl)) {
        seenUrls.add(page.stateSpecificPollingUrl);
        evidence.push({
          url: page.stateSpecificPollingUrl,
          title: hostAsSourceName(page.stateSpecificPollingUrl),
          snippet: `State-specific polling place locator link extracted for ${draft.state_name}.`,
        });
      }

      if (draft.allow_open_web_research) {
        const trustedVoteOrgStateDiscoveredUrl =
          isVoteOrgPollingLocatorUrl(page.url) && page.discoveredUrls.length > 0 ? page.discoveredUrls[0] : null;

        for (const discovered of page.discoveredUrls) {
          const isTrustedVoteOrgStateLink = discovered === trustedVoteOrgStateDiscoveredUrl;

          if (!isTrustedVoteOrgStateLink && !(await isSafeFetchUrl(discovered, safetyOptions))) {
            continue;
          }
          if (!seenUrls.has(discovered) && discoveredQueue.length < maxDiscoveredUrls) {
            discoveredQueue.push(discovered);
            seenUrls.add(discovered);
          }
        }
      }
    } else if (!seenUrls.has(seedUrl)) {
      seenUrls.add(seedUrl);
      evidence.push(fallbackEvidence(seedUrl, draft));
    }

    if (evidence.length >= maxEvidenceSnippets) {
      return evidence.slice(0, maxEvidenceSnippets);
    }
  }

  if (draft.allow_open_web_research) {
    for (const discoveredUrl of discoveredQueue) {
      if (evidence.length >= maxEvidenceSnippets) {
        break;
      }

      const page = await fetchPageEvidence(
        discoveredUrl,
        draft,
        fetchImpl,
        fetchTimeoutMs,
        snippetMaxChars,
        maxDiscoveredUrls,
        draft.allow_open_web_research,
        allowedSeedHosts,
        safetyOptions
      );

      if (page) {
        const exists = evidence.some((item) => item.url === page.url);
        if (!exists) {
          evidence.push({
            url: page.url,
            title: page.title,
            snippet: page.snippet,
          });
        }
      } else {
        const exists = evidence.some((item) => item.url === discoveredUrl);
        if (!exists) {
          evidence.push(fallbackEvidence(discoveredUrl, draft));
        }
      }
    }
  }

  // Last-resort fallback for extreme failure cases.
  if (evidence.length === 0 && seedUrls.length > 0) {
    return [fallbackEvidence(seedUrls[0], draft)];
  }

  return evidence.slice(0, maxEvidenceSnippets);
}
