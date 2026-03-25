import { isIP } from "node:net";
import type { EvidenceSnippet } from "../../ai/types.js";
import type { StateResourceDraftPayload } from "../../types/stateResource.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";

type EvidenceCollectorOptions = {
  fetchImpl?: typeof fetch;
  fetchTimeoutMs?: number;
  maxSeedUrls?: number;
  maxDiscoveredUrls?: number;
  maxEvidenceSnippets?: number;
  snippetMaxChars?: number;
};

type FetchPageResult = {
  url: string;
  title: string;
  snippet: string;
  discoveredUrls: string[];
};

const DEFAULT_FETCH_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_SEED_URLS = 5;
const DEFAULT_MAX_DISCOVERED_URLS = 5;
const DEFAULT_MAX_EVIDENCE_SNIPPETS = 8;
const DEFAULT_SNIPPET_MAX_CHARS = 800;
const DEFAULT_MAX_RESPONSE_BYTES = 1_000_000; // 1 MB cap for buffered page text.

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
  const host = hostname.toLowerCase();

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
 * Returns true if URL is safe to fetch for evidence collection.
 */
function isSafeFetchUrl(rawUrl: string): boolean {
  try {
    const parsed = new URL(rawUrl);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return false;
    }
    return !isBlockedHostname(parsed.hostname);
  } catch {
    return false;
  }
}

/**
 * Returns true for text-like content types we allow reading into snippets.
 */
function isAllowedTextContentType(contentType: string): boolean {
  if (!contentType || contentType.trim().length === 0) {
    return true;
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
 * Extracts and normalizes href links from a page, bounded by maxCount.
 */
function extractDiscoveredUrls(html: string, baseUrl: string, maxCount: number): string[] {
  const links = new Set<string>();
  const hrefRegex = /href\s*=\s*["']([^"']+)["']/gi;
  let match: RegExpExecArray | null = hrefRegex.exec(html);

  while (match && links.size < maxCount) {
    const normalized = normalizeHttpUrl(match[1], { baseUrl });
    if (normalized) {
      links.add(normalized);
    }
    match = hrefRegex.exec(html);
  }

  return Array.from(links);
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

  const lowered = text.toLowerCase();
  const targetA = stateName.toLowerCase();
  const targetB = escapeRegexLiteral(stateAbbreviation.toLowerCase());
  const idx = lowered.indexOf(targetA);
  const abbrevRegex = new RegExp(`\\b${targetB}\\b`);
  const altIdx = abbrevRegex.exec(lowered)?.index ?? -1;
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
  allowedSeedHosts: Set<string>
): Promise<FetchPageResult | null> {
  if (!isSafeFetchUrl(url)) {
    return null;
  }

  const urlHost = new URL(url).hostname.toLowerCase();
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

    const contentType = (response.headers.get("content-type") ?? "").toLowerCase();
    if (!isAllowedTextContentType(contentType)) {
      return null;
    }

    const raw = await readTextWithByteCap(response, DEFAULT_MAX_RESPONSE_BYTES);
    if (!raw) {
      return null;
    }

    const title = contentType.includes("html")
      ? extractTitle(raw, url)
      : hostAsSourceName(url);

    const text = contentType.includes("html")
      ? htmlToText(raw)
      : normalizeWhitespace(raw);

    const snippet = buildSnippet(text, draft.state_name, draft.state_abbreviation, snippetMaxChars);
    if (!snippet) {
      return null;
    }

    const discoveredUrls = contentType.includes("html")
      ? extractDiscoveredUrls(raw, url, maxDiscoveredUrls)
      : [];

    return {
      url,
      title,
      snippet,
      discoveredUrls,
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
  const fetchTimeoutMs = options.fetchTimeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS;
  const maxSeedUrls = options.maxSeedUrls ?? DEFAULT_MAX_SEED_URLS;
  const maxDiscoveredUrls = options.maxDiscoveredUrls ?? DEFAULT_MAX_DISCOVERED_URLS;
  const maxEvidenceSnippets = options.maxEvidenceSnippets ?? DEFAULT_MAX_EVIDENCE_SNIPPETS;
  const snippetMaxChars = options.snippetMaxChars ?? DEFAULT_SNIPPET_MAX_CHARS;

  const seedUrls = Array.from(
    new Set(
      draft.seed_sources
        .map((url) => normalizeHttpUrl(url))
        .filter((url): url is string => typeof url === "string" && isSafeFetchUrl(url))
    )
  ).slice(0, maxSeedUrls);

  const allowedSeedHosts = new Set(
    seedUrls.map((url) => {
      try {
        return new URL(url).hostname.toLowerCase();
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
      allowedSeedHosts
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

      if (draft.allow_open_web_research) {
        for (const discovered of page.discoveredUrls) {
          if (!isSafeFetchUrl(discovered)) {
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
        allowedSeedHosts
      );

      if (page) {
        evidence.push({
          url: page.url,
          title: page.title,
          snippet: page.snippet,
        });
      }
    }
  }

  // Last-resort fallback for extreme failure cases.
  if (evidence.length === 0 && seedUrls.length > 0) {
    return [fallbackEvidence(seedUrls[0], draft)];
  }

  return evidence.slice(0, maxEvidenceSnippets);
}
