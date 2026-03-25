import type { EvidenceSnippet } from "../../ai/types.js";
import type { StateResourceDraftPayload } from "../../types/stateResource.js";

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

function normalizeWhitespace(input: string): string {
  // PostgreSQL jsonb rejects some control chars (notably null); strip before storing.
  const sanitized = stripInvalidUnicode(input).replace(/[\u0000-\u001f\u007f]/g, " ");
  return sanitized.replace(/\s+/g, " ").trim();
}

function normalizeUrl(raw: string, base?: string): string | null {
  try {
    const parsed = base ? new URL(raw, base) : new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }
    parsed.hash = "";
    // Normalize trailing slash for stable URL comparisons.
    const normalized = parsed.toString();
    return normalized.endsWith("/") ? normalized.slice(0, -1) : normalized;
  } catch {
    return null;
  }
}

function hostAsSourceName(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "source";
  }
}

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

function extractDiscoveredUrls(html: string, baseUrl: string, maxCount: number): string[] {
  const links = new Set<string>();
  const hrefRegex = /href\s*=\s*["']([^"']+)["']/gi;
  let match: RegExpExecArray | null = hrefRegex.exec(html);

  while (match && links.size < maxCount) {
    const normalized = normalizeUrl(match[1], baseUrl);
    if (normalized) {
      links.add(normalized);
    }
    match = hrefRegex.exec(html);
  }

  return Array.from(links);
}

function buildSnippet(text: string, stateName: string, stateAbbreviation: string, maxChars: number): string {
  if (!text) {
    return "";
  }

  const lowered = text.toLowerCase();
  const targetA = stateName.toLowerCase();
  const targetB = stateAbbreviation.toLowerCase();
  const idx = lowered.indexOf(targetA);
  const altIdx = lowered.indexOf(` ${targetB} `);
  const anchor = idx >= 0 ? idx : altIdx;

  if (anchor >= 0) {
    const start = Math.max(0, anchor - Math.floor(maxChars / 3));
    const end = Math.min(text.length, start + maxChars);
    return text.slice(start, end).trim();
  }

  return text.slice(0, maxChars).trim();
}

async function fetchPageEvidence(
  url: string,
  draft: StateResourceDraftPayload,
  fetchImpl: typeof fetch,
  fetchTimeoutMs: number,
  snippetMaxChars: number,
  maxDiscoveredUrls: number
): Promise<FetchPageResult | null> {
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
    const raw = await response.text();

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

function fallbackEvidence(url: string, draft: StateResourceDraftPayload): EvidenceSnippet {
  return {
    url,
    title: hostAsSourceName(url),
    snippet: `Seed source captured for ${draft.state_name} voting information. Live page fetch was unavailable during collection.`,
  };
}

/**
 * Collects evidence snippets starting from seed URLs and discovered links.
 * This collector is deterministic and safe to run before AI enrichment.
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
        .map((url) => normalizeUrl(url))
        .filter((url): url is string => typeof url === "string")
    )
  ).slice(0, maxSeedUrls);

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
      maxDiscoveredUrls
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

      for (const discovered of page.discoveredUrls) {
        if (!seenUrls.has(discovered) && discoveredQueue.length < maxDiscoveredUrls) {
          discoveredQueue.push(discovered);
          seenUrls.add(discovered);
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
      maxDiscoveredUrls
    );

    if (page) {
      evidence.push({
        url: page.url,
        title: page.title,
        snippet: page.snippet,
      });
    }
  }

  // Last-resort fallback for extreme failure cases.
  if (evidence.length === 0 && seedUrls.length > 0) {
    return [fallbackEvidence(seedUrls[0], draft)];
  }

  return evidence.slice(0, maxEvidenceSnippets);
}
