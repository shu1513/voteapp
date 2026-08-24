import type { Pool, PoolClient } from "pg";

type Queryable = Pick<Pool | PoolClient, "query">;

export const DEFAULT_SITE_SITEMAP_CACHE_TTL_MS = 60 * 60 * 1000;

export const SITEMAP_STATIC_PATHS = ["/", "/mission", "/disclaimer", "/terms", "/privacy"] as const;

export type SiteSitemapUrl = {
  path: string;
  lastmod?: string | Date | null;
};

type SitemapRow = {
  path: string;
  lastmod: string | Date | null;
};

export type CachedSiteSitemapOptions = {
  db: Queryable;
  siteOrigin: string;
  ttlMs?: number;
  now?: () => number;
};

export function normalizeSiteOrigin(rawOrigin: string): string {
  const trimmed = rawOrigin.trim();
  if (!trimmed) {
    throw new Error("SITE_ORIGIN must not be empty");
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error(`SITE_ORIGIN must be an absolute http(s) URL: ${rawOrigin}`);
  }

  if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
    throw new Error(`SITE_ORIGIN must use http or https: ${rawOrigin}`);
  }
  if (parsed.pathname !== "/" || parsed.search || parsed.hash) {
    throw new Error(`SITE_ORIGIN must not include a path, query, or hash: ${rawOrigin}`);
  }

  return parsed.origin;
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function formatLastmod(value: string | Date | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

function absoluteUrl(siteOrigin: string, path: string): string {
  return new URL(path, `${siteOrigin}/`).toString();
}

export function buildSiteSitemapXml(input: { siteOrigin: string; urls: readonly SiteSitemapUrl[] }): string {
  const siteOrigin = normalizeSiteOrigin(input.siteOrigin);
  const entries = input.urls
    .map((url) => {
      const loc = escapeXml(absoluteUrl(siteOrigin, url.path));
      const lastmod = formatLastmod(url.lastmod);
      return lastmod
        ? `  <url><loc>${loc}</loc><lastmod>${escapeXml(lastmod)}</lastmod></url>`
        : `  <url><loc>${loc}</loc></url>`;
    })
    .join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${entries}\n</urlset>\n`;
}

export async function listSiteSitemapUrls(db: Queryable): Promise<SiteSitemapUrl[]> {
  const [elections, candidates] = await Promise.all([
    db.query<SitemapRow>(
      `
        SELECT
          ('/elections/' || id::text) AS path,
          updated_at AS lastmod
        FROM public.elections
        ORDER BY updated_at DESC, id ASC
      `
    ),
    db.query<SitemapRow>(
      `
        SELECT
          ('/candidates/' || id::text) AS path,
          updated_at AS lastmod
        FROM public.candidates
        WHERE deleted_at IS NULL
          AND merged_into_candidate_id IS NULL
        ORDER BY updated_at DESC, id ASC
      `
    ),
  ]);

  return [
    ...SITEMAP_STATIC_PATHS.map((path) => ({ path })),
    ...elections.rows.map((row) => ({ path: row.path, lastmod: row.lastmod })),
    ...candidates.rows.map((row) => ({ path: row.path, lastmod: row.lastmod })),
  ];
}

export function createCachedSiteSitemap(options: CachedSiteSitemapOptions): () => Promise<string> {
  const ttlMs = options.ttlMs ?? DEFAULT_SITE_SITEMAP_CACHE_TTL_MS;
  const siteOrigin = normalizeSiteOrigin(options.siteOrigin);
  const now = options.now ?? (() => Date.now());
  let cachedXml: string | null = null;
  let cachedUntil = 0;
  let inFlight: Promise<string> | null = null;

  return async () => {
    const currentTime = now();
    if (cachedXml && currentTime < cachedUntil) {
      return cachedXml;
    }
    if (inFlight) {
      return inFlight;
    }

    inFlight = (async () => {
      const urls = await listSiteSitemapUrls(options.db);
      const xml = buildSiteSitemapXml({ siteOrigin, urls });
      cachedXml = xml;
      cachedUntil = now() + ttlMs;
      return xml;
    })();

    try {
      return await inFlight;
    } catch (error) {
      if (cachedXml) {
        return cachedXml;
      }
      throw error;
    } finally {
      inFlight = null;
    }
  };
}
