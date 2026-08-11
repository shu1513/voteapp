import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { isEfileCalWorkbookData } from "./efileCalWorkbookParser.js";

/**
 * Download client for efile.systems campaign bulk exports. Agency-configurable
 * so San José and San Diego (same vendor, verified identical API + workbook
 * layout 2026-08-10) share one implementation.
 *
 * Flow (verified live against `efile.sanjoseca.gov`):
 * 1. `GET {portal}/api/v1/public/campaign-bulk-export-url?year=Y&most_recent_only=B`
 *    → `{"success":true,"data":"https://efs-efile-campaign-exports.s3.amazonaws.com/..."}`
 * 2. `HEAD`/`GET` the returned S3 URL (public object; serves ETag,
 *    Last-Modified, and Content-Length, so the artifact cache can skip
 *    unchanged downloads).
 */

export type EfileCalAgencyConfig = {
  /** Short slug used in cache file names, e.g. "csj". */
  agencyKey: string;
  /** https portal origin, e.g. "https://efile.sanjoseca.gov". */
  portalBaseUrl: string;
  /** Exact hostnames the export URL may point at — anything else is refused. */
  allowedExportHosts: readonly string[];
};

export const EFILE_CAL_FETCH_TIMEOUT_MS = 30_000;
export const EFILE_CAL_DOWNLOAD_TIMEOUT_MS = 120_000;
/** 2026 San José file is ~2.6 MB; cap leaves lots of headroom without letting a bad URL exhaust memory. */
export const EFILE_CAL_MAX_WORKBOOK_BYTES = 64 * 1024 * 1024;

export type EfileCalRemoteWorkbookMetadata = {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type EfileCalWorkbookArtifactCacheMetadata = {
  version: 1;
  agencyKey: string;
  year: number;
  mostRecentOnly: boolean;
  workbookPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: EfileCalRemoteWorkbookMetadata;
  bytesWritten: number;
};

export type EfileCalWorkbookArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  workbookPath: string;
  metadataPath: string;
  remote: EfileCalRemoteWorkbookMetadata;
  previous: EfileCalWorkbookArtifactCacheMetadata | null;
  current: EfileCalWorkbookArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

function parseHttpsUrl(value: string, fieldName: string): URL {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Invalid ${fieldName}: ${value}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Invalid ${fieldName} protocol: ${parsed.protocol}. Only https is allowed.`);
  }
  return parsed;
}

function assertAllowedExportUrl(url: string, config: EfileCalAgencyConfig): string {
  const parsed = parseHttpsUrl(url, `efile CAL ${config.agencyKey} export URL`);
  if (!config.allowedExportHosts.includes(parsed.hostname)) {
    throw new Error(
      `efile CAL ${config.agencyKey} export URL host ${parsed.hostname} is not in the allowlist (${config.allowedExportHosts.join(", ")})`
    );
  }
  return parsed.toString();
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

/**
 * Runs a fetch and the full consumption of its response under one deadline:
 * `use` must read everything it needs from the response before returning,
 * so a stalled body stream still trips the timeout. The controller is
 * aborted in `finally`, which is a no-op after full consumption but closes
 * the socket on every error path (size cap, bad magic, non-OK status).
 */
async function withTimedFetch<T>(
  url: string,
  init: RequestInit,
  options: FetchOptions,
  use: (response: Response) => Promise<T>
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? EFILE_CAL_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await (options.fetchImpl ?? fetch)(url, { ...init, signal: controller.signal });
    return await use(response);
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`efile CAL request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
    controller.abort();
  }
}

export async function fetchEfileCalBulkExportUrl(
  config: EfileCalAgencyConfig,
  input: { year: number; mostRecentOnly: boolean },
  options: FetchOptions = {}
): Promise<string> {
  if (!Number.isInteger(input.year) || input.year < 2000 || input.year > 2100) {
    throw new Error(`efile CAL ${config.agencyKey} export year is not plausible: ${input.year}`);
  }
  const portal = parseHttpsUrl(config.portalBaseUrl, `efile CAL ${config.agencyKey} portal base URL`);
  const endpoint = new URL("/api/v1/public/campaign-bulk-export-url", portal);
  endpoint.searchParams.set("year", String(input.year));
  endpoint.searchParams.set("most_recent_only", String(input.mostRecentOnly));

  const body = await withTimedFetch(
    endpoint.toString(),
    { method: "GET", headers: { accept: "application/json" } },
    options,
    async (response): Promise<unknown> => {
      if (!response.ok) {
        throw new Error(
          `efile CAL ${config.agencyKey} bulk-export-url request failed: ${response.status} ${response.statusText}`
        );
      }
      try {
        return await response.json();
      } catch (error) {
        if (isAbortError(error)) throw error;
        throw new Error(`efile CAL ${config.agencyKey} bulk-export-url response is not JSON`);
      }
    }
  );
  const record = body as { success?: unknown; data?: unknown };
  if (record?.success !== true || typeof record.data !== "string") {
    throw new Error(`efile CAL ${config.agencyKey} bulk-export-url response is malformed: ${JSON.stringify(body).slice(0, 200)}`);
  }
  return assertAllowedExportUrl(record.data, config);
}

function metadataFromResponse(url: string, response: Response): EfileCalRemoteWorkbookMetadata {
  const contentLength = response.headers.get("content-length");
  const parsedLength = contentLength ? Number(contentLength) : null;
  return {
    url,
    contentLength: parsedLength !== null && Number.isFinite(parsedLength) ? parsedLength : null,
    contentType: response.headers.get("content-type"),
    etag: response.headers.get("etag"),
    lastModified: response.headers.get("last-modified"),
  };
}

export async function fetchEfileCalWorkbookMetadata(
  url: string,
  config: EfileCalAgencyConfig,
  options: FetchOptions = {}
): Promise<EfileCalRemoteWorkbookMetadata> {
  const allowed = assertAllowedExportUrl(url, config);
  return withTimedFetch(allowed, { method: "HEAD" }, options, async (response) => {
    if (!response.ok) {
      throw new Error(`efile CAL ${config.agencyKey} workbook HEAD failed: ${response.status} ${response.statusText}`);
    }
    return metadataFromResponse(allowed, response);
  });
}

export async function downloadEfileCalWorkbook(
  url: string,
  config: EfileCalAgencyConfig,
  options: FetchOptions & { maxBytes?: number } = {}
): Promise<{ data: Uint8Array; remote: EfileCalRemoteWorkbookMetadata }> {
  const allowed = assertAllowedExportUrl(url, config);
  const maxBytes = options.maxBytes ?? EFILE_CAL_MAX_WORKBOOK_BYTES;
  return withTimedFetch(
    allowed,
    { method: "GET", headers: { accept: "application/octet-stream,*/*;q=0.5" } },
    { ...options, timeoutMs: options.timeoutMs ?? EFILE_CAL_DOWNLOAD_TIMEOUT_MS },
    async (response) => {
      if (!response.ok) {
        throw new Error(
          `efile CAL ${config.agencyKey} workbook download failed: ${response.status} ${response.statusText}`
        );
      }
      const remote = metadataFromResponse(allowed, response);
      if (remote.contentLength !== null && remote.contentLength > maxBytes) {
        throw new Error(
          `efile CAL ${config.agencyKey} workbook is ${remote.contentLength} bytes, over the ${maxBytes}-byte cap`
        );
      }
      if (!response.body) {
        throw new Error(`efile CAL ${config.agencyKey} workbook response did not include a body`);
      }

      const chunks: Uint8Array[] = [];
      let total = 0;
      for await (const chunk of response.body as AsyncIterable<Uint8Array>) {
        total += chunk.byteLength;
        if (total > maxBytes) {
          throw new Error(`efile CAL ${config.agencyKey} workbook exceeded the ${maxBytes}-byte cap while streaming`);
        }
        chunks.push(chunk);
      }
      const data = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        data.set(chunk, offset);
        offset += chunk.byteLength;
      }

      if (!isEfileCalWorkbookData(data)) {
        throw new Error(`efile CAL ${config.agencyKey} workbook download is not an XLSX file; refusing to cache it`);
      }
      return { data, remote };
    }
  );
}

export function getEfileCalWorkbookArtifactCachePaths(input: {
  cacheDir: string;
  agencyKey: string;
  year: number;
  mostRecentOnly: boolean;
}): { cacheDir: string; workbookPath: string; metadataPath: string } {
  const cacheDir = resolve(input.cacheDir);
  const baseName = `${input.agencyKey}_${input.year}_${input.mostRecentOnly ? "most_recent" : "full"}`;
  return {
    cacheDir,
    workbookPath: resolve(cacheDir, `${baseName}.xlsx`),
    metadataPath: resolve(cacheDir, `${baseName}.metadata.json`),
  };
}

export async function readEfileCalWorkbookArtifactCacheMetadata(
  metadataPath: string
): Promise<EfileCalWorkbookArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<EfileCalWorkbookArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.agencyKey !== "string" ||
      typeof parsed.year !== "number" ||
      typeof parsed.mostRecentOnly !== "boolean" ||
      typeof parsed.workbookPath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string"
    ) {
      return null;
    }
    return parsed as EfileCalWorkbookArtifactCacheMetadata;
  } catch {
    return null;
  }
}

function remoteMetadataMatches(
  previous: EfileCalWorkbookArtifactCacheMetadata | null,
  remote: EfileCalRemoteWorkbookMetadata
): boolean {
  if (!previous || previous.remote.url !== remote.url) {
    return false;
  }
  if (previous.remote.etag && remote.etag) {
    return previous.remote.etag === remote.etag;
  }
  if (previous.remote.lastModified && remote.lastModified) {
    return (
      previous.remote.lastModified === remote.lastModified && previous.remote.contentLength === remote.contentLength
    );
  }
  return false;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

export async function refreshEfileCalWorkbookArtifactCache(input: {
  config: EfileCalAgencyConfig;
  year: number;
  mostRecentOnly: boolean;
  cacheDir: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  maxBytes?: number;
  now?: Date;
}): Promise<EfileCalWorkbookArtifactRefreshResult> {
  const paths = getEfileCalWorkbookArtifactCachePaths({
    cacheDir: input.cacheDir,
    agencyKey: input.config.agencyKey,
    year: input.year,
    mostRecentOnly: input.mostRecentOnly,
  });
  const exportUrl = await fetchEfileCalBulkExportUrl(
    input.config,
    { year: input.year, mostRecentOnly: input.mostRecentOnly },
    input
  );
  const remote = await fetchEfileCalWorkbookMetadata(exportUrl, input.config, input);
  const previous = await readEfileCalWorkbookArtifactCacheMetadata(paths.metadataPath);
  const workbookExists = await pathExists(paths.workbookPath);

  if (!input.force && workbookExists && remoteMetadataMatches(previous, remote)) {
    return { status: "unchanged", ...paths, remote, previous, current: previous! };
  }

  const downloaded = await downloadEfileCalWorkbook(exportUrl, input.config, input);
  await mkdir(paths.cacheDir, { recursive: true });
  const tmpPath = `${paths.workbookPath}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(tmpPath, downloaded.data);
  await rename(tmpPath, paths.workbookPath);

  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid efile CAL workbook artifact refresh timestamp");
  }
  const current: EfileCalWorkbookArtifactCacheMetadata = {
    version: 1,
    agencyKey: input.config.agencyKey,
    year: input.year,
    mostRecentOnly: input.mostRecentOnly,
    workbookPath: paths.workbookPath,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    remote: downloaded.remote,
    bytesWritten: downloaded.data.byteLength,
  };
  await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");

  return { status: "downloaded", ...paths, remote, previous, current };
}
