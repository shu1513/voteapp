import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const TEXAS_TEC_CSV_DATABASE_URL =
  "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip";
export const TEXAS_TEC_CSV_DATABASE_FETCH_TIMEOUT_MS = 30_000;
export const DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR = "scratch/texas-campaign-finance/tec";
export const TEXAS_TEC_CSV_DATABASE_CACHE_ZIP_FILE_NAME = "TEC_CF_CSV.zip";
export const TEXAS_TEC_CSV_DATABASE_CACHE_METADATA_FILE_NAME = "TEC_CF_CSV.metadata.json";

export type TexasTecCsvDatabaseRemoteMetadata = {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type TexasTecCsvDatabaseDownloadResult = TexasTecCsvDatabaseRemoteMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type TexasTecCsvDatabaseArtifactCacheMetadata = {
  version: 1;
  zipPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: TexasTecCsvDatabaseRemoteMetadata;
  bytesWritten: number;
};

export type TexasTecCsvDatabaseArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
  remote: TexasTecCsvDatabaseRemoteMetadata;
  previous: TexasTecCsvDatabaseArtifactCacheMetadata | null;
  current: TexasTecCsvDatabaseArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function parseTexasTecHttpsUrl(value: string, fieldName = "Texas TEC CSV database URL"): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Invalid ${fieldName}: ${value}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Invalid ${fieldName} protocol: ${parsed.protocol}. Only https is allowed.`);
  }
  return parsed.toString();
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? TEXAS_TEC_CSV_DATABASE_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "application/zip,application/octet-stream;q=0.9,*/*;q=0.1");
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Texas TEC CSV database request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(url: string, response: Response): TexasTecCsvDatabaseRemoteMetadata {
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

export async function fetchTexasTecCsvDatabaseMetadata(input: {
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
} = {}): Promise<TexasTecCsvDatabaseRemoteMetadata> {
  const normalizedUrl = parseTexasTecHttpsUrl(input.url ?? TEXAS_TEC_CSV_DATABASE_URL, "--url");
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (!response.ok) {
    throw new Error(`Failed to fetch Texas TEC CSV database metadata: ${response.status} ${response.statusText}`);
  }
  return metadataFromResponse(normalizedUrl, response);
}

export async function downloadTexasTecCsvDatabase(input: {
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<TexasTecCsvDatabaseDownloadResult> {
  const normalizedUrl = parseTexasTecHttpsUrl(input.url ?? TEXAS_TEC_CSV_DATABASE_URL, "--url");
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Texas TEC CSV database: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Texas TEC CSV database response did not include a body");
  }

  const timeoutMs = input.timeoutMs ?? TEXAS_TEC_CSV_DATABASE_FETCH_TIMEOUT_MS;
  let timeout: NodeJS.Timeout | undefined;
  let outputStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    timeout = setTimeout(() => {
      source.destroy(new Error(`Texas TEC CSV database download timed out after ${timeoutMs}ms for ${normalizedUrl}`));
    }, timeoutMs);
    await pipeline(source, createWriteStream(outputPath));
    outputStat = await stat(outputPath);
  } catch (error) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw error;
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }

  return {
    ...metadataFromResponse(normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getTexasTecCsvDatabaseArtifactCachePaths(cacheDir: string): {
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
} {
  const normalizedCacheDir = resolve(cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    zipPath: resolve(normalizedCacheDir, TEXAS_TEC_CSV_DATABASE_CACHE_ZIP_FILE_NAME),
    metadataPath: resolve(normalizedCacheDir, TEXAS_TEC_CSV_DATABASE_CACHE_METADATA_FILE_NAME),
  };
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

export async function readTexasTecCsvDatabaseArtifactCacheMetadata(
  metadataPath: string
): Promise<TexasTecCsvDatabaseArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<TexasTecCsvDatabaseArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.zipPath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string"
    ) {
      return null;
    }
    return parsed as TexasTecCsvDatabaseArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Texas TEC CSV database cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: TexasTecCsvDatabaseArtifactCacheMetadata | null,
  remote: TexasTecCsvDatabaseRemoteMetadata
): boolean {
  if (!previous || previous.remote.url !== remote.url) {
    return false;
  }
  if (previous.remote.etag && remote.etag) {
    return previous.remote.etag === remote.etag;
  }
  if (previous.remote.lastModified && remote.lastModified) {
    return (
      previous.remote.lastModified === remote.lastModified &&
      previous.remote.contentLength === remote.contentLength
    );
  }
  return false;
}

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Texas TEC CSV database artifact refresh timestamp");
  }
  return normalized;
}

export async function refreshTexasTecCsvDatabaseArtifactCache(input: {
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<TexasTecCsvDatabaseArtifactRefreshResult> {
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getTexasTecCsvDatabaseArtifactCachePaths(input.cacheDir);
  const remote = await fetchTexasTecCsvDatabaseMetadata({
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readTexasTecCsvDatabaseArtifactCacheMetadata(paths.metadataPath);
  const zipExists = await pathExists(paths.zipPath);

  if (!input.force && zipExists && remoteMetadataMatches(previous, remote)) {
    return {
      status: "unchanged",
      ...paths,
      remote,
      previous,
      current: previous!,
    };
  }

  await mkdir(paths.cacheDir, { recursive: true });
  const tmpPath = `${paths.zipPath}.tmp-${process.pid}-${Date.now()}`;
  const downloaded = await downloadTexasTecCsvDatabase({
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  try {
    await rename(tmpPath, paths.zipPath);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }

  const current: TexasTecCsvDatabaseArtifactCacheMetadata = {
    version: 1,
    zipPath: paths.zipPath,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    remote: {
      url: downloaded.url,
      contentLength: downloaded.contentLength,
      contentType: downloaded.contentType,
      etag: downloaded.etag,
      lastModified: downloaded.lastModified,
    },
    bytesWritten: downloaded.bytesWritten,
  };
  await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");

  return {
    status: "downloaded",
    ...paths,
    remote,
    previous,
    current,
  };
}
