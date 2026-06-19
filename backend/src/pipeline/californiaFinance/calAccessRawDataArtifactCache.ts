import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const CAL_ACCESS_RAW_DATA_ZIP_URL = "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip";
export const CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS = 30_000;
export const CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME = "dbwebexport.zip";
export const CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME = "dbwebexport.metadata.json";

export type CalAccessRemoteZipMetadata = {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type CalAccessRawDataDownloadResult = CalAccessRemoteZipMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type CalAccessRawDataArtifactCacheMetadata = {
  version: 1;
  zipPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: CalAccessRemoteZipMetadata;
  bytesWritten: number;
};

export type CalAccessRawDataArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
  remote: CalAccessRemoteZipMetadata;
  previous: CalAccessRawDataArtifactCacheMetadata | null;
  current: CalAccessRawDataArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function parseCalAccessHttpsUrl(value: string, fieldName = "CAL-ACCESS raw data URL"): string {
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
  const timeoutMs = options.timeoutMs ?? CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS;
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
      throw new Error(`CAL-ACCESS raw data request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(url: string, response: Response): CalAccessRemoteZipMetadata {
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

export async function fetchCalAccessRawDataZipMetadata(
  url = CAL_ACCESS_RAW_DATA_ZIP_URL,
  options: FetchOptions = {}
): Promise<CalAccessRemoteZipMetadata> {
  const normalizedUrl = parseCalAccessHttpsUrl(url, "--url");
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, options);
  if (!response.ok) {
    throw new Error(`Failed to fetch CAL-ACCESS raw data metadata: ${response.status} ${response.statusText}`);
  }
  return metadataFromResponse(normalizedUrl, response);
}

export async function downloadCalAccessRawDataZip(input: {
  url?: string;
  outputPath: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<CalAccessRawDataDownloadResult> {
  const normalizedUrl = parseCalAccessHttpsUrl(input.url ?? CAL_ACCESS_RAW_DATA_ZIP_URL, "--url");
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download CAL-ACCESS raw data ZIP: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("CAL-ACCESS raw data ZIP response did not include a body");
  }

  await pipeline(Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>), createWriteStream(outputPath));
  const outputStat = await stat(outputPath);
  return {
    ...metadataFromResponse(normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getCalAccessRawDataArtifactCachePaths(cacheDir: string): {
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
} {
  const normalizedCacheDir = resolve(cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    zipPath: resolve(normalizedCacheDir, CAL_ACCESS_RAW_DATA_CACHE_ZIP_FILE_NAME),
    metadataPath: resolve(normalizedCacheDir, CAL_ACCESS_RAW_DATA_CACHE_METADATA_FILE_NAME),
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

export async function readCalAccessRawDataArtifactCacheMetadata(
  metadataPath: string
): Promise<CalAccessRawDataArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<CalAccessRawDataArtifactCacheMetadata>;
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
    return parsed as CalAccessRawDataArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    return null;
  }
}

function remoteMetadataMatches(
  previous: CalAccessRawDataArtifactCacheMetadata | null,
  remote: CalAccessRemoteZipMetadata
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

export async function refreshCalAccessRawDataArtifactCache(input: {
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<CalAccessRawDataArtifactRefreshResult> {
  const paths = getCalAccessRawDataArtifactCachePaths(input.cacheDir);
  const remote = await fetchCalAccessRawDataZipMetadata(input.url ?? CAL_ACCESS_RAW_DATA_ZIP_URL, input);
  const previous = await readCalAccessRawDataArtifactCacheMetadata(paths.metadataPath);
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
  const downloaded = await downloadCalAccessRawDataZip({
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  await rename(tmpPath, paths.zipPath);

  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid CAL-ACCESS raw data artifact refresh timestamp");
  }
  const current: CalAccessRawDataArtifactCacheMetadata = {
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
