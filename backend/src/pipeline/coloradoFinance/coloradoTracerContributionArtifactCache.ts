import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const COLORADO_TRACER_BULK_DATA_BASE_URL =
  "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads";
export const COLORADO_TRACER_CONTRIBUTION_FETCH_TIMEOUT_MS = 900_000;
export const DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR =
  "scratch/colorado-campaign-finance/contributions";

export type ColoradoTracerContributionRemoteZipMetadata = {
  year: number;
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type ColoradoTracerContributionDownloadResult = ColoradoTracerContributionRemoteZipMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type ColoradoTracerContributionArtifactCacheMetadata = {
  version: 1;
  year: number;
  zipPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: ColoradoTracerContributionRemoteZipMetadata;
  bytesWritten: number;
};

export type ColoradoTracerContributionArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
  remote: ColoradoTracerContributionRemoteZipMetadata;
  previous: ColoradoTracerContributionArtifactCacheMetadata | null;
  current: ColoradoTracerContributionArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeColoradoTracerContributionYear(year: number): number {
  if (!Number.isInteger(year) || year < 2001 || year > 2100) {
    throw new Error(`Invalid Colorado TRACER contribution year: ${year}`);
  }
  return year;
}

export function parseColoradoTracerHttpsUrl(value: string, fieldName = "Colorado TRACER URL"): string {
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

export function buildColoradoTracerContributionZipUrl(input: {
  year: number;
  baseUrl?: string;
}): string {
  const year = normalizeColoradoTracerContributionYear(input.year);
  const baseUrl = parseColoradoTracerHttpsUrl(
    input.baseUrl ?? COLORADO_TRACER_BULK_DATA_BASE_URL,
    "Colorado TRACER bulk data base URL"
  ).replace(/\/$/, "");
  return `${baseUrl}/${year}_ContributionData.csv.zip`;
}

function contributionZipFileName(year: number): string {
  return `${normalizeColoradoTracerContributionYear(year)}_ContributionData.csv.zip`;
}

function contributionMetadataFileName(year: number): string {
  return `${normalizeColoradoTracerContributionYear(year)}_ContributionData.metadata.json`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? COLORADO_TRACER_CONTRIBUTION_FETCH_TIMEOUT_MS;
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
      throw new Error(`Colorado TRACER contribution ZIP request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(
  year: number,
  url: string,
  response: Response
): ColoradoTracerContributionRemoteZipMetadata {
  const contentLength = response.headers.get("content-length");
  const parsedLength = contentLength ? Number(contentLength) : null;
  return {
    year,
    url,
    contentLength: parsedLength !== null && Number.isFinite(parsedLength) ? parsedLength : null,
    contentType: response.headers.get("content-type"),
    etag: response.headers.get("etag"),
    lastModified: response.headers.get("last-modified"),
  };
}

export async function fetchColoradoTracerContributionZipMetadata(input: {
  year: number;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<ColoradoTracerContributionRemoteZipMetadata> {
  const year = normalizeColoradoTracerContributionYear(input.year);
  const normalizedUrl = parseColoradoTracerHttpsUrl(
    input.url ?? buildColoradoTracerContributionZipUrl({ year }),
    "--url"
  );
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (!response.ok) {
    throw new Error(`Failed to fetch Colorado TRACER contribution ZIP metadata: ${response.status} ${response.statusText}`);
  }
  return metadataFromResponse(year, normalizedUrl, response);
}

export async function downloadColoradoTracerContributionZip(input: {
  year: number;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<ColoradoTracerContributionDownloadResult> {
  const year = normalizeColoradoTracerContributionYear(input.year);
  const normalizedUrl = parseColoradoTracerHttpsUrl(
    input.url ?? buildColoradoTracerContributionZipUrl({ year }),
    "--url"
  );
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Colorado TRACER contribution ZIP: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Colorado TRACER contribution ZIP response did not include a body");
  }

  await pipeline(Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>), createWriteStream(outputPath));
  const outputStat = await stat(outputPath);
  return {
    ...metadataFromResponse(year, normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getColoradoTracerContributionArtifactCachePaths(input: {
  cacheDir: string;
  year: number;
}): {
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
} {
  const year = normalizeColoradoTracerContributionYear(input.year);
  const normalizedCacheDir = resolve(input.cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    zipPath: resolve(normalizedCacheDir, contributionZipFileName(year)),
    metadataPath: resolve(normalizedCacheDir, contributionMetadataFileName(year)),
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

export async function readColoradoTracerContributionArtifactCacheMetadata(
  metadataPath: string
): Promise<ColoradoTracerContributionArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<ColoradoTracerContributionArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.year !== "number" ||
      typeof parsed.zipPath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string" ||
      typeof parsed.remote?.year !== "number"
    ) {
      return null;
    }
    return parsed as ColoradoTracerContributionArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Colorado TRACER cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: ColoradoTracerContributionArtifactCacheMetadata | null,
  remote: ColoradoTracerContributionRemoteZipMetadata
): boolean {
  if (!previous || previous.year !== remote.year || previous.remote.url !== remote.url) {
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

export async function refreshColoradoTracerContributionArtifactCache(input: {
  year: number;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<ColoradoTracerContributionArtifactRefreshResult> {
  const year = normalizeColoradoTracerContributionYear(input.year);
  const paths = getColoradoTracerContributionArtifactCachePaths({ cacheDir: input.cacheDir, year });
  const remote = await fetchColoradoTracerContributionZipMetadata({
    year,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readColoradoTracerContributionArtifactCacheMetadata(paths.metadataPath);
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
  const downloaded = await downloadColoradoTracerContributionZip({
    year,
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  await rename(tmpPath, paths.zipPath);

  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid Colorado TRACER contribution artifact refresh timestamp");
  }
  const current: ColoradoTracerContributionArtifactCacheMetadata = {
    version: 1,
    year,
    zipPath: paths.zipPath,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    remote: {
      year,
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
