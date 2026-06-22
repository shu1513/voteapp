import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const OKLAHOMA_GUARDIAN_BULK_DATA_BASE_URL =
  "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads";
export const OKLAHOMA_GUARDIAN_CONTRIBUTION_FETCH_TIMEOUT_MS = 30_000;
export const DEFAULT_OKLAHOMA_GUARDIAN_CONTRIBUTION_CACHE_DIR =
  "scratch/oklahoma-campaign-finance/guardian/contributions";

export type OklahomaGuardianContributionRemoteZipMetadata = {
  year: number;
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type OklahomaGuardianContributionDownloadResult = OklahomaGuardianContributionRemoteZipMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type OklahomaGuardianContributionArtifactCacheMetadata = {
  version: 1;
  year: number;
  zipPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: OklahomaGuardianContributionRemoteZipMetadata;
  bytesWritten: number;
};

export type OklahomaGuardianContributionArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
  remote: OklahomaGuardianContributionRemoteZipMetadata;
  previous: OklahomaGuardianContributionArtifactCacheMetadata | null;
  current: OklahomaGuardianContributionArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeOklahomaGuardianContributionYear(year: number): number {
  if (!Number.isInteger(year) || year < 2014 || year > 2100) {
    throw new Error(`Invalid Oklahoma Guardian contribution year: ${year}`);
  }
  return year;
}

export function parseOklahomaGuardianHttpsUrl(value: string, fieldName = "Oklahoma Guardian URL"): string {
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

export function buildOklahomaGuardianContributionZipUrl(input: {
  year: number;
  baseUrl?: string;
}): string {
  const year = normalizeOklahomaGuardianContributionYear(input.year);
  const baseUrl = parseOklahomaGuardianHttpsUrl(
    input.baseUrl ?? OKLAHOMA_GUARDIAN_BULK_DATA_BASE_URL,
    "Oklahoma Guardian bulk data base URL"
  ).replace(/\/$/, "");
  return `${baseUrl}/${year}_ContributionLoanExtract.csv.zip`;
}

function contributionZipFileName(year: number): string {
  return `${normalizeOklahomaGuardianContributionYear(year)}_ContributionLoanExtract.csv.zip`;
}

function contributionMetadataFileName(year: number): string {
  return `${normalizeOklahomaGuardianContributionYear(year)}_ContributionLoanExtract.metadata.json`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? OKLAHOMA_GUARDIAN_CONTRIBUTION_FETCH_TIMEOUT_MS;
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
      throw new Error(`Oklahoma Guardian contribution ZIP request timed out after ${timeoutMs}ms for ${url}`);
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
): OklahomaGuardianContributionRemoteZipMetadata {
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

export async function fetchOklahomaGuardianContributionZipMetadata(input: {
  year: number;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<OklahomaGuardianContributionRemoteZipMetadata> {
  const year = normalizeOklahomaGuardianContributionYear(input.year);
  const normalizedUrl = parseOklahomaGuardianHttpsUrl(
    input.url ?? buildOklahomaGuardianContributionZipUrl({ year }),
    "--url"
  );
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch Oklahoma Guardian contribution ZIP metadata: ${response.status} ${response.statusText}`
    );
  }
  return metadataFromResponse(year, normalizedUrl, response);
}

export async function downloadOklahomaGuardianContributionZip(input: {
  year: number;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<OklahomaGuardianContributionDownloadResult> {
  const year = normalizeOklahomaGuardianContributionYear(input.year);
  const normalizedUrl = parseOklahomaGuardianHttpsUrl(
    input.url ?? buildOklahomaGuardianContributionZipUrl({ year }),
    "--url"
  );
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Oklahoma Guardian contribution ZIP: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Oklahoma Guardian contribution ZIP response did not include a body");
  }

  let outputStat;
  try {
    await pipeline(Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>), createWriteStream(outputPath));
    outputStat = await stat(outputPath);
  } catch (error) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw error;
  }

  return {
    ...metadataFromResponse(year, normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getOklahomaGuardianContributionArtifactCachePaths(input: {
  cacheDir: string;
  year: number;
}): {
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
} {
  const year = normalizeOklahomaGuardianContributionYear(input.year);
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

export async function readOklahomaGuardianContributionArtifactCacheMetadata(
  metadataPath: string
): Promise<OklahomaGuardianContributionArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<OklahomaGuardianContributionArtifactCacheMetadata>;
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
    return parsed as OklahomaGuardianContributionArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Oklahoma Guardian contribution cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: OklahomaGuardianContributionArtifactCacheMetadata | null,
  remote: OklahomaGuardianContributionRemoteZipMetadata
): boolean {
  if (!previous || previous.year !== remote.year || previous.remote.url !== remote.url) {
    return false;
  }
  if (previous.remote.etag && remote.etag) {
    return previous.remote.etag === remote.etag;
  }
  if (previous.remote.lastModified && remote.lastModified) {
    return previous.remote.lastModified === remote.lastModified && previous.remote.contentLength === remote.contentLength;
  }
  return false;
}

export async function refreshOklahomaGuardianContributionArtifactCache(input: {
  year: number;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<OklahomaGuardianContributionArtifactRefreshResult> {
  const year = normalizeOklahomaGuardianContributionYear(input.year);
  const paths = getOklahomaGuardianContributionArtifactCachePaths({ cacheDir: input.cacheDir, year });
  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid Oklahoma Guardian contribution artifact refresh timestamp");
  }
  const remote = await fetchOklahomaGuardianContributionZipMetadata({
    year,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readOklahomaGuardianContributionArtifactCacheMetadata(paths.metadataPath);
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
  let downloaded: OklahomaGuardianContributionDownloadResult;
  try {
    downloaded = await downloadOklahomaGuardianContributionZip({
      year,
      url: remote.url,
      outputPath: tmpPath,
      fetchImpl: input.fetchImpl,
      timeoutMs: input.timeoutMs,
    });
    await rename(tmpPath, paths.zipPath);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }

  const current: OklahomaGuardianContributionArtifactCacheMetadata = {
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
