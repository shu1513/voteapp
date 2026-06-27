import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const INDIANA_CAMPAIGN_FINANCE_BULK_DATA_BASE_URL =
  "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads";
export const INDIANA_CAMPAIGN_FINANCE_METADATA_FETCH_TIMEOUT_MS = 30_000;
export const INDIANA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS = INDIANA_CAMPAIGN_FINANCE_METADATA_FETCH_TIMEOUT_MS;
export const INDIANA_CAMPAIGN_FINANCE_DOWNLOAD_TIMEOUT_MS = 300_000;
export const DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR = "scratch/indiana-campaign-finance/public-bulk";

export type IndianaCampaignFinanceArtifactKind = "contribution" | "expenditure";

export type IndianaCampaignFinanceArtifactIdentity = {
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
};

export type IndianaCampaignFinanceRemoteArtifactMetadata = IndianaCampaignFinanceArtifactIdentity & {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type IndianaCampaignFinanceArtifactDownloadResult = IndianaCampaignFinanceRemoteArtifactMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type IndianaCampaignFinanceArtifactCacheMetadata = {
  version: 1;
  artifact: IndianaCampaignFinanceArtifactIdentity;
  zipPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: IndianaCampaignFinanceRemoteArtifactMetadata;
  bytesWritten: number;
};

export type IndianaCampaignFinanceArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
  remote: IndianaCampaignFinanceRemoteArtifactMetadata;
  previous: IndianaCampaignFinanceArtifactCacheMetadata | null;
  current: IndianaCampaignFinanceArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

type FetchWithTimeoutResult = {
  response: Response;
  signal: AbortSignal;
  timeoutMs: number;
  clearRequestTimeout: () => void;
};

export function normalizeIndianaCampaignFinanceYear(year: number): number {
  if (!Number.isInteger(year) || year < 2000 || year > 2100) {
    throw new Error(`Invalid Indiana campaign finance artifact year: ${year}`);
  }
  return year;
}

export function normalizeIndianaCampaignFinanceArtifactKind(kind: string): IndianaCampaignFinanceArtifactKind {
  if (kind === "contribution" || kind === "expenditure") {
    return kind;
  }
  throw new Error(`Invalid Indiana campaign finance artifact kind: ${kind}`);
}

export function normalizeIndianaCampaignFinanceArtifactIdentity(input: {
  year: number;
  artifactKind: string;
}): IndianaCampaignFinanceArtifactIdentity {
  return {
    year: normalizeIndianaCampaignFinanceYear(input.year),
    artifactKind: normalizeIndianaCampaignFinanceArtifactKind(input.artifactKind),
  };
}

export function parseIndianaCampaignFinanceHttpsUrl(
  value: string,
  fieldName = "Indiana campaign finance URL"
): string {
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

function officialArtifactPart(kind: IndianaCampaignFinanceArtifactKind): string {
  return kind === "contribution" ? "ContributionData" : "ExpenditureData";
}

export function buildIndianaCampaignFinanceArtifactUrl(input: {
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
  baseUrl?: string;
}): string {
  const artifact = normalizeIndianaCampaignFinanceArtifactIdentity(input);
  const baseUrl = parseIndianaCampaignFinanceHttpsUrl(
    input.baseUrl ?? INDIANA_CAMPAIGN_FINANCE_BULK_DATA_BASE_URL,
    "Indiana campaign finance bulk data base URL"
  ).replace(/\/$/, "");
  return `${baseUrl}/${artifact.year}_${officialArtifactPart(artifact.artifactKind)}.csv.zip`;
}

function artifactZipFileName(artifact: IndianaCampaignFinanceArtifactIdentity): string {
  return `${artifact.year}_${officialArtifactPart(artifact.artifactKind)}.csv.zip`;
}

function artifactMetadataFileName(artifact: IndianaCampaignFinanceArtifactIdentity): string {
  return `${artifact.year}_${artifact.artifactKind}.metadata.json`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function applyIndianaBrowserHeaders(headers: Headers): void {
  if (!headers.has("accept")) {
    headers.set("accept", "application/zip,application/octet-stream;q=0.9,*/*;q=0.1");
  }
  if (!headers.has("user-agent")) {
    headers.set(
      "user-agent",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
    );
  }
}

async function fetchWithTimeout(input: {
  url: string;
  init: RequestInit;
  options: FetchOptions;
  defaultTimeoutMs: number;
}): Promise<FetchWithTimeoutResult> {
  const timeoutMs = input.options.timeoutMs ?? input.defaultTimeoutMs;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(input.init.headers);
  applyIndianaBrowserHeaders(headers);
  let cleared = false;
  const clearRequestTimeout = () => {
    if (!cleared) {
      clearTimeout(timeout);
      cleared = true;
    }
  };

  try {
    const response = await (input.options.fetchImpl ?? fetch)(input.url, {
      ...input.init,
      headers,
      signal: controller.signal,
    });
    return {
      response,
      signal: controller.signal,
      timeoutMs,
      clearRequestTimeout,
    };
  } catch (error) {
    clearRequestTimeout();
    if (isAbortError(error)) {
      throw new Error(`Indiana campaign finance artifact request timed out after ${timeoutMs}ms for ${input.url}`);
    }
    throw error;
  }
}

function metadataFromResponse(
  artifact: IndianaCampaignFinanceArtifactIdentity,
  url: string,
  response: Response
): IndianaCampaignFinanceRemoteArtifactMetadata {
  const contentLength = response.headers.get("content-length");
  const parsedLength = contentLength ? Number(contentLength) : null;
  return {
    ...artifact,
    url,
    contentLength: parsedLength !== null && Number.isFinite(parsedLength) ? parsedLength : null,
    contentType: response.headers.get("content-type"),
    etag: response.headers.get("etag"),
    lastModified: response.headers.get("last-modified"),
  };
}

export async function fetchIndianaCampaignFinanceArtifactMetadata(input: {
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<IndianaCampaignFinanceRemoteArtifactMetadata> {
  const artifact = normalizeIndianaCampaignFinanceArtifactIdentity(input);
  const normalizedUrl = parseIndianaCampaignFinanceHttpsUrl(
    input.url ?? buildIndianaCampaignFinanceArtifactUrl(artifact),
    "--url"
  );
  const request = await fetchWithTimeout({
    url: normalizedUrl,
    init: { method: "HEAD" },
    options: input,
    defaultTimeoutMs: INDIANA_CAMPAIGN_FINANCE_METADATA_FETCH_TIMEOUT_MS,
  });
  try {
    if (!request.response.ok) {
      throw new Error(
        `Failed to fetch Indiana campaign finance artifact metadata: ${request.response.status} ${request.response.statusText}`
      );
    }
    return metadataFromResponse(artifact, normalizedUrl, request.response);
  } finally {
    request.clearRequestTimeout();
  }
}

export async function downloadIndianaCampaignFinanceArtifact(input: {
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<IndianaCampaignFinanceArtifactDownloadResult> {
  const artifact = normalizeIndianaCampaignFinanceArtifactIdentity(input);
  const normalizedUrl = parseIndianaCampaignFinanceHttpsUrl(
    input.url ?? buildIndianaCampaignFinanceArtifactUrl(artifact),
    "--url"
  );
  const outputPath = resolve(input.outputPath);
  const request = await fetchWithTimeout({
    url: normalizedUrl,
    init: { method: "GET" },
    options: input,
    defaultTimeoutMs: INDIANA_CAMPAIGN_FINANCE_DOWNLOAD_TIMEOUT_MS,
  });
  const response = request.response;
  if (!response.ok) {
    request.clearRequestTimeout();
    throw new Error(`Failed to download Indiana campaign finance artifact: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    request.clearRequestTimeout();
    throw new Error("Indiana campaign finance artifact response did not include a body");
  }

  let outputStat;
  try {
    await pipeline(
      Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>, { signal: request.signal }),
      createWriteStream(outputPath)
    );
    outputStat = await stat(outputPath);
  } catch (error) {
    await rm(outputPath, { force: true }).catch(() => {});
    if (isAbortError(error)) {
      throw new Error(`Indiana campaign finance artifact request timed out after ${request.timeoutMs}ms for ${normalizedUrl}`);
    }
    throw error;
  } finally {
    request.clearRequestTimeout();
  }
  return {
    ...metadataFromResponse(artifact, normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getIndianaCampaignFinanceArtifactCachePaths(input: {
  cacheDir: string;
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
}): {
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
} {
  const artifact = normalizeIndianaCampaignFinanceArtifactIdentity(input);
  const normalizedCacheDir = resolve(input.cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    zipPath: resolve(normalizedCacheDir, artifactZipFileName(artifact)),
    metadataPath: resolve(normalizedCacheDir, artifactMetadataFileName(artifact)),
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

export async function readIndianaCampaignFinanceArtifactCacheMetadata(
  metadataPath: string
): Promise<IndianaCampaignFinanceArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<IndianaCampaignFinanceArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.artifact?.year !== "number" ||
      typeof parsed.artifact?.artifactKind !== "string" ||
      typeof parsed.zipPath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string" ||
      typeof parsed.remote?.year !== "number" ||
      typeof parsed.remote?.artifactKind !== "string"
    ) {
      return null;
    }
    return parsed as IndianaCampaignFinanceArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Indiana campaign finance cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: IndianaCampaignFinanceArtifactCacheMetadata | null,
  remote: IndianaCampaignFinanceRemoteArtifactMetadata
): boolean {
  if (!previous || previous.artifact.year !== remote.year || previous.artifact.artifactKind !== remote.artifactKind) {
    return false;
  }
  if (previous.remote.url !== remote.url) {
    return false;
  }
  const checks: boolean[] = [];
  if (remote.etag && previous.remote.etag) {
    checks.push(remote.etag === previous.remote.etag);
  }
  if (remote.lastModified && previous.remote.lastModified) {
    checks.push(remote.lastModified === previous.remote.lastModified);
  }
  if (remote.contentLength !== null) {
    checks.push(previous.bytesWritten === remote.contentLength);
  }
  return checks.length > 0 && checks.every(Boolean);
}

export async function refreshIndianaCampaignFinanceArtifactCache(input: {
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<IndianaCampaignFinanceArtifactRefreshResult> {
  const artifact = normalizeIndianaCampaignFinanceArtifactIdentity(input);
  const paths = getIndianaCampaignFinanceArtifactCachePaths({
    cacheDir: input.cacheDir,
    year: artifact.year,
    artifactKind: artifact.artifactKind,
  });
  const previous = await readIndianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  const remote = await fetchIndianaCampaignFinanceArtifactMetadata({
    year: artifact.year,
    artifactKind: artifact.artifactKind,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });

  if (!input.force && previous && remoteMetadataMatches(previous, remote) && (await pathExists(paths.zipPath))) {
    return {
      status: "unchanged",
      ...paths,
      remote,
      previous,
      current: previous,
    };
  }

  await mkdir(paths.cacheDir, { recursive: true });
  const tempPath = `${paths.zipPath}.tmp-${process.pid}-${Date.now()}`;
  const download = await downloadIndianaCampaignFinanceArtifact({
    year: artifact.year,
    artifactKind: artifact.artifactKind,
    outputPath: tempPath,
    url: remote.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  await rename(tempPath, paths.zipPath);

  const current: IndianaCampaignFinanceArtifactCacheMetadata = {
    version: 1,
    artifact,
    zipPath: paths.zipPath,
    metadataPath: paths.metadataPath,
    downloadedAt: (input.now ?? new Date()).toISOString(),
    remote: {
      ...remote,
      contentLength: download.contentLength,
      contentType: download.contentType,
      etag: download.etag,
      lastModified: download.lastModified,
    },
    bytesWritten: download.bytesWritten,
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
