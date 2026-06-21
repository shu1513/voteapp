import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const NEBRASKA_NADC_BULK_DATA_BASE_URL =
  "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads";
export const NEBRASKA_NADC_FETCH_TIMEOUT_MS = 30_000;
export const DEFAULT_NEBRASKA_NADC_CACHE_DIR = "scratch/nebraska-campaign-finance/nadc";

export type NebraskaNadcArtifactKind = "contribution_loan" | "expenditure";

export type NebraskaNadcArtifactIdentity = {
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
};

export type NebraskaNadcRemoteArtifactMetadata = NebraskaNadcArtifactIdentity & {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type NebraskaNadcArtifactDownloadResult = NebraskaNadcRemoteArtifactMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type NebraskaNadcArtifactCacheMetadata = {
  version: 1;
  artifact: NebraskaNadcArtifactIdentity;
  zipPath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: NebraskaNadcRemoteArtifactMetadata;
  bytesWritten: number;
};

export type NebraskaNadcArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
  remote: NebraskaNadcRemoteArtifactMetadata;
  previous: NebraskaNadcArtifactCacheMetadata | null;
  current: NebraskaNadcArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeNebraskaNadcYear(year: number): number {
  if (!Number.isInteger(year) || year < 2021 || year > 2100) {
    throw new Error(`Invalid Nebraska NADC artifact year: ${year}`);
  }
  return year;
}

export function normalizeNebraskaNadcArtifactKind(kind: string): NebraskaNadcArtifactKind {
  if (kind === "contribution_loan" || kind === "expenditure") {
    return kind;
  }
  throw new Error(`Invalid Nebraska NADC artifact kind: ${kind}`);
}

export function normalizeNebraskaNadcArtifactIdentity(input: {
  year: number;
  artifactKind: string;
}): NebraskaNadcArtifactIdentity {
  return {
    year: normalizeNebraskaNadcYear(input.year),
    artifactKind: normalizeNebraskaNadcArtifactKind(input.artifactKind),
  };
}

export function parseNebraskaNadcHttpsUrl(value: string, fieldName = "Nebraska NADC URL"): string {
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

function officialArtifactPart(kind: NebraskaNadcArtifactKind): string {
  return kind === "contribution_loan" ? "ContributionLoan" : "Expenditure";
}

export function buildNebraskaNadcArtifactUrl(input: {
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
  baseUrl?: string;
}): string {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
  const baseUrl = parseNebraskaNadcHttpsUrl(
    input.baseUrl ?? NEBRASKA_NADC_BULK_DATA_BASE_URL,
    "Nebraska NADC bulk data base URL"
  ).replace(/\/$/, "");
  return `${baseUrl}/${artifact.year}_${officialArtifactPart(artifact.artifactKind)}Extract.csv.zip`;
}

function artifactZipFileName(artifact: NebraskaNadcArtifactIdentity): string {
  return `${artifact.year}_${officialArtifactPart(artifact.artifactKind)}Extract.csv.zip`;
}

function artifactMetadataFileName(artifact: NebraskaNadcArtifactIdentity): string {
  return `${artifact.year}_${artifact.artifactKind}.metadata.json`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? NEBRASKA_NADC_FETCH_TIMEOUT_MS;
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
      throw new Error(`Nebraska NADC artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(
  artifact: NebraskaNadcArtifactIdentity,
  url: string,
  response: Response
): NebraskaNadcRemoteArtifactMetadata {
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

export async function fetchNebraskaNadcArtifactMetadata(input: {
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<NebraskaNadcRemoteArtifactMetadata> {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
  const normalizedUrl = parseNebraskaNadcHttpsUrl(input.url ?? buildNebraskaNadcArtifactUrl(artifact), "--url");
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (!response.ok) {
    throw new Error(`Failed to fetch Nebraska NADC artifact metadata: ${response.status} ${response.statusText}`);
  }
  return metadataFromResponse(artifact, normalizedUrl, response);
}

export async function downloadNebraskaNadcArtifact(input: {
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<NebraskaNadcArtifactDownloadResult> {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
  const normalizedUrl = parseNebraskaNadcHttpsUrl(input.url ?? buildNebraskaNadcArtifactUrl(artifact), "--url");
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Nebraska NADC artifact: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Nebraska NADC artifact response did not include a body");
  }

  await pipeline(Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>), createWriteStream(outputPath));
  const outputStat = await stat(outputPath);
  return {
    ...metadataFromResponse(artifact, normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getNebraskaNadcArtifactCachePaths(input: {
  cacheDir: string;
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
}): {
  cacheDir: string;
  zipPath: string;
  metadataPath: string;
} {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
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

export async function readNebraskaNadcArtifactCacheMetadata(
  metadataPath: string
): Promise<NebraskaNadcArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<NebraskaNadcArtifactCacheMetadata>;
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
    return parsed as NebraskaNadcArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Nebraska NADC cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: NebraskaNadcArtifactCacheMetadata | null,
  remote: NebraskaNadcRemoteArtifactMetadata
): boolean {
  if (!previous || previous.remote.url !== remote.url) {
    return false;
  }
  if (previous.artifact.year !== remote.year || previous.artifact.artifactKind !== remote.artifactKind) {
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

export async function refreshNebraskaNadcArtifactCache(input: {
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<NebraskaNadcArtifactRefreshResult> {
  const artifact = normalizeNebraskaNadcArtifactIdentity(input);
  const paths = getNebraskaNadcArtifactCachePaths({ cacheDir: input.cacheDir, ...artifact });
  const remote = await fetchNebraskaNadcArtifactMetadata({
    ...artifact,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readNebraskaNadcArtifactCacheMetadata(paths.metadataPath);
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
  const downloaded = await downloadNebraskaNadcArtifact({
    ...artifact,
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  await rename(tmpPath, paths.zipPath);

  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid Nebraska NADC artifact refresh timestamp");
  }
  const current: NebraskaNadcArtifactCacheMetadata = {
    version: 1,
    artifact,
    zipPath: paths.zipPath,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    remote: {
      ...artifact,
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
