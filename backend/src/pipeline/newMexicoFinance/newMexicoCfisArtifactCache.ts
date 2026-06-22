import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const NEW_MEXICO_CFIS_DOWNLOAD_BASE_URL =
  "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport";
export const NEW_MEXICO_CFIS_DATA_CATALOG_URL =
  "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCheckDatadownload";
export const NEW_MEXICO_CFIS_FETCH_TIMEOUT_MS = 30_000;
export const DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR = "scratch/new-mexico-campaign-finance/cfis";

export type NewMexicoCfisArtifactKind = "contributions" | "expenditures";

export type NewMexicoCfisArtifactIdentity = {
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
};

export type NewMexicoCfisRemoteArtifactMetadata = NewMexicoCfisArtifactIdentity & {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type NewMexicoCfisArtifactDownloadResult = NewMexicoCfisRemoteArtifactMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type NewMexicoCfisArtifactCacheMetadata = {
  version: 1;
  artifact: NewMexicoCfisArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: NewMexicoCfisRemoteArtifactMetadata;
  bytesWritten: number;
};

export type NewMexicoCfisArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  remote: NewMexicoCfisRemoteArtifactMetadata;
  previous: NewMexicoCfisArtifactCacheMetadata | null;
  current: NewMexicoCfisArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeNewMexicoCfisYear(year: number): number {
  if (!Number.isInteger(year) || year < 2020 || year > 2100) {
    throw new Error(`Invalid New Mexico CFIS artifact year: ${year}`);
  }
  return year;
}

export function normalizeNewMexicoCfisArtifactKind(kind: string): NewMexicoCfisArtifactKind {
  if (kind === "contributions" || kind === "expenditures") {
    return kind;
  }
  throw new Error(`Invalid New Mexico CFIS artifact kind: ${kind}`);
}

export function normalizeNewMexicoCfisArtifactIdentity(input: {
  year: number;
  artifactKind: string;
}): NewMexicoCfisArtifactIdentity {
  return {
    year: normalizeNewMexicoCfisYear(input.year),
    artifactKind: normalizeNewMexicoCfisArtifactKind(input.artifactKind),
  };
}

export function parseNewMexicoCfisHttpsUrl(value: string, fieldName = "New Mexico CFIS URL"): string {
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

export function newMexicoCfisTransactionType(kind: NewMexicoCfisArtifactKind): "CON" | "EXP" {
  return kind === "contributions" ? "CON" : "EXP";
}

function artifactFileName(artifact: NewMexicoCfisArtifactIdentity): string {
  return `${newMexicoCfisTransactionType(artifact.artifactKind)}_${artifact.year}.csv`;
}

function artifactMetadataFileName(artifact: NewMexicoCfisArtifactIdentity): string {
  return `${newMexicoCfisTransactionType(artifact.artifactKind)}_${artifact.year}.metadata.json`;
}

export function buildNewMexicoCfisArtifactUrl(input: {
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
  baseUrl?: string;
}): string {
  const artifact = normalizeNewMexicoCfisArtifactIdentity(input);
  const url = new URL(parseNewMexicoCfisHttpsUrl(input.baseUrl ?? NEW_MEXICO_CFIS_DOWNLOAD_BASE_URL));
  const transactionType = newMexicoCfisTransactionType(artifact.artifactKind);
  url.searchParams.set("year", String(artifact.year));
  url.searchParams.set("transactionType", transactionType);
  url.searchParams.set("reportFormat", "csv");
  url.searchParams.set("fileName", artifactFileName(artifact));
  return url.toString();
}

export function buildNewMexicoCfisDataCatalogUrl(input: {
  pageNumber?: number;
  pageSize?: number;
  sortDir?: "asc" | "desc";
  sortedBy?: string;
  baseUrl?: string;
} = {}): string {
  const pageNumber = input.pageNumber ?? 1;
  const pageSize = input.pageSize ?? 100;
  if (!Number.isInteger(pageNumber) || pageNumber <= 0) {
    throw new Error(`Invalid New Mexico CFIS catalog pageNumber: ${pageNumber}`);
  }
  if (!Number.isInteger(pageSize) || pageSize <= 0) {
    throw new Error(`Invalid New Mexico CFIS catalog pageSize: ${pageSize}`);
  }
  const url = new URL(parseNewMexicoCfisHttpsUrl(input.baseUrl ?? NEW_MEXICO_CFIS_DATA_CATALOG_URL));
  url.searchParams.set("pageNumber", String(pageNumber));
  url.searchParams.set("pageSize", String(pageSize));
  url.searchParams.set("sortDir", input.sortDir ?? "asc");
  url.searchParams.set("sortedBy", input.sortedBy ?? "Year");
  return url.toString();
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? NEW_MEXICO_CFIS_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "text/csv,text/plain;q=0.9,*/*;q=0.1");
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`New Mexico CFIS artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(
  artifact: NewMexicoCfisArtifactIdentity,
  url: string,
  response: Response
): NewMexicoCfisRemoteArtifactMetadata {
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

export async function fetchNewMexicoCfisArtifactMetadata(input: {
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<NewMexicoCfisRemoteArtifactMetadata> {
  const artifact = normalizeNewMexicoCfisArtifactIdentity(input);
  const normalizedUrl = parseNewMexicoCfisHttpsUrl(input.url ?? buildNewMexicoCfisArtifactUrl(artifact), "--url");
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (response.ok) {
    return metadataFromResponse(artifact, normalizedUrl, response);
  }

  if (response.status === 405 || response.status === 501) {
    const fallbackResponse = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
    try {
      if (!fallbackResponse.ok) {
        throw new Error(
          `Failed to fetch New Mexico CFIS artifact metadata: ${fallbackResponse.status} ${fallbackResponse.statusText}`
        );
      }
      return metadataFromResponse(artifact, normalizedUrl, fallbackResponse);
    } finally {
      await fallbackResponse.body?.cancel().catch(() => {});
    }
  }

  {
    throw new Error(`Failed to fetch New Mexico CFIS artifact metadata: ${response.status} ${response.statusText}`);
  }
}

export async function downloadNewMexicoCfisArtifact(input: {
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<NewMexicoCfisArtifactDownloadResult> {
  const artifact = normalizeNewMexicoCfisArtifactIdentity(input);
  const normalizedUrl = parseNewMexicoCfisHttpsUrl(input.url ?? buildNewMexicoCfisArtifactUrl(artifact), "--url");
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download New Mexico CFIS artifact: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("New Mexico CFIS artifact response did not include a body");
  }

  const timeoutMs = input.timeoutMs ?? NEW_MEXICO_CFIS_FETCH_TIMEOUT_MS;
  let timeout: NodeJS.Timeout | undefined;
  let outputStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    timeout = setTimeout(() => {
      source.destroy(new Error(`New Mexico CFIS artifact download timed out after ${timeoutMs}ms for ${normalizedUrl}`));
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
    ...metadataFromResponse(artifact, normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getNewMexicoCfisArtifactCachePaths(input: {
  cacheDir: string;
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
}): {
  cacheDir: string;
  filePath: string;
  metadataPath: string;
} {
  const artifact = normalizeNewMexicoCfisArtifactIdentity(input);
  const normalizedCacheDir = resolve(input.cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    filePath: resolve(normalizedCacheDir, artifactFileName(artifact)),
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

export async function readNewMexicoCfisArtifactCacheMetadata(
  metadataPath: string
): Promise<NewMexicoCfisArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<NewMexicoCfisArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.artifact?.year !== "number" ||
      typeof parsed.artifact?.artifactKind !== "string" ||
      typeof parsed.filePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string" ||
      typeof parsed.remote?.year !== "number" ||
      typeof parsed.remote?.artifactKind !== "string"
    ) {
      return null;
    }
    return parsed as NewMexicoCfisArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading New Mexico CFIS cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: NewMexicoCfisArtifactCacheMetadata | null,
  remote: NewMexicoCfisRemoteArtifactMetadata
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
    throw new Error("Invalid New Mexico CFIS artifact refresh timestamp");
  }
  return normalized;
}

export async function refreshNewMexicoCfisArtifactCache(input: {
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<NewMexicoCfisArtifactRefreshResult> {
  const artifact = normalizeNewMexicoCfisArtifactIdentity(input);
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getNewMexicoCfisArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  const remote = await fetchNewMexicoCfisArtifactMetadata({ ...artifact, url: input.url, ...input });
  const previous = await readNewMexicoCfisArtifactCacheMetadata(paths.metadataPath);
  const fileExists = await pathExists(paths.filePath);

  if (!input.force && fileExists && remoteMetadataMatches(previous, remote)) {
    return {
      status: "unchanged",
      ...paths,
      remote,
      previous,
      current: previous!,
    };
  }

  await mkdir(paths.cacheDir, { recursive: true });
  const tmpPath = `${paths.filePath}.tmp-${process.pid}-${Date.now()}`;
  const downloaded = await downloadNewMexicoCfisArtifact({
    ...artifact,
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  try {
    await rename(tmpPath, paths.filePath);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }

  const current: NewMexicoCfisArtifactCacheMetadata = {
    version: 1,
    artifact,
    filePath: paths.filePath,
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
