import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const CONNECTICUT_ECRIS_DOWNLOAD_PAGE_URL = "https://seec.ct.gov/portal/ecris/CurPreYears";
export const CONNECTICUT_ECRIS_EXPORT_BASE_URL =
  "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles";
export const CONNECTICUT_ECRIS_FETCH_TIMEOUT_MS = 30_000;
export const DEFAULT_CONNECTICUT_ECRIS_CACHE_DIR = "scratch/connecticut-campaign-finance/ecris";

export type ConnecticutEcrisArtifactTransactionType = "receipts" | "disbursements";
export type ConnecticutEcrisArtifactCommitteeType = "candidate_exploratory" | "party_pac";
export type ConnecticutEcrisArtifactPeriod = "election" | "calendar";
export type ConnecticutEcrisArtifactFormat = "csv" | "xlsx" | "xls";

export type ConnecticutEcrisArtifactIdentity = {
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format: ConnecticutEcrisArtifactFormat;
};

export type ConnecticutEcrisRemoteArtifactMetadata = ConnecticutEcrisArtifactIdentity & {
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type ConnecticutEcrisArtifactDownloadResult = ConnecticutEcrisRemoteArtifactMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type ConnecticutEcrisArtifactCacheMetadata = {
  version: 1;
  artifact: ConnecticutEcrisArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: ConnecticutEcrisRemoteArtifactMetadata;
  bytesWritten: number;
};

export type ConnecticutEcrisArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  remote: ConnecticutEcrisRemoteArtifactMetadata;
  previous: ConnecticutEcrisArtifactCacheMetadata | null;
  current: ConnecticutEcrisArtifactCacheMetadata;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export function normalizeConnecticutEcrisYear(year: number): number {
  if (!Number.isInteger(year) || year < 2008 || year > 2100) {
    throw new Error(`Invalid Connecticut eCRIS artifact year: ${year}`);
  }
  return year;
}

export function parseConnecticutEcrisHttpsUrl(value: string, fieldName = "Connecticut eCRIS URL"): string {
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

export function defaultConnecticutEcrisArtifactFormat(year: number): ConnecticutEcrisArtifactFormat {
  const normalizedYear = normalizeConnecticutEcrisYear(year);
  if (normalizedYear >= 2025) {
    return "csv";
  }
  if (normalizedYear >= 2022) {
    return "xlsx";
  }
  return "csv";
}

function transactionFilePrefix(transactionType: ConnecticutEcrisArtifactTransactionType): string {
  return transactionType === "receipts" ? "Receipts" : "Disbursements";
}

function periodFilePart(period: ConnecticutEcrisArtifactPeriod): string {
  return period === "election" ? "ElectionYear" : "CalendarYear";
}

function committeeFilePart(committeeType: ConnecticutEcrisArtifactCommitteeType): string {
  return committeeType === "candidate_exploratory" ? "CandidateExploratoryCommittees" : "PartyPACCommittees";
}

function validateArtifactPair(input: {
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
}): void {
  if (input.committeeType === "candidate_exploratory" && input.period !== "election") {
    throw new Error("Connecticut eCRIS candidate/exploratory artifacts use election-year files");
  }
  if (input.committeeType === "party_pac" && input.period !== "calendar") {
    throw new Error("Connecticut eCRIS party/PAC artifacts use calendar-year files");
  }
}

export function normalizeConnecticutEcrisArtifactIdentity(input: {
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
}): ConnecticutEcrisArtifactIdentity {
  const year = normalizeConnecticutEcrisYear(input.year);
  validateArtifactPair(input);
  return {
    year,
    transactionType: input.transactionType,
    committeeType: input.committeeType,
    period: input.period,
    format: input.format ?? defaultConnecticutEcrisArtifactFormat(year),
  };
}

export function buildConnecticutEcrisArtifactUrl(input: {
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
  baseUrl?: string;
}): string {
  const artifact = normalizeConnecticutEcrisArtifactIdentity(input);
  const baseUrl = parseConnecticutEcrisHttpsUrl(
    input.baseUrl ?? CONNECTICUT_ECRIS_EXPORT_BASE_URL,
    "Connecticut eCRIS export base URL"
  ).replace(/\/$/, "");
  const fileName = `${transactionFilePrefix(artifact.transactionType)}${artifact.year}${periodFilePart(
    artifact.period
  )}${committeeFilePart(artifact.committeeType)}.${artifact.format}`;
  return `${baseUrl}/${fileName}`;
}

function artifactFileStem(artifact: ConnecticutEcrisArtifactIdentity): string {
  return `${artifact.year}_${artifact.period}_${artifact.committeeType}_${artifact.transactionType}`;
}

function artifactFileName(artifact: ConnecticutEcrisArtifactIdentity): string {
  return `${artifactFileStem(artifact)}.${artifact.format}`;
}

function artifactMetadataFileName(artifact: ConnecticutEcrisArtifactIdentity): string {
  return `${artifactFileStem(artifact)}.metadata.json`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? CONNECTICUT_ECRIS_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set(
      "accept",
      "text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel;q=0.9,*/*;q=0.1"
    );
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Connecticut eCRIS artifact request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function metadataFromResponse(
  artifact: ConnecticutEcrisArtifactIdentity,
  url: string,
  response: Response
): ConnecticutEcrisRemoteArtifactMetadata {
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

export async function fetchConnecticutEcrisArtifactMetadata(input: {
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<ConnecticutEcrisRemoteArtifactMetadata> {
  const artifact = normalizeConnecticutEcrisArtifactIdentity(input);
  const normalizedUrl = parseConnecticutEcrisHttpsUrl(input.url ?? buildConnecticutEcrisArtifactUrl(artifact), "--url");
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (!response.ok) {
    throw new Error(`Failed to fetch Connecticut eCRIS artifact metadata: ${response.status} ${response.statusText}`);
  }
  return metadataFromResponse(artifact, normalizedUrl, response);
}

export async function downloadConnecticutEcrisArtifact(input: {
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<ConnecticutEcrisArtifactDownloadResult> {
  const artifact = normalizeConnecticutEcrisArtifactIdentity(input);
  const normalizedUrl = parseConnecticutEcrisHttpsUrl(input.url ?? buildConnecticutEcrisArtifactUrl(artifact), "--url");
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Connecticut eCRIS artifact: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Connecticut eCRIS artifact response did not include a body");
  }

  await pipeline(Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>), createWriteStream(outputPath));
  const outputStat = await stat(outputPath);
  return {
    ...metadataFromResponse(artifact, normalizedUrl, response),
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getConnecticutEcrisArtifactCachePaths(input: {
  cacheDir: string;
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
}): {
  cacheDir: string;
  filePath: string;
  metadataPath: string;
} {
  const artifact = normalizeConnecticutEcrisArtifactIdentity(input);
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

export async function readConnecticutEcrisArtifactCacheMetadata(
  metadataPath: string
): Promise<ConnecticutEcrisArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<ConnecticutEcrisArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.artifact?.year !== "number" ||
      typeof parsed.artifact?.transactionType !== "string" ||
      typeof parsed.artifact?.committeeType !== "string" ||
      typeof parsed.artifact?.period !== "string" ||
      typeof parsed.artifact?.format !== "string" ||
      typeof parsed.filePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string" ||
      typeof parsed.remote?.year !== "number"
    ) {
      return null;
    }
    return parsed as ConnecticutEcrisArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Connecticut eCRIS cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: ConnecticutEcrisArtifactCacheMetadata | null,
  remote: ConnecticutEcrisRemoteArtifactMetadata
): boolean {
  if (!previous || previous.remote.url !== remote.url) {
    return false;
  }
  if (
    previous.artifact.year !== remote.year ||
    previous.artifact.transactionType !== remote.transactionType ||
    previous.artifact.committeeType !== remote.committeeType ||
    previous.artifact.period !== remote.period ||
    previous.artifact.format !== remote.format
  ) {
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

export async function refreshConnecticutEcrisArtifactCache(input: {
  cacheDir: string;
  year: number;
  transactionType: ConnecticutEcrisArtifactTransactionType;
  committeeType: ConnecticutEcrisArtifactCommitteeType;
  period: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<ConnecticutEcrisArtifactRefreshResult> {
  const artifact = normalizeConnecticutEcrisArtifactIdentity(input);
  const paths = getConnecticutEcrisArtifactCachePaths({ ...input, ...artifact });
  const remote = await fetchConnecticutEcrisArtifactMetadata({
    ...artifact,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readConnecticutEcrisArtifactCacheMetadata(paths.metadataPath);
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
  const downloaded = await downloadConnecticutEcrisArtifact({
    ...artifact,
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  await rename(tmpPath, paths.filePath);

  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid Connecticut eCRIS artifact refresh timestamp");
  }
  const current: ConnecticutEcrisArtifactCacheMetadata = {
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
