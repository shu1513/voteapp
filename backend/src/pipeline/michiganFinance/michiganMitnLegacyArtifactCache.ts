import { execFile } from "node:child_process";
import { createWriteStream } from "node:fs";
import { access, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { delimiter, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { promisify } from "node:util";

export const MICHIGAN_MITN_LEGACY_ARCHIVE_BASE_URL =
  "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data";
export const MICHIGAN_MITN_LEGACY_ARCHIVE_INDEX_URL =
  "https://www.michigan.gov/sos/elections/disclosure/cfr/committee-search/intro/welcome-to-the-michigan-campaign-finance-searchable-database";
export const MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS = 900_000;
export const DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR =
  "scratch/michigan-campaign-finance/mitn";

export type MichiganMitnLegacyArchiveRemoteMetadata = {
  year: number;
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type MichiganMitnLegacyArchiveDownloadResult = MichiganMitnLegacyArchiveRemoteMetadata & {
  outputPath: string;
  bytesWritten: number;
};

export type MichiganMitnLegacyArchiveCacheMetadata = {
  version: 1;
  year: number;
  archivePath: string;
  extractedDir?: string;
  metadataPath: string;
  downloadedAt: string;
  remote: MichiganMitnLegacyArchiveRemoteMetadata;
  bytesWritten: number;
};

export type MichiganMitnLegacyArchiveRefreshResult = {
  status: "downloaded" | "extracted" | "unchanged";
  cacheDir: string;
  archivePath: string;
  extractedDir: string;
  metadataPath: string;
  remote: MichiganMitnLegacyArchiveRemoteMetadata;
  previous: MichiganMitnLegacyArchiveCacheMetadata | null;
  current: MichiganMitnLegacyArchiveCacheMetadata;
};

export type MichiganMitnLegacyArchiveExtractInput = {
  archivePath: string;
  extractedDir: string;
  year: number;
};

export type MichiganMitnLegacyArchiveExtractor = (
  input: MichiganMitnLegacyArchiveExtractInput
) => Promise<void>;

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

const execFileAsync = promisify(execFile);

export function normalizeMichiganMitnLegacyArchiveYear(year: number): number {
  if (!Number.isInteger(year) || year < 2020 || year > 2100) {
    throw new Error(`Invalid Michigan MiTN legacy archive year: ${year}`);
  }
  return year;
}

export function parseMichiganMitnHttpsUrl(value: string, fieldName = "Michigan MiTN URL"): string {
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

export function buildMichiganMitnLegacyArchiveUrl(input: {
  year: number;
  baseUrl?: string;
}): string {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  const baseUrl = parseMichiganMitnHttpsUrl(
    input.baseUrl ?? MICHIGAN_MITN_LEGACY_ARCHIVE_BASE_URL,
    "Michigan MiTN legacy archive base URL"
  ).replace(/\/$/, "");
  return `${baseUrl}/${year}_mi_cfr.7z`;
}

function archiveFileName(year: number): string {
  return `${normalizeMichiganMitnLegacyArchiveYear(year)}_mi_cfr.7z`;
}

function metadataFileName(year: number): string {
  return `${normalizeMichiganMitnLegacyArchiveYear(year)}_mi_cfr.metadata.json`;
}

function extractedDirName(year: number): string {
  return `${normalizeMichiganMitnLegacyArchiveYear(year)}_mi_cfr`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "application/x-7z-compressed,application/octet-stream;q=0.9,*/*;q=0.1");
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Michigan MiTN legacy archive request timed out after ${timeoutMs}ms for ${url}`);
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
): MichiganMitnLegacyArchiveRemoteMetadata {
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

function validatedFinalResponseUrl(input: { requestedUrl: string; response: Response; fieldName: string }): string {
  const requested = new URL(parseMichiganMitnHttpsUrl(input.requestedUrl, input.fieldName));
  const finalUrl = parseMichiganMitnHttpsUrl(input.response.url || input.requestedUrl, input.fieldName);
  const final = new URL(finalUrl);
  if (final.host !== requested.host) {
    throw new Error(`Invalid ${input.fieldName} host: ${final.host}. Expected ${requested.host}.`);
  }
  return finalUrl;
}

async function discoverMichiganMitnLegacyArchiveUrl(input: {
  year: number;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<string | null> {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  const response = await fetchWithTimeout(
    MICHIGAN_MITN_LEGACY_ARCHIVE_INDEX_URL,
    {
      method: "GET",
      headers: {
        accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
      },
    },
    input
  );
  if (!response.ok) {
    return null;
  }
  const html = await response.text();
  const pattern = new RegExp(
    `(?:https://www\\.michigan\\.gov)?(/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/${year}_mi_cfr\\.7z\\?[^"'<>\\s]+)`,
    "i"
  );
  const match = pattern.exec(html);
  if (!match) {
    return null;
  }
  return parseMichiganMitnHttpsUrl(`https://www.michigan.gov${match[1].replace(/&amp;/g, "&")}`);
}

export async function fetchMichiganMitnLegacyArchiveMetadata(input: {
  year: number;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MichiganMitnLegacyArchiveRemoteMetadata> {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  const normalizedUrl = parseMichiganMitnHttpsUrl(
    input.url ?? buildMichiganMitnLegacyArchiveUrl({ year }),
    "--url"
  );
  let response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  let metadataUrl = normalizedUrl;
  if (!response.ok && normalizedUrl === buildMichiganMitnLegacyArchiveUrl({ year })) {
    const discoveredUrl = await discoverMichiganMitnLegacyArchiveUrl({
      year,
      fetchImpl: input.fetchImpl,
      timeoutMs: input.timeoutMs,
    });
    if (discoveredUrl) {
      metadataUrl = discoveredUrl;
      response = await fetchWithTimeout(discoveredUrl, { method: "HEAD" }, input);
    }
  }
  if (!response.ok) {
    throw new Error(`Failed to fetch Michigan MiTN legacy archive metadata: ${response.status} ${response.statusText}`);
  }
  metadataUrl = validatedFinalResponseUrl({
    requestedUrl: metadataUrl,
    response,
    fieldName: "Michigan MiTN legacy archive metadata response URL",
  });
  return metadataFromResponse(year, metadataUrl, response);
}

export async function downloadMichiganMitnLegacyArchive(input: {
  year: number;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MichiganMitnLegacyArchiveDownloadResult> {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  const normalizedUrl = parseMichiganMitnHttpsUrl(
    input.url ?? buildMichiganMitnLegacyArchiveUrl({ year }),
    "--url"
  );
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Michigan MiTN legacy archive: ${response.status} ${response.statusText}`);
  }
  const finalUrl = validatedFinalResponseUrl({
    requestedUrl: normalizedUrl,
    response,
    fieldName: "Michigan MiTN legacy archive download response URL",
  });
  if (!response.body) {
    throw new Error("Michigan MiTN legacy archive response did not include a body");
  }

  const timeoutMs = input.timeoutMs ?? MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS;
  let timeout: NodeJS.Timeout | undefined;
  let outputStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    timeout = setTimeout(() => {
      source.destroy(new Error(`Michigan MiTN legacy archive download timed out after ${timeoutMs}ms for ${normalizedUrl}`));
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

  const metadata = metadataFromResponse(year, finalUrl, response);
  if (metadata.contentLength !== null && outputStat.size !== metadata.contentLength) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw new Error(
      `Michigan MiTN legacy archive download size mismatch: expected ${metadata.contentLength} bytes, received ${outputStat.size} bytes`
    );
  }

  return {
    ...metadata,
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getMichiganMitnLegacyArchiveCachePaths(input: {
  cacheDir: string;
  year: number;
}): {
  cacheDir: string;
  archivePath: string;
  extractedDir: string;
  metadataPath: string;
} {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  const normalizedCacheDir = resolve(input.cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    archivePath: resolve(normalizedCacheDir, archiveFileName(year)),
    extractedDir: resolve(normalizedCacheDir, extractedDirName(year)),
    metadataPath: resolve(normalizedCacheDir, metadataFileName(year)),
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

function pathCandidates(commandName: string): string[] {
  if (commandName.includes("/")) {
    return [commandName];
  }
  return (process.env.PATH ?? "")
    .split(delimiter)
    .filter(Boolean)
    .map((dir) => resolve(dir, commandName));
}

async function findExecutable(commandName: string): Promise<string | null> {
  for (const candidate of pathCandidates(commandName)) {
    try {
      await access(candidate);
      return candidate;
    } catch {
      // Keep looking.
    }
  }
  return null;
}

async function resolveMichiganMitnExtractorCommand(): Promise<string> {
  const configured = process.env.MICHIGAN_MITN_ARCHIVE_EXTRACT_COMMAND?.trim();
  const candidates = configured ? [configured] : ["7zz", "7z", "7za", "bsdtar"];
  for (const candidate of candidates) {
    const command = await findExecutable(candidate);
    if (command) {
      return command;
    }
  }
  throw new Error(
    "Michigan MiTN legacy archive extraction requires 7zz, 7z, 7za, or bsdtar. Set MICHIGAN_MITN_ARCHIVE_EXTRACT_COMMAND to an installed extractor."
  );
}

function buildExtractorArgs(command: string, input: MichiganMitnLegacyArchiveExtractInput): string[] {
  const commandName = command.split("/").pop()?.toLowerCase() ?? command.toLowerCase();
  if (commandName.includes("bsdtar")) {
    return ["-xf", input.archivePath, "-C", input.extractedDir];
  }
  return ["x", "-y", `-o${input.extractedDir}`, input.archivePath];
}

export async function extractMichiganMitnLegacyArchive(
  input: MichiganMitnLegacyArchiveExtractInput
): Promise<void> {
  const archivePath = resolve(input.archivePath);
  const extractedDir = resolve(input.extractedDir);
  const command = await resolveMichiganMitnExtractorCommand();
  const tmpDir = `${extractedDir}.tmp-${process.pid}-${Date.now()}`;
  await rm(tmpDir, { recursive: true, force: true });
  await mkdir(tmpDir, { recursive: true });
  try {
    await execFileAsync(command, buildExtractorArgs(command, { archivePath, extractedDir: tmpDir, year: input.year }), {
      maxBuffer: 1024 * 1024 * 10,
    });
    await rm(extractedDir, { recursive: true, force: true });
    await rename(tmpDir, extractedDir);
  } catch (error) {
    await rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to extract Michigan MiTN legacy archive ${archivePath}: ${message}`);
  }
}

async function ensureMichiganMitnLegacyArchiveExtracted(input: {
  archivePath: string;
  extractedDir: string;
  year: number;
  extractArchive?: MichiganMitnLegacyArchiveExtractor;
}): Promise<void> {
  const extractor = input.extractArchive ?? extractMichiganMitnLegacyArchive;
  await extractor({
    archivePath: input.archivePath,
    extractedDir: input.extractedDir,
    year: input.year,
  });
}

export async function readMichiganMitnLegacyArchiveCacheMetadata(
  metadataPath: string
): Promise<MichiganMitnLegacyArchiveCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<MichiganMitnLegacyArchiveCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.year !== "number" ||
      typeof parsed.archivePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string" ||
      typeof parsed.remote?.year !== "number"
    ) {
      return null;
    }
    return parsed as MichiganMitnLegacyArchiveCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Michigan MiTN legacy archive cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: MichiganMitnLegacyArchiveCacheMetadata | null,
  remote: MichiganMitnLegacyArchiveRemoteMetadata
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

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Michigan MiTN legacy archive refresh timestamp");
  }
  return normalized;
}

export async function refreshMichiganMitnLegacyArchiveCache(input: {
  year: number;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  extractArchive?: MichiganMitnLegacyArchiveExtractor;
  timeoutMs?: number;
  now?: Date;
}): Promise<MichiganMitnLegacyArchiveRefreshResult> {
  const year = normalizeMichiganMitnLegacyArchiveYear(input.year);
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getMichiganMitnLegacyArchiveCachePaths({ cacheDir: input.cacheDir, year });
  const remote = await fetchMichiganMitnLegacyArchiveMetadata({
    year,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readMichiganMitnLegacyArchiveCacheMetadata(paths.metadataPath);
  const archiveExists = await pathExists(paths.archivePath);
  const extractedExists = await pathExists(paths.extractedDir);

  if (!input.force && archiveExists && extractedExists && remoteMetadataMatches(previous, remote)) {
    return {
      status: "unchanged",
      ...paths,
      remote,
      previous,
      current: previous!,
    };
  }

  if (!input.force && archiveExists && !extractedExists && remoteMetadataMatches(previous, remote)) {
    await ensureMichiganMitnLegacyArchiveExtracted({
      archivePath: paths.archivePath,
      extractedDir: paths.extractedDir,
      year,
      extractArchive: input.extractArchive,
    });
    return {
      status: "extracted",
      ...paths,
      remote,
      previous,
      current: previous!,
    };
  }

  await mkdir(paths.cacheDir, { recursive: true });
  const suffix = `${process.pid}-${Date.now()}`;
  const tmpPath = `${paths.archivePath}.tmp-${suffix}`;
  const tmpExtractedDir = `${paths.extractedDir}.tmp-${suffix}`;
  const tmpMetadataPath = `${paths.metadataPath}.tmp-${suffix}`;
  const archiveBackupPath = `${paths.archivePath}.bak-${suffix}`;
  const extractedBackupDir = `${paths.extractedDir}.bak-${suffix}`;
  let archiveBackedUp = false;
  let extractedBackedUp = false;
  const downloaded = await downloadMichiganMitnLegacyArchive({
    year,
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  await ensureMichiganMitnLegacyArchiveExtracted({
    archivePath: tmpPath,
    extractedDir: tmpExtractedDir,
    year,
    extractArchive: input.extractArchive,
  });

  const current: MichiganMitnLegacyArchiveCacheMetadata = {
    version: 1,
    year,
    archivePath: paths.archivePath,
    extractedDir: paths.extractedDir,
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
  await writeFile(tmpMetadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");

  try {
    if (archiveExists && (await pathExists(paths.archivePath))) {
      await rename(paths.archivePath, archiveBackupPath);
      archiveBackedUp = true;
    }
    if (extractedExists && (await pathExists(paths.extractedDir))) {
      await rename(paths.extractedDir, extractedBackupDir);
      extractedBackedUp = true;
    }
    await rename(tmpPath, paths.archivePath);
    await rename(tmpExtractedDir, paths.extractedDir);
    await rename(tmpMetadataPath, paths.metadataPath);
  } catch (error) {
    await rm(paths.archivePath, { force: true }).catch(() => {});
    await rm(paths.extractedDir, { recursive: true, force: true }).catch(() => {});
    if (archiveBackedUp) {
      await rename(archiveBackupPath, paths.archivePath).catch(() => {});
    }
    if (extractedBackedUp) {
      await rename(extractedBackupDir, paths.extractedDir).catch(() => {});
    }
    throw error;
  } finally {
    await rm(tmpPath, { force: true }).catch(() => {});
    await rm(tmpExtractedDir, { recursive: true, force: true }).catch(() => {});
    await rm(tmpMetadataPath, { force: true }).catch(() => {});
    await rm(archiveBackupPath, { force: true }).catch(() => {});
    await rm(extractedBackupDir, { recursive: true, force: true }).catch(() => {});
  }

  return {
    status: "downloaded",
    ...paths,
    remote,
    previous,
    current,
  };
}
