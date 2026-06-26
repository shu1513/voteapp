import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { access, mkdir, open, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { delimiter, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { promisify } from "node:util";

export const PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_BASE_URL =
  "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data";
export const PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS = 900_000;
export const DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR =
  "scratch/pennsylvania-campaign-finance/exports";
const PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_ALLOWED_HOSTS = new Set(["pa.gov", "www.pa.gov"]);
const PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_LOCK_WAIT_MS = 60_000;
const PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_LOCK_STALE_MS =
  PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS * 2;

export type PennsylvaniaCampaignFinanceExportRemoteMetadata = {
  year: number;
  url: string;
  contentLength: number | null;
  contentType: string | null;
  etag: string | null;
  lastModified: string | null;
};

export type PennsylvaniaCampaignFinanceExportDownloadResult =
  PennsylvaniaCampaignFinanceExportRemoteMetadata & {
    outputPath: string;
    bytesWritten: number;
  };

export type PennsylvaniaCampaignFinanceExportCacheMetadata = {
  version: 1;
  year: number;
  archivePath: string;
  extractedDir?: string;
  metadataPath: string;
  downloadedAt: string;
  remote: PennsylvaniaCampaignFinanceExportRemoteMetadata;
  bytesWritten: number;
};

export type PennsylvaniaCampaignFinanceExportRefreshResult = {
  status: "downloaded" | "extracted" | "unchanged";
  cacheDir: string;
  archivePath: string;
  extractedDir: string;
  metadataPath: string;
  remote: PennsylvaniaCampaignFinanceExportRemoteMetadata;
  previous: PennsylvaniaCampaignFinanceExportCacheMetadata | null;
  current: PennsylvaniaCampaignFinanceExportCacheMetadata;
};

export type PennsylvaniaCampaignFinanceExportExtractInput = {
  archivePath: string;
  extractedDir: string;
  year: number;
};

export type PennsylvaniaCampaignFinanceExportExtractor = (
  input: PennsylvaniaCampaignFinanceExportExtractInput
) => Promise<void>;

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

const execFileAsync = promisify(execFile);
const refreshLocks = new Map<string, Promise<void>>();

async function withRefreshLock<T>(key: string, task: () => Promise<T>): Promise<T> {
  const previous = refreshLocks.get(key) ?? Promise.resolve();
  let releaseCurrentLock!: () => void;
  const current = new Promise<void>((resolveLock) => {
    releaseCurrentLock = resolveLock;
  });
  const chained = previous.catch(() => {}).then(() => current);
  refreshLocks.set(key, chained);

  await previous.catch(() => {});
  try {
    return await task();
  } finally {
    releaseCurrentLock();
    if (refreshLocks.get(key) === chained) {
      refreshLocks.delete(key);
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => {
    setTimeout(resolveDelay, ms);
  });
}

async function withRefreshFileLock<T>(input: {
  cacheDir: string;
  lockPath: string;
  task: () => Promise<T>;
}): Promise<T> {
  await mkdir(input.cacheDir, { recursive: true });
  const startedAt = Date.now();
  while (true) {
    let lockHandle: Awaited<ReturnType<typeof open>> | undefined;
    try {
      lockHandle = await open(input.lockPath, "wx");
      await lockHandle.writeFile(`${process.pid}\n${new Date().toISOString()}\n`, "utf8");
      try {
        return await input.task();
      } finally {
        await lockHandle.close();
        await rm(input.lockPath, { force: true }).catch(() => {});
      }
    } catch (error) {
      if (lockHandle) {
        await lockHandle.close().catch(() => {});
      }
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
        throw error;
      }

      const lockStat = await stat(input.lockPath).catch(() => null);
      if (lockStat && Date.now() - lockStat.mtimeMs > PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_LOCK_STALE_MS) {
        await rm(input.lockPath, { force: true }).catch(() => {});
        continue;
      }
      if (Date.now() - startedAt > PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_LOCK_WAIT_MS) {
        throw new Error(`Pennsylvania campaign finance export refresh lock is busy: ${input.lockPath}`);
      }
      await delay(250);
    }
  }
}

export function normalizePennsylvaniaCampaignFinanceExportYear(year: number): number {
  if (!Number.isInteger(year) || year < 2000 || year > 2100) {
    throw new Error(`Invalid Pennsylvania campaign finance export year: ${year}`);
  }
  return year;
}

export function parsePennsylvaniaCampaignFinanceHttpsUrl(
  value: string,
  fieldName = "Pennsylvania campaign finance URL"
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
  const hostname = parsed.hostname.toLowerCase().replace(/\.$/, "");
  if (!PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_ALLOWED_HOSTS.has(hostname)) {
    throw new Error(`Invalid ${fieldName} host: ${parsed.hostname}. Only official Pennsylvania hosts are allowed.`);
  }
  return parsed.toString();
}

export function buildPennsylvaniaCampaignFinanceExportUrl(input: {
  year: number;
  baseUrl?: string;
}): string {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(input.year);
  const baseUrl = parsePennsylvaniaCampaignFinanceHttpsUrl(
    input.baseUrl ?? PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_BASE_URL,
    "Pennsylvania campaign finance export base URL"
  ).replace(/\/$/, "");
  return `${baseUrl}/${year}.zip`;
}

function archiveFileName(year: number): string {
  return `${normalizePennsylvaniaCampaignFinanceExportYear(year)}.zip`;
}

function extractedDirName(year: number): string {
  return `${normalizePennsylvaniaCampaignFinanceExportYear(year)}`;
}

function metadataFileName(year: number): string {
  return `${normalizePennsylvaniaCampaignFinanceExportYear(year)}.metadata.json`;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS;
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
      throw new Error(`Pennsylvania campaign finance export request timed out after ${timeoutMs}ms for ${url}`);
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
): PennsylvaniaCampaignFinanceExportRemoteMetadata {
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

export async function fetchPennsylvaniaCampaignFinanceExportMetadata(input: {
  year: number;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<PennsylvaniaCampaignFinanceExportRemoteMetadata> {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(input.year);
  const normalizedUrl = parsePennsylvaniaCampaignFinanceHttpsUrl(
    input.url ?? buildPennsylvaniaCampaignFinanceExportUrl({ year }),
    "--url"
  );
  const response = await fetchWithTimeout(normalizedUrl, { method: "HEAD" }, input);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch Pennsylvania campaign finance export metadata: ${response.status} ${response.statusText}`
    );
  }
  return metadataFromResponse(year, normalizedUrl, response);
}

export async function downloadPennsylvaniaCampaignFinanceExport(input: {
  year: number;
  outputPath: string;
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<PennsylvaniaCampaignFinanceExportDownloadResult> {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(input.year);
  const normalizedUrl = parsePennsylvaniaCampaignFinanceHttpsUrl(
    input.url ?? buildPennsylvaniaCampaignFinanceExportUrl({ year }),
    "--url"
  );
  const outputPath = resolve(input.outputPath);
  const response = await fetchWithTimeout(normalizedUrl, { method: "GET" }, input);
  if (!response.ok) {
    throw new Error(`Failed to download Pennsylvania campaign finance export: ${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("Pennsylvania campaign finance export response did not include a body");
  }

  const timeoutMs = input.timeoutMs ?? PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS;
  let timeout: NodeJS.Timeout | undefined;
  let outputStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    timeout = setTimeout(() => {
      source.destroy(
        new Error(`Pennsylvania campaign finance export download timed out after ${timeoutMs}ms for ${normalizedUrl}`)
      );
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

  const metadata = metadataFromResponse(year, normalizedUrl, response);
  if (metadata.contentLength !== null && outputStat.size !== metadata.contentLength) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw new Error(
      `Pennsylvania campaign finance export download size mismatch: expected ${metadata.contentLength} bytes, received ${outputStat.size} bytes`
    );
  }

  return {
    ...metadata,
    outputPath,
    bytesWritten: outputStat.size,
  };
}

export function getPennsylvaniaCampaignFinanceExportCachePaths(input: {
  cacheDir: string;
  year: number;
}): {
  cacheDir: string;
  archivePath: string;
  extractedDir: string;
  metadataPath: string;
} {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(input.year);
  const cacheDir = resolve(input.cacheDir);
  return {
    cacheDir,
    archivePath: resolve(cacheDir, archiveFileName(year)),
    extractedDir: resolve(cacheDir, extractedDirName(year)),
    metadataPath: resolve(cacheDir, metadataFileName(year)),
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

export async function readPennsylvaniaCampaignFinanceExportCacheMetadata(
  metadataPath: string
): Promise<PennsylvaniaCampaignFinanceExportCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<PennsylvaniaCampaignFinanceExportCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.year !== "number" ||
      typeof parsed.archivePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string"
    ) {
      return null;
    }
    return parsed as PennsylvaniaCampaignFinanceExportCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Pennsylvania campaign finance export cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: PennsylvaniaCampaignFinanceExportCacheMetadata | null,
  remote: PennsylvaniaCampaignFinanceExportRemoteMetadata
): boolean {
  if (!previous || previous.remote.url !== remote.url || previous.year !== remote.year) {
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
    throw new Error("Invalid Pennsylvania campaign finance export refresh timestamp");
  }
  return normalized;
}

function pathWithDefaultTools(): string {
  const existingPath = process.env.PATH?.trim();
  const defaultSegments = ["/usr/bin", "/bin", "/usr/sbin", "/sbin", "/opt/homebrew/bin", "/usr/local/bin"];
  return [existingPath, ...defaultSegments].filter(Boolean).join(delimiter);
}

export async function defaultPennsylvaniaCampaignFinanceExportExtractor(
  input: PennsylvaniaCampaignFinanceExportExtractInput
): Promise<void> {
  await mkdir(input.extractedDir, { recursive: true });
  await execFileAsync("unzip", ["-q", "-o", input.archivePath, "-d", input.extractedDir], {
    env: {
      ...process.env,
      PATH: pathWithDefaultTools(),
    },
    maxBuffer: 1024 * 1024,
  });
}

async function extractArchiveToFinalDir(input: {
  archivePath: string;
  extractedDir: string;
  year: number;
  extractArchive: PennsylvaniaCampaignFinanceExportExtractor;
}): Promise<void> {
  const tmpExtractedDir = `${input.extractedDir}.tmp-${process.pid}-${randomUUID()}`;
  await rm(tmpExtractedDir, { recursive: true, force: true }).catch(() => {});
  try {
    await input.extractArchive({
      archivePath: input.archivePath,
      extractedDir: tmpExtractedDir,
      year: input.year,
    });
    await access(tmpExtractedDir);
    await rm(input.extractedDir, { recursive: true, force: true });
    await rename(tmpExtractedDir, input.extractedDir);
  } catch (error) {
    await rm(tmpExtractedDir, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
}

async function refreshPennsylvaniaCampaignFinanceExportCacheUnlocked(input: {
  year: number;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  extractArchive?: PennsylvaniaCampaignFinanceExportExtractor;
  timeoutMs?: number;
  now?: Date;
}): Promise<PennsylvaniaCampaignFinanceExportRefreshResult> {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(input.year);
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getPennsylvaniaCampaignFinanceExportCachePaths({ cacheDir: input.cacheDir, year });
  const remote = await fetchPennsylvaniaCampaignFinanceExportMetadata({
    year,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readPennsylvaniaCampaignFinanceExportCacheMetadata(paths.metadataPath);
  const archiveExists = await pathExists(paths.archivePath);
  const extractedDirExists = await pathExists(paths.extractedDir);
  const extractArchive = input.extractArchive ?? defaultPennsylvaniaCampaignFinanceExportExtractor;

  if (!input.force && archiveExists && remoteMetadataMatches(previous, remote)) {
    if (extractedDirExists && previous) {
      return {
        status: "unchanged",
        ...paths,
        remote,
        previous,
        current: previous,
      };
    }

    await extractArchiveToFinalDir({
      archivePath: paths.archivePath,
      extractedDir: paths.extractedDir,
      year,
      extractArchive,
    });
    const current: PennsylvaniaCampaignFinanceExportCacheMetadata = {
      ...(previous ?? {
        version: 1,
        year,
        archivePath: paths.archivePath,
        metadataPath: paths.metadataPath,
        downloadedAt: downloadedAt.toISOString(),
        remote,
        bytesWritten: (await stat(paths.archivePath)).size,
      }),
      extractedDir: paths.extractedDir,
    };
    await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");
    return {
      status: "extracted",
      ...paths,
      remote,
      previous,
      current,
    };
  }

  await mkdir(paths.cacheDir, { recursive: true });
  const tmpPath = `${paths.archivePath}.tmp-${process.pid}-${randomUUID()}`;
  const downloaded = await downloadPennsylvaniaCampaignFinanceExport({
    year,
    url: remote.url,
    outputPath: tmpPath,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });

  try {
    await extractArchiveToFinalDir({
      archivePath: tmpPath,
      extractedDir: paths.extractedDir,
      year,
      extractArchive,
    });
    await rename(tmpPath, paths.archivePath);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }

  const current: PennsylvaniaCampaignFinanceExportCacheMetadata = {
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
  await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");

  return {
    status: "downloaded",
    ...paths,
    remote,
    previous,
    current,
  };
}

export async function refreshPennsylvaniaCampaignFinanceExportCache(input: {
  year: number;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  extractArchive?: PennsylvaniaCampaignFinanceExportExtractor;
  timeoutMs?: number;
  now?: Date;
}): Promise<PennsylvaniaCampaignFinanceExportRefreshResult> {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(input.year);
  const paths = getPennsylvaniaCampaignFinanceExportCachePaths({ cacheDir: input.cacheDir, year });
  return await withRefreshLock(`${paths.cacheDir}\u0000${year}`, () =>
    withRefreshFileLock({
      cacheDir: paths.cacheDir,
      lockPath: `${paths.metadataPath}.lock`,
      task: () => refreshPennsylvaniaCampaignFinanceExportCacheUnlocked(input),
    })
  );
}
