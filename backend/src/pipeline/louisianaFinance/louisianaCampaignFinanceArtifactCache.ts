import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

export const LOUISIANA_CAMPAIGN_FINANCE_SOURCE = "LOUISIANA_ETHICS" as const;
export const LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL =
  "https://www.ethics.la.gov/campaignfinancesearch/ShowPremadereports.aspx";
export const LOUISIANA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS = 900_000;
export const DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR = "scratch/louisiana-campaign-finance";
export const DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_START_YEAR = 2024;
export const DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_END_YEAR = 2027;

export const LOUISIANA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME = "Contributions_2024_to_2027.csv";
export const LOUISIANA_CAMPAIGN_FINANCE_EXPENDITURES_CSV_FILE_NAME = "Expenditures_2024_to_2027.csv";

export type LouisianaCampaignFinanceDownloadKey = "contributions" | "expenditures";

export type LouisianaCampaignFinanceYearRange = {
  startYear?: number;
  endYear?: number;
};

export type NormalizedLouisianaCampaignFinanceYearRange = {
  startYear: number;
  endYear: number;
};

export type LouisianaCampaignFinanceRemoteDownloadMetadata = {
  key: LouisianaCampaignFinanceDownloadKey;
  label: string;
  sourcePageUrl: string;
  url: string;
  filename: string;
  contentLength: number | null;
  contentType: string | null;
  contentDisposition: string | null;
  lastModified: string | null;
};

export type LouisianaCampaignFinanceDownloadResult = LouisianaCampaignFinanceRemoteDownloadMetadata & {
  outputPath: string;
  bytesWritten: number;
  sha256: string;
};

export type LouisianaCampaignFinanceArtifactCacheDownloadMetadata = {
  outputPath: string;
  bytesWritten: number;
  sha256: string;
  remote: LouisianaCampaignFinanceRemoteDownloadMetadata;
};

export type LouisianaCampaignFinanceArtifactCacheMetadata = {
  version: 1;
  cacheDir: string;
  metadataPath: string;
  downloadedAt: string;
  sourcePageUrl: string;
  yearRange: NormalizedLouisianaCampaignFinanceYearRange;
  downloads: Record<LouisianaCampaignFinanceDownloadKey, LouisianaCampaignFinanceArtifactCacheDownloadMetadata>;
};

export type LouisianaCampaignFinanceArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  metadataPath: string;
  previous: LouisianaCampaignFinanceArtifactCacheMetadata | null;
  current: LouisianaCampaignFinanceArtifactCacheMetadata;
};

export type LouisianaCampaignFinanceDownloadDescriptor = {
  key: LouisianaCampaignFinanceDownloadKey;
  label: string;
};

export type LouisianaCampaignFinanceArtifactCachePaths = {
  cacheDir: string;
  metadataPath: string;
  downloads: Record<LouisianaCampaignFinanceDownloadKey, string>;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

const DOWNLOAD_DESCRIPTORS: readonly LouisianaCampaignFinanceDownloadDescriptor[] = [
  {
    key: "contributions",
    label: "E-filed Contributions",
  },
  {
    key: "expenditures",
    label: "E-filed Expenditures",
  },
];

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function normalizeHttpsUrl(value: string, fieldName = "Louisiana campaign finance URL"): string {
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

function normalizeYear(value: number, fieldName: string): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Louisiana campaign finance ${fieldName}: ${value}`);
  }
  return value;
}

export function normalizeLouisianaCampaignFinanceYearRange(
  input: LouisianaCampaignFinanceYearRange = {}
): NormalizedLouisianaCampaignFinanceYearRange {
  const startYear = normalizeYear(input.startYear ?? DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_START_YEAR, "startYear");
  const endYear = normalizeYear(input.endYear ?? DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_END_YEAR, "endYear");
  if (endYear < startYear) {
    throw new Error(`Invalid Louisiana campaign finance year range: ${startYear} to ${endYear}`);
  }
  return { startYear, endYear };
}

export function buildLouisianaCampaignFinanceDownloadFileName(
  key: LouisianaCampaignFinanceDownloadKey,
  range: LouisianaCampaignFinanceYearRange = {}
): string {
  const normalized = normalizeLouisianaCampaignFinanceYearRange(range);
  const prefix = key === "contributions" ? "Contributions" : "Expenditures";
  return `${prefix}_${normalized.startYear}_to_${normalized.endYear}.csv`;
}

export function buildLouisianaCampaignFinanceDownloadUrl(
  key: LouisianaCampaignFinanceDownloadKey,
  range: LouisianaCampaignFinanceYearRange = {}
): string {
  const filename = buildLouisianaCampaignFinanceDownloadFileName(key, range);
  const directory = key === "contributions" ? "ContributionReports" : "ExpenditureReports";
  return normalizeHttpsUrl(
    `https://www.ethics.la.gov/Pub/CampFinan/DataDownload/${directory}/${filename}`,
    `Louisiana campaign finance ${key} download URL`
  );
}

export function getLouisianaCampaignFinanceDownloadDescriptors(): readonly LouisianaCampaignFinanceDownloadDescriptor[] {
  return DOWNLOAD_DESCRIPTORS;
}

export function getLouisianaCampaignFinanceArtifactCachePaths(
  cacheDir: string,
  range: LouisianaCampaignFinanceYearRange = {}
): LouisianaCampaignFinanceArtifactCachePaths {
  const normalizedCacheDir = resolve(cacheDir);
  const normalizedRange = normalizeLouisianaCampaignFinanceYearRange(range);
  const rangeSuffix = `${normalizedRange.startYear}_to_${normalizedRange.endYear}`;
  return {
    cacheDir: normalizedCacheDir,
    metadataPath: resolve(normalizedCacheDir, `louisiana_campaign_finance_${rangeSuffix}.metadata.json`),
    downloads: {
      contributions: resolve(
        normalizedCacheDir,
        buildLouisianaCampaignFinanceDownloadFileName("contributions", normalizedRange)
      ),
      expenditures: resolve(
        normalizedCacheDir,
        buildLouisianaCampaignFinanceDownloadFileName("expenditures", normalizedRange)
      ),
    },
  };
}

function parseBytesCount(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function parseContentDispositionFilename(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const filenameMatch = /filename\*=UTF-8''([^;]+)|filename="([^"]+)"|filename=([^;]+)/i.exec(value);
  const rawFilename = filenameMatch?.[1] ?? filenameMatch?.[2] ?? filenameMatch?.[3];
  if (!rawFilename) {
    return null;
  }

  try {
    return decodeURIComponent(rawFilename.trim());
  } catch {
    return rawFilename.trim();
  }
}

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? LOUISIANA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "application/octet-stream,text/csv;q=0.9,*/*;q=0.1");
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Louisiana campaign finance request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function cancelResponseBody(response: Response): Promise<void> {
  if (!response.body) {
    return;
  }
  try {
    await response.body.cancel();
  } catch {
    // Metadata fallback callers do not consume the response body.
  }
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

function assertExpectedHost(responseUrl: string, requestedUrl: string, key: LouisianaCampaignFinanceDownloadKey): void {
  if (new URL(responseUrl).host !== new URL(requestedUrl).host) {
    throw new Error(
      `Invalid Louisiana campaign finance ${key} response host: ${new URL(responseUrl).host}. Expected ${new URL(requestedUrl).host}.`
    );
  }
}

export async function fetchLouisianaCampaignFinanceDownloadMetadata(
  key: LouisianaCampaignFinanceDownloadKey,
  range: LouisianaCampaignFinanceYearRange = {},
  options: FetchOptions = {}
): Promise<LouisianaCampaignFinanceRemoteDownloadMetadata> {
  const descriptor = DOWNLOAD_DESCRIPTORS.find((item) => item.key === key);
  if (!descriptor) {
    throw new Error(`Unknown Louisiana campaign finance download key: ${key}`);
  }

  const requestedUrl = buildLouisianaCampaignFinanceDownloadUrl(key, range);
  let response = await fetchWithTimeout(requestedUrl, { method: "HEAD" }, options);
  if (response.status === 403 || response.status === 405) {
    response = await fetchWithTimeout(
      requestedUrl,
      {
        method: "GET",
        headers: {
          range: "bytes=0-0",
        },
      },
      options
    );
    await cancelResponseBody(response);
  }
  if (!response.ok) {
    throw new Error(`Failed to fetch Louisiana campaign finance ${key} metadata: ${response.status} ${response.statusText}`);
  }

  const responseUrl = normalizeHttpsUrl(response.url || requestedUrl, `Louisiana campaign finance ${key} response URL`);
  assertExpectedHost(responseUrl, requestedUrl, key);

  const contentDisposition = response.headers.get("content-disposition");
  return {
    key,
    label: descriptor.label,
    sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
    url: responseUrl,
    filename: parseContentDispositionFilename(contentDisposition) ?? buildLouisianaCampaignFinanceDownloadFileName(key, range),
    contentLength: parseBytesCount(response.headers.get("content-length")),
    contentType: response.headers.get("content-type"),
    contentDisposition,
    lastModified: response.headers.get("last-modified"),
  };
}

async function downloadLouisianaCampaignFinanceAttachment(input: {
  metadata: LouisianaCampaignFinanceRemoteDownloadMetadata;
  outputPath: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<LouisianaCampaignFinanceDownloadResult> {
  const requestedUrl = normalizeHttpsUrl(input.metadata.url, `Louisiana campaign finance ${input.metadata.key} download URL`);
  const response = await fetchWithTimeout(
    requestedUrl,
    {
      method: "GET",
      headers: {
        accept: "application/octet-stream,text/csv;q=0.9,*/*;q=0.1",
      },
    },
    input
  );

  if (!response.ok) {
    throw new Error(
      `Failed to download Louisiana campaign finance ${input.metadata.key}: ${response.status} ${response.statusText}`
    );
  }
  if (!response.body) {
    throw new Error(`Louisiana campaign finance ${input.metadata.key} response did not include a body`);
  }

  const responseUrl = normalizeHttpsUrl(
    response.url || requestedUrl,
    `Louisiana campaign finance ${input.metadata.key} response URL`
  );
  assertExpectedHost(responseUrl, requestedUrl, input.metadata.key);

  const outputPath = resolve(input.outputPath);
  await mkdir(resolve(outputPath, ".."), { recursive: true });
  const tmpPath = `${outputPath}.tmp-${process.pid}-${Date.now()}`;
  const hash = createHash("sha256");
  const hashSink = new Transform({
    transform(chunk, _encoding, callback) {
      hash.update(chunk);
      callback(null, chunk);
    },
  });

  let fileStat;
  try {
    const source = Readable.fromWeb(response.body as NodeReadableStream<Uint8Array>);
    await pipeline(source, hashSink, createWriteStream(tmpPath));
    fileStat = await stat(tmpPath);
    await rename(tmpPath, outputPath);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }

  const sha256 = hash.digest("hex");
  const contentLength = parseBytesCount(response.headers.get("content-length")) ?? input.metadata.contentLength;
  if (contentLength !== null && fileStat.size !== contentLength) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw new Error(
      `Louisiana campaign finance ${input.metadata.key} download size mismatch: expected ${contentLength} bytes, received ${fileStat.size} bytes`
    );
  }

  const contentDisposition = response.headers.get("content-disposition") ?? input.metadata.contentDisposition;
  return {
    ...input.metadata,
    url: responseUrl,
    filename: parseContentDispositionFilename(contentDisposition) ?? input.metadata.filename,
    contentLength,
    contentType: response.headers.get("content-type") ?? input.metadata.contentType,
    contentDisposition,
    lastModified: response.headers.get("last-modified") ?? input.metadata.lastModified,
    outputPath,
    bytesWritten: fileStat.size,
    sha256,
  };
}

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Louisiana campaign finance artifact refresh timestamp");
  }
  return normalized;
}

export async function readLouisianaCampaignFinanceArtifactCacheMetadata(
  metadataPath: string
): Promise<LouisianaCampaignFinanceArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<LouisianaCampaignFinanceArtifactCacheMetadata>;
    const downloads = parsed.downloads as Partial<
      Record<LouisianaCampaignFinanceDownloadKey, LouisianaCampaignFinanceArtifactCacheDownloadMetadata>
    > | undefined;
    if (
      parsed.version !== 1 ||
      typeof parsed.cacheDir !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.sourcePageUrl !== "string" ||
      typeof parsed.yearRange?.startYear !== "number" ||
      typeof parsed.yearRange?.endYear !== "number" ||
      !downloads ||
      typeof downloads.contributions?.outputPath !== "string" ||
      typeof downloads.contributions?.bytesWritten !== "number" ||
      typeof downloads.contributions?.sha256 !== "string" ||
      typeof downloads.contributions?.remote?.url !== "string" ||
      typeof downloads.expenditures?.outputPath !== "string" ||
      typeof downloads.expenditures?.bytesWritten !== "number" ||
      typeof downloads.expenditures?.sha256 !== "string" ||
      typeof downloads.expenditures?.remote?.url !== "string"
    ) {
      return null;
    }
    return parsed as LouisianaCampaignFinanceArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Louisiana campaign finance cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: LouisianaCampaignFinanceArtifactCacheMetadata | null,
  key: LouisianaCampaignFinanceDownloadKey,
  nextRemote: LouisianaCampaignFinanceRemoteDownloadMetadata
): boolean {
  const prior = previous?.downloads[key]?.remote;
  return (
    !!prior &&
    prior.url === nextRemote.url &&
    prior.contentLength === nextRemote.contentLength &&
    prior.lastModified === nextRemote.lastModified
  );
}

async function metadataFilesExist(metadata: LouisianaCampaignFinanceArtifactCacheMetadata): Promise<boolean> {
  for (const key of Object.keys(metadata.downloads) as LouisianaCampaignFinanceDownloadKey[]) {
    if (!(await pathExists(metadata.downloads[key].outputPath))) {
      return false;
    }
  }
  return true;
}

function toDownloadMetadata(
  result: LouisianaCampaignFinanceDownloadResult
): LouisianaCampaignFinanceArtifactCacheDownloadMetadata {
  const { outputPath, bytesWritten, sha256, ...remote } = result;
  return { outputPath, bytesWritten, sha256, remote };
}

export async function refreshLouisianaCampaignFinanceArtifactCache(input: {
  cacheDir: string;
  range?: LouisianaCampaignFinanceYearRange;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<LouisianaCampaignFinanceArtifactRefreshResult> {
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const yearRange = normalizeLouisianaCampaignFinanceYearRange(input.range);
  const paths = getLouisianaCampaignFinanceArtifactCachePaths(input.cacheDir, yearRange);
  await mkdir(paths.cacheDir, { recursive: true });

  const previous = await readLouisianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  const remoteMetadata: Partial<Record<LouisianaCampaignFinanceDownloadKey, LouisianaCampaignFinanceRemoteDownloadMetadata>> = {};
  for (const descriptor of DOWNLOAD_DESCRIPTORS) {
    remoteMetadata[descriptor.key] = await fetchLouisianaCampaignFinanceDownloadMetadata(descriptor.key, yearRange, {
      fetchImpl: input.fetchImpl,
      timeoutMs: input.timeoutMs,
    });
  }

  const canReusePrevious =
    !input.force &&
    !!previous &&
    previous.sourcePageUrl === LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL &&
    previous.yearRange.startYear === yearRange.startYear &&
    previous.yearRange.endYear === yearRange.endYear &&
    remoteMetadataMatches(previous, "contributions", remoteMetadata.contributions!) &&
    remoteMetadataMatches(previous, "expenditures", remoteMetadata.expenditures!) &&
    (await metadataFilesExist(previous));

  if (canReusePrevious) {
    return {
      status: "unchanged",
      cacheDir: paths.cacheDir,
      metadataPath: paths.metadataPath,
      previous,
      current: previous,
    };
  }

  const downloaded: Partial<Record<LouisianaCampaignFinanceDownloadKey, LouisianaCampaignFinanceDownloadResult>> = {};
  for (const descriptor of DOWNLOAD_DESCRIPTORS) {
    downloaded[descriptor.key] = await downloadLouisianaCampaignFinanceAttachment({
      metadata: remoteMetadata[descriptor.key]!,
      outputPath: paths.downloads[descriptor.key],
      fetchImpl: input.fetchImpl,
      timeoutMs: input.timeoutMs,
    });
  }

  const current: LouisianaCampaignFinanceArtifactCacheMetadata = {
    version: 1,
    cacheDir: paths.cacheDir,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    sourcePageUrl: LOUISIANA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL,
    yearRange,
    downloads: {
      contributions: toDownloadMetadata(downloaded.contributions!),
      expenditures: toDownloadMetadata(downloaded.expenditures!),
    },
  };

  await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");
  return {
    status: "downloaded",
    cacheDir: paths.cacheDir,
    metadataPath: paths.metadataPath,
    previous,
    current,
  };
}
