import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { Transform } from "node:stream";

export const MINNESOTA_CAMPAIGN_FINANCE_SOURCE = "MINNESOTA_CFB" as const;
export const MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL =
  "https://register.cfb.mn.gov/reports-and-data/self-help/data-downloads/campaign-finance/";
export const MINNESOTA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS = 900_000;
export const DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR =
  "scratch/minnesota-campaign-finance";

export const MINNESOTA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME =
  "all_contributions_received.csv";
export const MINNESOTA_CAMPAIGN_FINANCE_INDEPENDENT_EXPENDITURES_CSV_FILE_NAME =
  "all_independent_expenditures.csv";
export const MINNESOTA_CAMPAIGN_FINANCE_IE_CONTRIBUTORS_CSV_FILE_NAME =
  "ie_committee_contributions.csv";

export type MinnesotaCampaignFinanceDownloadKey =
  | "contributions_received"
  | "independent_expenditures"
  | "independent_expenditure_contributions";

export type MinnesotaCampaignFinanceRemoteDownloadMetadata = {
  key: MinnesotaCampaignFinanceDownloadKey;
  label: string;
  sourcePageUrl: string;
  url: string;
  filename: string | null;
  contentLength: number | null;
  contentType: string | null;
  contentDisposition: string | null;
};

export type MinnesotaCampaignFinanceDownloadResult = MinnesotaCampaignFinanceRemoteDownloadMetadata & {
  outputPath: string;
  bytesWritten: number;
  sha256: string;
};

export type MinnesotaCampaignFinanceArtifactCacheDownloadMetadata = {
  outputPath: string;
  bytesWritten: number;
  sha256: string;
  remote: MinnesotaCampaignFinanceRemoteDownloadMetadata;
};

export type MinnesotaCampaignFinanceArtifactCacheMetadata = {
  version: 1;
  cacheDir: string;
  metadataPath: string;
  downloadedAt: string;
  sourcePageUrl: string;
  downloads: Record<MinnesotaCampaignFinanceDownloadKey, MinnesotaCampaignFinanceArtifactCacheDownloadMetadata>;
};

export type MinnesotaCampaignFinanceArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  metadataPath: string;
  previous: MinnesotaCampaignFinanceArtifactCacheMetadata | null;
  current: MinnesotaCampaignFinanceArtifactCacheMetadata;
};

export type MinnesotaCampaignFinanceDownloadDescriptor = {
  key: MinnesotaCampaignFinanceDownloadKey;
  label: string;
};

export type MinnesotaCampaignFinanceArtifactCachePaths = {
  cacheDir: string;
  metadataPath: string;
  downloads: Record<MinnesotaCampaignFinanceDownloadKey, string>;
};

type FetchOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

const MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_DESCRIPTORS: readonly MinnesotaCampaignFinanceDownloadDescriptor[] = [
  {
    key: "contributions_received",
    label: "All Contributions received by all entities - 2015 to present",
  },
  {
    key: "independent_expenditures",
    label: "All Independent expenditures by all entities - 2015 to present",
  },
  {
    key: "independent_expenditure_contributions",
    label: "Independent expenditure committees and funds Contributions received by independent expenditure committees and funds only - 2015 to present",
  },
];

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function normalizeNonEmptyText(value: string): string {
  const normalized = value.trim().replace(/\s+/g, " ");
  if (normalized.length === 0) {
    throw new Error("Minnesota campaign finance label is required");
  }
  return normalized;
}

function normalizeHttpsUrl(value: string, fieldName = "Minnesota campaign finance URL"): string {
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

async function fetchWithTimeout(url: string, init: RequestInit, options: FetchOptions): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? MINNESOTA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "text/html,application/xhtml+xml,application/octet-stream,text/csv;q=0.9,*/*;q=0.1");
  }

  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Minnesota campaign finance request timed out after ${timeoutMs}ms for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
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

function htmlDecode(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripHtmlTags(value: string): string {
  return value.replace(/<[^>]*>/g, " ");
}

function normalizeHtmlText(value: string): string {
  return htmlDecode(stripHtmlTags(value)).replace(/\s+/g, " ").trim().toLowerCase();
}

function findDownloadUrlsByLabel(input: {
  html: string;
  pageUrl: string;
  descriptors: readonly MinnesotaCampaignFinanceDownloadDescriptor[];
}): Record<MinnesotaCampaignFinanceDownloadKey, string> {
  const normalizedDescriptors = input.descriptors.map((descriptor) => ({
    ...descriptor,
    normalizedLabel: normalizeNonEmptyText(descriptor.label).toLowerCase(),
  }));

  const matches = new Map<MinnesotaCampaignFinanceDownloadKey, string>();
  const rowPattern = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  for (const rowHtml of input.html.matchAll(rowPattern)) {
    const row = rowHtml[1] ?? "";
    const rowText = normalizeHtmlText(row);
    const hrefMatch = /<a\b[^>]*href="([^"]+)"/i.exec(row);
    if (!hrefMatch) {
      continue;
    }

    for (const descriptor of normalizedDescriptors) {
      if (!rowText.includes(descriptor.normalizedLabel)) {
        continue;
      }
      const resolvedUrl = normalizeHttpsUrl(new URL(htmlDecode(hrefMatch[1]), input.pageUrl).toString());
      const previous = matches.get(descriptor.key);
      if (previous && previous !== resolvedUrl) {
        throw new Error(
          `Ambiguous Minnesota campaign finance download link for ${descriptor.key}: ${previous} vs ${resolvedUrl}`
        );
      }
      matches.set(descriptor.key, resolvedUrl);
    }
  }

  const resolved: Partial<Record<MinnesotaCampaignFinanceDownloadKey, string>> = {};
  for (const descriptor of normalizedDescriptors) {
    const url = matches.get(descriptor.key);
    if (!url) {
      throw new Error(`Missing Minnesota campaign finance download link for ${descriptor.key}`);
    }
    resolved[descriptor.key] = url;
  }

  return resolved as Record<MinnesotaCampaignFinanceDownloadKey, string>;
}

async function fetchMinnesotaCampaignFinanceDownloadPage(input: FetchOptions): Promise<{ pageUrl: string; html: string }> {
  const pageUrl = normalizeHttpsUrl(MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_PAGE_URL, "Minnesota campaign finance page URL");
  const response = await fetchWithTimeout(
    pageUrl,
    {
      method: "GET",
      headers: {
        accept: "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
      },
    },
    input
  );

  if (!response.ok) {
    throw new Error(`Failed to fetch Minnesota campaign finance download page: ${response.status} ${response.statusText}`);
  }

  const responseUrl = normalizeHttpsUrl(response.url || pageUrl, "Minnesota campaign finance page response URL");
  if (new URL(responseUrl).host !== new URL(pageUrl).host) {
    throw new Error(
      `Invalid Minnesota campaign finance page response host: ${new URL(responseUrl).host}. Expected ${new URL(pageUrl).host}.`
    );
  }

  return {
    pageUrl: responseUrl,
    html: await response.text(),
  };
}

export function getMinnesotaCampaignFinanceArtifactCachePaths(cacheDir: string): MinnesotaCampaignFinanceArtifactCachePaths {
  const normalizedCacheDir = resolve(cacheDir);
  return {
    cacheDir: normalizedCacheDir,
    metadataPath: resolve(normalizedCacheDir, "minnesota_campaign_finance.metadata.json"),
    downloads: {
      contributions_received: resolve(normalizedCacheDir, MINNESOTA_CAMPAIGN_FINANCE_CONTRIBUTIONS_CSV_FILE_NAME),
      independent_expenditures: resolve(normalizedCacheDir, MINNESOTA_CAMPAIGN_FINANCE_INDEPENDENT_EXPENDITURES_CSV_FILE_NAME),
      independent_expenditure_contributions: resolve(
        normalizedCacheDir,
        MINNESOTA_CAMPAIGN_FINANCE_IE_CONTRIBUTORS_CSV_FILE_NAME
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

async function downloadMinnesotaCampaignFinanceAttachment(input: {
  key: MinnesotaCampaignFinanceDownloadKey;
  label: string;
  sourcePageUrl: string;
  url: string;
  outputPath: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): Promise<MinnesotaCampaignFinanceDownloadResult> {
  const requestedUrl = normalizeHttpsUrl(input.url, `Minnesota campaign finance ${input.key} download URL`);
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
      `Failed to download Minnesota campaign finance ${input.key}: ${response.status} ${response.statusText}`
    );
  }
  if (!response.body) {
    throw new Error(`Minnesota campaign finance ${input.key} response did not include a body`);
  }

  const responseUrl = normalizeHttpsUrl(
    response.url || requestedUrl,
    `Minnesota campaign finance ${input.key} response URL`
  );
  if (new URL(responseUrl).host !== new URL(requestedUrl).host) {
    throw new Error(
      `Invalid Minnesota campaign finance ${input.key} response host: ${new URL(responseUrl).host}. Expected ${new URL(requestedUrl).host}.`
    );
  }

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
  const contentDisposition = response.headers.get("content-disposition");
  const contentType = response.headers.get("content-type");
  const contentLength = parseBytesCount(response.headers.get("content-length"));
  if (contentLength !== null && fileStat.size !== contentLength) {
    await rm(outputPath, { force: true }).catch(() => {});
    throw new Error(
      `Minnesota campaign finance ${input.key} download size mismatch: expected ${contentLength} bytes, received ${fileStat.size} bytes`
    );
  }

  return {
    key: input.key,
    label: input.label,
    sourcePageUrl: input.sourcePageUrl,
    url: responseUrl,
    filename: parseContentDispositionFilename(contentDisposition),
    contentLength,
    contentType,
    contentDisposition,
    outputPath,
    bytesWritten: fileStat.size,
    sha256,
  };
}

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Minnesota campaign finance artifact refresh timestamp");
  }
  return normalized;
}

export async function readMinnesotaCampaignFinanceArtifactCacheMetadata(
  metadataPath: string
): Promise<MinnesotaCampaignFinanceArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<MinnesotaCampaignFinanceArtifactCacheMetadata>;
    const downloads = parsed.downloads as Partial<
      Record<MinnesotaCampaignFinanceDownloadKey, MinnesotaCampaignFinanceArtifactCacheDownloadMetadata>
    > | undefined;
    if (
      parsed.version !== 1 ||
      typeof parsed.cacheDir !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.sourcePageUrl !== "string" ||
      !downloads ||
      typeof downloads.contributions_received?.outputPath !== "string" ||
      typeof downloads.contributions_received?.bytesWritten !== "number" ||
      typeof downloads.contributions_received?.sha256 !== "string" ||
      typeof downloads.contributions_received?.remote?.url !== "string" ||
      typeof downloads.independent_expenditures?.outputPath !== "string" ||
      typeof downloads.independent_expenditures?.bytesWritten !== "number" ||
      typeof downloads.independent_expenditures?.sha256 !== "string" ||
      typeof downloads.independent_expenditures?.remote?.url !== "string" ||
      typeof downloads.independent_expenditure_contributions?.outputPath !== "string" ||
      typeof downloads.independent_expenditure_contributions?.bytesWritten !== "number" ||
      typeof downloads.independent_expenditure_contributions?.sha256 !== "string" ||
      typeof downloads.independent_expenditure_contributions?.remote?.url !== "string"
    ) {
      return null;
    }
    return parsed as MinnesotaCampaignFinanceArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Minnesota campaign finance cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function metadataMatches(
  previous: MinnesotaCampaignFinanceArtifactCacheMetadata | null,
  nextDownloads: Record<MinnesotaCampaignFinanceDownloadKey, MinnesotaCampaignFinanceArtifactCacheDownloadMetadata>,
  sourcePageUrl: string
): boolean {
  if (!previous || previous.sourcePageUrl !== sourcePageUrl) {
    return false;
  }

  return (Object.keys(nextDownloads) as MinnesotaCampaignFinanceDownloadKey[]).every((key) => {
    const prior = previous.downloads[key];
    const next = nextDownloads[key];
    return (
      prior &&
      prior.sha256 === next.sha256 &&
      prior.bytesWritten === next.bytesWritten &&
      prior.remote.url === next.remote.url
    );
  });
}

export async function refreshMinnesotaCampaignFinanceArtifactCache(input: {
  cacheDir: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<MinnesotaCampaignFinanceArtifactRefreshResult> {
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getMinnesotaCampaignFinanceArtifactCachePaths(input.cacheDir);
  await mkdir(paths.cacheDir, { recursive: true });

  const previous = await readMinnesotaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  const discovered = await fetchMinnesotaCampaignFinanceDownloadPage({
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const resolvedUrls = findDownloadUrlsByLabel({
    html: discovered.html,
    pageUrl: discovered.pageUrl,
    descriptors: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_DESCRIPTORS,
  });

  const downloaded: Partial<Record<MinnesotaCampaignFinanceDownloadKey, MinnesotaCampaignFinanceDownloadResult>> = {};
  for (const descriptor of MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_DESCRIPTORS) {
    downloaded[descriptor.key] = await downloadMinnesotaCampaignFinanceAttachment({
      key: descriptor.key,
      label: descriptor.label,
      sourcePageUrl: discovered.pageUrl,
      url: resolvedUrls[descriptor.key],
      outputPath: paths.downloads[descriptor.key],
      fetchImpl: input.fetchImpl,
      timeoutMs: input.timeoutMs,
    });
  }

  const current: MinnesotaCampaignFinanceArtifactCacheMetadata = {
    version: 1,
    cacheDir: paths.cacheDir,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    sourcePageUrl: discovered.pageUrl,
    downloads: {
      contributions_received: (({ outputPath, bytesWritten, sha256, ...remote }) => ({
        outputPath,
        bytesWritten,
        sha256,
        remote,
      }))(downloaded.contributions_received!),
      independent_expenditures: (({ outputPath, bytesWritten, sha256, ...remote }) => ({
        outputPath,
        bytesWritten,
        sha256,
        remote,
      }))(downloaded.independent_expenditures!),
      independent_expenditure_contributions: (({ outputPath, bytesWritten, sha256, ...remote }) => ({
        outputPath,
        bytesWritten,
        sha256,
        remote,
      }))(downloaded.independent_expenditure_contributions!),
    },
  };

  if (!input.force && (await pathExists(paths.metadataPath)) && previous && metadataMatches(previous, current.downloads, current.sourcePageUrl)) {
    return {
      status: "unchanged",
      cacheDir: paths.cacheDir,
      metadataPath: paths.metadataPath,
      previous,
      current: previous,
    };
  }

  await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");

  return {
    status: "downloaded",
    cacheDir: paths.cacheDir,
    metadataPath: paths.metadataPath,
    previous,
    current,
  };
}

export function discoverMinnesotaCampaignFinanceDownloadDescriptors(): readonly MinnesotaCampaignFinanceDownloadDescriptor[] {
  return MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_DESCRIPTORS;
}

export async function discoverMinnesotaCampaignFinanceDownloadUrls(input: FetchOptions = {}): Promise<
  Record<MinnesotaCampaignFinanceDownloadKey, string>
> {
  const discovered = await fetchMinnesotaCampaignFinanceDownloadPage(input);
  return findDownloadUrlsByLabel({
    html: discovered.html,
    pageUrl: discovered.pageUrl,
    descriptors: MINNESOTA_CAMPAIGN_FINANCE_DOWNLOAD_DESCRIPTORS,
  });
}
