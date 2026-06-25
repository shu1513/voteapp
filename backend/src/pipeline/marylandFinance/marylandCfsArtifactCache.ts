import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  type MarylandCfsArtifactDownloadResult,
  type MarylandCfsArtifactIdentity,
  type MarylandCfsArtifactKind,
  type MarylandCfsRemoteArtifactMetadata,
  buildMarylandCfsPublicExportRequestBody,
  downloadMarylandCfsArtifact,
  fetchMarylandCfsArtifactMetadata,
  marylandCfsTransactionTypeCode,
  normalizeMarylandCfsArtifactIdentity,
} from "./marylandCfsClient.js";

export const DEFAULT_MARYLAND_CFS_CACHE_DIR = "scratch/maryland-campaign-finance/cfs";

export type MarylandCfsArtifactCacheMetadata = {
  version: 1;
  artifact: MarylandCfsArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: MarylandCfsRemoteArtifactMetadata;
  bytesWritten: number;
};

export type MarylandCfsArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  remote: MarylandCfsRemoteArtifactMetadata;
  previous: MarylandCfsArtifactCacheMetadata | null;
  current: MarylandCfsArtifactCacheMetadata;
};

function artifactFileName(artifact: MarylandCfsArtifactIdentity): string {
  return `${marylandCfsTransactionTypeCode(artifact.artifactKind)}_${artifact.filingYear}.csv`;
}

function artifactMetadataFileName(artifact: MarylandCfsArtifactIdentity): string {
  return `${marylandCfsTransactionTypeCode(artifact.artifactKind)}_${artifact.filingYear}.metadata.json`;
}

export function getMarylandCfsArtifactCachePaths(input: {
  cacheDir: string;
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
}): {
  cacheDir: string;
  filePath: string;
  metadataPath: string;
} {
  const artifact = normalizeMarylandCfsArtifactIdentity(input);
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

export async function readMarylandCfsArtifactCacheMetadata(
  metadataPath: string
): Promise<MarylandCfsArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<MarylandCfsArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.artifact?.filingYear !== "number" ||
      typeof parsed.artifact?.artifactKind !== "string" ||
      typeof parsed.filePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      typeof parsed.bytesWritten !== "number" ||
      typeof parsed.remote?.url !== "string" ||
      typeof parsed.remote?.filingYear !== "number" ||
      typeof parsed.remote?.artifactKind !== "string" ||
      typeof parsed.remote?.requestBody?.Type !== "string" ||
      typeof parsed.remote?.requestBody?.TransactionTypeCode !== "string" ||
      typeof parsed.remote?.requestBody?.FilingYear !== "number"
    ) {
      return null;
    }
    return parsed as MarylandCfsArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Maryland CFS cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: MarylandCfsArtifactCacheMetadata | null,
  remote: MarylandCfsRemoteArtifactMetadata
): boolean {
  if (
    !previous ||
    previous.artifact.filingYear !== remote.filingYear ||
    previous.artifact.artifactKind !== remote.artifactKind ||
    previous.remote.url !== remote.url ||
    previous.remote.requestBody.TransactionTypeCode !== remote.requestBody.TransactionTypeCode ||
    previous.remote.requestBody.FilingYear !== remote.requestBody.FilingYear
  ) {
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

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Maryland CFS artifact refresh timestamp");
  }
  return normalized;
}

function cacheMetadataFromDownload(input: {
  artifact: MarylandCfsArtifactIdentity;
  paths: { filePath: string; metadataPath: string };
  downloadedAt: Date;
  downloaded: MarylandCfsArtifactDownloadResult;
}): MarylandCfsArtifactCacheMetadata {
  return {
    version: 1,
    artifact: input.artifact,
    filePath: input.paths.filePath,
    metadataPath: input.paths.metadataPath,
    downloadedAt: input.downloadedAt.toISOString(),
    remote: {
      ...input.artifact,
      url: input.downloaded.url,
      requestBody: buildMarylandCfsPublicExportRequestBody(input.artifact),
      contentLength: input.downloaded.contentLength,
      contentType: input.downloaded.contentType,
      etag: input.downloaded.etag,
      lastModified: input.downloaded.lastModified,
    },
    bytesWritten: input.downloaded.bytesWritten,
  };
}

export async function refreshMarylandCfsArtifactCache(input: {
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<MarylandCfsArtifactRefreshResult> {
  const artifact = normalizeMarylandCfsArtifactIdentity(input);
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getMarylandCfsArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  const remote = await fetchMarylandCfsArtifactMetadata({
    ...artifact,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readMarylandCfsArtifactCacheMetadata(paths.metadataPath);
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
  const downloaded = await downloadMarylandCfsArtifact({
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

  const current = cacheMetadataFromDownload({
    artifact,
    paths,
    downloadedAt,
    downloaded,
  });
  await writeFile(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`, "utf8");

  return {
    status: "downloaded",
    ...paths,
    remote,
    previous,
    current,
  };
}
