import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  buildMaineCfisCsvDownloadRequestBody,
  downloadMaineCfisArtifact,
  fetchMaineCfisArtifactMetadata,
  maineCfisTransactionType,
  normalizeMaineCfisArtifactIdentity,
  type MaineCfisArtifactDownloadResult,
  type MaineCfisArtifactIdentity,
  type MaineCfisArtifactKind,
  type MaineCfisRemoteArtifactMetadata,
} from "./maineCfisClient.js";

export const DEFAULT_MAINE_CFIS_CACHE_DIR = "scratch/maine-campaign-finance/cfis";

export type MaineCfisArtifactCacheMetadata = {
  version: 1;
  artifact: MaineCfisArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: MaineCfisRemoteArtifactMetadata;
  bytesWritten: number;
};

export type MaineCfisArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  remote: MaineCfisRemoteArtifactMetadata;
  previous: MaineCfisArtifactCacheMetadata | null;
  current: MaineCfisArtifactCacheMetadata;
};

function artifactFileName(artifact: MaineCfisArtifactIdentity): string {
  return `${maineCfisTransactionType(artifact.artifactKind)}_${artifact.filingYear}.csv`;
}

function artifactMetadataFileName(artifact: MaineCfisArtifactIdentity): string {
  return `${maineCfisTransactionType(artifact.artifactKind)}_${artifact.filingYear}.metadata.json`;
}

export function getMaineCfisArtifactCachePaths(input: {
  cacheDir: string;
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
}): {
  cacheDir: string;
  filePath: string;
  metadataPath: string;
} {
  const artifact = normalizeMaineCfisArtifactIdentity(input);
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

async function cachedFileMatchesMetadata(input: {
  filePath: string;
  metadata: MaineCfisArtifactCacheMetadata | null;
}): Promise<boolean> {
  if (!input.metadata) {
    return false;
  }
  try {
    const fileStat = await stat(input.filePath);
    return fileStat.isFile() && fileStat.size === input.metadata.bytesWritten;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

export async function readMaineCfisArtifactCacheMetadata(
  metadataPath: string
): Promise<MaineCfisArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<MaineCfisArtifactCacheMetadata>;
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
      typeof parsed.remote?.requestBody?.year !== "number" ||
      typeof parsed.remote?.requestBody?.transactionType !== "string"
    ) {
      return null;
    }
    return parsed as MaineCfisArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading Maine CFIS cache metadata at ${metadataPath}:`, error);
    return null;
  }
}

function remoteMetadataMatches(
  previous: MaineCfisArtifactCacheMetadata | null,
  remote: MaineCfisRemoteArtifactMetadata
): boolean {
  if (
    !previous ||
    previous.artifact.filingYear !== remote.filingYear ||
    previous.artifact.artifactKind !== remote.artifactKind ||
    previous.remote.url !== remote.url ||
    previous.remote.requestBody.transactionType !== remote.requestBody.transactionType ||
    previous.remote.requestBody.year !== remote.requestBody.year
  ) {
    return false;
  }
  if (previous.remote.etag && remote.etag) {
    return previous.remote.etag === remote.etag;
  }
  if (previous.remote.lastModified && remote.lastModified) {
    return previous.remote.lastModified === remote.lastModified && previous.remote.contentLength === remote.contentLength;
  }
  return previous.remote.contentLength !== null && previous.remote.contentLength === remote.contentLength;
}

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Maine CFIS artifact refresh timestamp");
  }
  return normalized;
}

function cacheMetadataFromDownload(input: {
  artifact: MaineCfisArtifactIdentity;
  paths: { filePath: string; metadataPath: string };
  downloadedAt: Date;
  downloaded: MaineCfisArtifactDownloadResult;
}): MaineCfisArtifactCacheMetadata {
  return {
    version: 1,
    artifact: input.artifact,
    filePath: input.paths.filePath,
    metadataPath: input.paths.metadataPath,
    downloadedAt: input.downloadedAt.toISOString(),
    remote: {
      ...input.artifact,
      url: input.downloaded.url,
      requestBody: buildMaineCfisCsvDownloadRequestBody(input.artifact),
      contentLength: input.downloaded.contentLength,
      contentType: input.downloaded.contentType,
      contentDisposition: input.downloaded.contentDisposition,
      etag: input.downloaded.etag,
      lastModified: input.downloaded.lastModified,
    },
    bytesWritten: input.downloaded.bytesWritten,
  };
}

export async function refreshMaineCfisArtifactCache(input: {
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
  cacheDir: string;
  url?: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<MaineCfisArtifactRefreshResult> {
  const artifact = normalizeMaineCfisArtifactIdentity(input);
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getMaineCfisArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  const remote = await fetchMaineCfisArtifactMetadata({
    ...artifact,
    url: input.url,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.timeoutMs,
  });
  const previous = await readMaineCfisArtifactCacheMetadata(paths.metadataPath);
  const fileExists = await pathExists(paths.filePath);
  const cachedFileValid = await cachedFileMatchesMetadata({ filePath: paths.filePath, metadata: previous });

  if (!input.force && fileExists && cachedFileValid && remoteMetadataMatches(previous, remote)) {
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
  const downloaded = await downloadMaineCfisArtifact({
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
    remote: current.remote,
    previous,
    current,
  };
}
