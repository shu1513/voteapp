import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { chmod, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  NEW_HAMPSHIRE_CFS_API_BASE_URL,
  NEW_HAMPSHIRE_CFS_ENDPOINTS,
  downloadNewHampshireCfsBulkCsvToFile,
  type NewHampshireCfsBulkDownloadResult,
  type NewHampshireCfsTransactionTypeCode,
} from "./newHampshireCfsClient.js";
import {
  validateNewHampshireExpenditureCsvArtifact,
  validateNewHampshireReceiptCsvArtifact,
} from "./newHampshireCfsArtifactReader.js";

export const DEFAULT_NEW_HAMPSHIRE_CFS_CACHE_DIR =
  "scratch/new-hampshire-campaign-finance/cfs";

export type NewHampshireCfsArtifactKind = "contributions" | "expenditures";

export type NewHampshireCfsArtifactIdentity = {
  filingYear: number;
  artifactKind: NewHampshireCfsArtifactKind;
};

export type NewHampshireCfsArtifactCacheMetadata = {
  version: 1;
  artifact: NewHampshireCfsArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  remote: Omit<NewHampshireCfsBulkDownloadResult, "outputPath" | "bytesWritten" | "sha256">;
  bytesWritten: number;
  sha256: string;
};

export type NewHampshireCfsArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  previous: NewHampshireCfsArtifactCacheMetadata | null;
  current: NewHampshireCfsArtifactCacheMetadata;
};

const CACHE_DIRECTORY_MODE = 0o700;
const CACHE_FILE_MODE = 0o600;

export function normalizeNewHampshireCfsFilingYear(filingYear: number): number {
  if (!Number.isInteger(filingYear) || filingYear < 2016 || filingYear > 2100) {
    throw new Error(`Invalid New Hampshire CFS filing year: ${filingYear}`);
  }
  return filingYear;
}

export function normalizeNewHampshireCfsArtifactKind(value: string): NewHampshireCfsArtifactKind {
  const normalized = value.trim().toLowerCase();
  if (
    normalized === "contributions" ||
    normalized === "contribution" ||
    normalized === "receipts" ||
    normalized === "tcon"
  ) {
    return "contributions";
  }
  if (normalized === "expenditures" || normalized === "expenditure" || normalized === "texp") {
    return "expenditures";
  }
  throw new Error(`Invalid New Hampshire CFS artifact kind: ${value}`);
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

export function normalizeNewHampshireCfsArtifactIdentity(input: {
  filingYear: number;
  artifactKind: string;
}): NewHampshireCfsArtifactIdentity {
  return {
    filingYear: normalizeNewHampshireCfsFilingYear(input.filingYear),
    artifactKind: normalizeNewHampshireCfsArtifactKind(input.artifactKind),
  };
}

export function newHampshireCfsTransactionTypeCode(
  artifactKind: NewHampshireCfsArtifactKind
): NewHampshireCfsTransactionTypeCode {
  return artifactKind === "contributions" ? "TCON" : "TEXP";
}

function artifactPrefix(artifactKind: NewHampshireCfsArtifactKind): "CON" | "EXP" {
  return artifactKind === "contributions" ? "CON" : "EXP";
}

export function getNewHampshireCfsArtifactCachePaths(input: {
  cacheDir: string;
  filingYear: number;
  artifactKind: NewHampshireCfsArtifactKind;
}): { cacheDir: string; filePath: string; metadataPath: string } {
  const artifact = normalizeNewHampshireCfsArtifactIdentity(input);
  const cacheDir = resolve(input.cacheDir);
  const stem = `${artifactPrefix(artifact.artifactKind)}_${artifact.filingYear}`;
  return {
    cacheDir,
    filePath: resolve(cacheDir, `${stem}.csv`),
    metadataPath: resolve(cacheDir, `${stem}.metadata.json`),
  };
}

export async function readNewHampshireCfsArtifactCacheMetadata(
  metadataPath: string
): Promise<NewHampshireCfsArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(
      await readFile(metadataPath, "utf8")
    ) as Partial<NewHampshireCfsArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.artifact?.filingYear !== "number" ||
      (parsed.artifact?.artifactKind !== "contributions" && parsed.artifact?.artifactKind !== "expenditures") ||
      typeof parsed.filePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      Number.isNaN(Date.parse(parsed.downloadedAt)) ||
      !Number.isSafeInteger(parsed.bytesWritten) ||
      parsed.bytesWritten! <= 0 ||
      typeof parsed.sha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(parsed.sha256) ||
      typeof parsed.remote?.url !== "string" ||
      !isNullableString(parsed.remote?.contentType) ||
      !isNullableString(parsed.remote?.contentDisposition) ||
      !isNullableString(parsed.remote?.contentEncoding) ||
      !isNullableString(parsed.remote?.responseDate) ||
      parsed.remote?.requestBody?.type !== "CSV" ||
      typeof parsed.remote?.requestBody?.filingYear !== "number" ||
      (parsed.remote?.requestBody?.transactionTypeCode !== "TCON" &&
        parsed.remote?.requestBody?.transactionTypeCode !== "TEXP")
    ) {
      return null;
    }
    return parsed as NewHampshireCfsArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" || error instanceof SyntaxError) {
      return null;
    }
    throw error;
  }
}

async function calculateFileSha256(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) {
    hash.update(chunk);
  }
  return hash.digest("hex");
}

async function chmodIfExists(path: string, mode: number): Promise<void> {
  try {
    await chmod(path, mode);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

async function validateDownloadedArtifact(
  filePath: string,
  artifactKind: NewHampshireCfsArtifactKind
): Promise<void> {
  if (artifactKind === "contributions") {
    await validateNewHampshireReceiptCsvArtifact({ filePath });
    return;
  }
  await validateNewHampshireExpenditureCsvArtifact({ filePath });
}

async function cachedFileMatches(input: {
  artifact: NewHampshireCfsArtifactIdentity;
  filePath: string;
  metadataPath: string;
  metadata: NewHampshireCfsArtifactCacheMetadata | null;
}): Promise<boolean> {
  const metadata = input.metadata;
  if (
    !metadata ||
    metadata.artifact.filingYear !== input.artifact.filingYear ||
    metadata.artifact.artifactKind !== input.artifact.artifactKind ||
    metadata.filePath !== input.filePath ||
    metadata.metadataPath !== input.metadataPath ||
    metadata.remote.url !==
      `${NEW_HAMPSHIRE_CFS_API_BASE_URL}/${NEW_HAMPSHIRE_CFS_ENDPOINTS.bulkExport}` ||
    metadata.remote.requestBody.filingYear !== input.artifact.filingYear ||
    metadata.remote.requestBody.transactionTypeCode !==
      newHampshireCfsTransactionTypeCode(input.artifact.artifactKind)
  ) {
    return false;
  }
  try {
    const fileStat = await stat(input.filePath);
    return (
      fileStat.isFile() &&
      fileStat.size === metadata.bytesWritten &&
      (await calculateFileSha256(input.filePath)) === metadata.sha256
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

function normalizeRefreshTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid New Hampshire CFS artifact refresh timestamp");
  }
  return normalized;
}

async function writeMetadataAtomically(
  metadataPath: string,
  metadata: NewHampshireCfsArtifactCacheMetadata
): Promise<void> {
  const tmpPath = `${metadataPath}.tmp-${process.pid}-${Date.now()}`;
  try {
    await writeFile(tmpPath, `${JSON.stringify(metadata, null, 2)}\n`, {
      encoding: "utf8",
      mode: CACHE_FILE_MODE,
    });
    await chmod(tmpPath, CACHE_FILE_MODE);
    await rename(tmpPath, metadataPath);
    await chmod(metadataPath, CACHE_FILE_MODE);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }
}

export async function refreshNewHampshireCfsArtifactCache(input: {
  filingYear: number;
  artifactKind: NewHampshireCfsArtifactKind;
  cacheDir: string;
  force?: boolean;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: Date;
}): Promise<NewHampshireCfsArtifactRefreshResult> {
  const artifact = normalizeNewHampshireCfsArtifactIdentity(input);
  const downloadedAt = normalizeRefreshTimestamp(input.now);
  const paths = getNewHampshireCfsArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  await mkdir(paths.cacheDir, { recursive: true, mode: CACHE_DIRECTORY_MODE });
  // mkdir's mode is ignored when the directory already exists.
  await chmod(paths.cacheDir, CACHE_DIRECTORY_MODE);
  await chmodIfExists(paths.filePath, CACHE_FILE_MODE);
  await chmodIfExists(paths.metadataPath, CACHE_FILE_MODE);

  const previous = await readNewHampshireCfsArtifactCacheMetadata(paths.metadataPath);
  const previousFileValid = await cachedFileMatches({
    artifact,
    filePath: paths.filePath,
    metadataPath: paths.metadataPath,
    metadata: previous,
  });
  const tmpPath = `${paths.filePath}.tmp-${process.pid}-${Date.now()}`;

  let downloaded: NewHampshireCfsBulkDownloadResult;
  try {
    downloaded = await downloadNewHampshireCfsBulkCsvToFile(
      {
        filingYear: artifact.filingYear,
        transactionTypeCode: newHampshireCfsTransactionTypeCode(artifact.artifactKind),
        outputPath: tmpPath,
      },
      { fetchImpl: input.fetchImpl, timeoutMs: input.timeoutMs }
    );
    await validateDownloadedArtifact(tmpPath, artifact.artifactKind);

    if (
      !input.force &&
      previous &&
      previousFileValid &&
      previous.bytesWritten === downloaded.bytesWritten &&
      previous.sha256 === downloaded.sha256
    ) {
      await rm(tmpPath, { force: true });
      return { status: "unchanged", ...paths, previous, current: previous };
    }

    await rename(tmpPath, paths.filePath);
    await chmod(paths.filePath, CACHE_FILE_MODE);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }

  const { outputPath: _outputPath, bytesWritten, sha256, ...remote } = downloaded;
  const current: NewHampshireCfsArtifactCacheMetadata = {
    version: 1,
    artifact,
    filePath: paths.filePath,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    remote,
    bytesWritten,
    sha256,
  };
  await writeMetadataAtomically(paths.metadataPath, current);

  return { status: "downloaded", ...paths, previous, current };
}
