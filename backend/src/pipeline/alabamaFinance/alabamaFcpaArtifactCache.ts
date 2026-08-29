// On-disk cache of Alabama FCPA bulk-extract artifacts, keyed by
// (extract kind, year). Stores the raw zip exactly as the portal served it,
// with integrity metadata alongside (pattern: newHampshireCfsArtifactCache.ts).
// Catalog download ids are unstable rows, so every refresh resolves the id
// from a fresh catalog read (plan-alabama-finance.md, gotcha 2). A failed or
// rejected download never disturbs the last good artifact.

import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  downloadAlabamaExtractZipBytes,
  getAlabamaExtractCatalog,
  unzipAlabamaExtract,
  type AlabamaExtractCatalogRow,
  type AlabamaFcpaClientOptions,
} from "./alabamaFcpaClient.js";
import {
  parseAlabamaCashExtract,
  parseAlabamaExpenditureExtract,
} from "./alabamaFcpaCsv.js";

export const DEFAULT_ALABAMA_FCPA_CACHE_DIR = "scratch/alabama-campaign-finance/fcpa";

export type AlabamaExtractKind = "cash" | "expenditure";

/** Catalog DATATYPE strings, verified live 2026-08-26. */
export const ALABAMA_EXTRACT_KIND_DATATYPE: Record<AlabamaExtractKind, string> = {
  cash: "Cash Contribution",
  expenditure: "Expenditure",
};

export type AlabamaFcpaArtifactIdentity = {
  kind: AlabamaExtractKind;
  year: number;
};

export type AlabamaFcpaArtifactCacheMetadata = {
  version: 1;
  artifact: AlabamaFcpaArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  source: {
    /** Catalog row the artifact came from; the id is not stable across days. */
    dataType: string;
    downloadId: number;
    lastUpdatedRaw: string;
  };
  /** CSV entry name inside the zip. */
  fileName: string;
  zipBytes: number;
  zipSha256: string;
  /** Content identity: zips recompress daily, the CSV text is what matters. */
  csvSha256: string;
  recordCount: number;
  quarantinedCount: number;
};

export type AlabamaFcpaArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  previous: AlabamaFcpaArtifactCacheMetadata | null;
  current: AlabamaFcpaArtifactCacheMetadata;
};

const CACHE_DIRECTORY_MODE = 0o700;
const CACHE_FILE_MODE = 0o600;

export function normalizeAlabamaExtractYear(year: number): number {
  // Extracts exist 2013 onward.
  if (!Number.isInteger(year) || year < 2013 || year > 2100) {
    throw new Error(`Invalid Alabama FCPA extract year: ${year}`);
  }
  return year;
}

export function normalizeAlabamaExtractKind(value: string): AlabamaExtractKind {
  const normalized = value.trim().toLowerCase();
  if (normalized === "cash" || normalized === "cash contribution") return "cash";
  if (normalized === "expenditure" || normalized === "expenditures") return "expenditure";
  throw new Error(`Invalid Alabama FCPA extract kind: ${value}`);
}

export function getAlabamaFcpaArtifactCachePaths(input: {
  cacheDir: string;
  kind: AlabamaExtractKind;
  year: number;
}): { cacheDir: string; filePath: string; metadataPath: string } {
  const year = normalizeAlabamaExtractYear(input.year);
  const cacheDir = resolve(input.cacheDir);
  const stem = `${input.kind === "cash" ? "CASH" : "EXP"}_${year}`;
  return {
    cacheDir,
    filePath: resolve(cacheDir, `${stem}.zip`),
    metadataPath: resolve(cacheDir, `${stem}.metadata.json`),
  };
}

export async function readAlabamaFcpaArtifactCacheMetadata(
  metadataPath: string
): Promise<AlabamaFcpaArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(
      await readFile(metadataPath, "utf8")
    ) as Partial<AlabamaFcpaArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      (parsed.artifact?.kind !== "cash" && parsed.artifact?.kind !== "expenditure") ||
      typeof parsed.artifact?.year !== "number" ||
      typeof parsed.filePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      Number.isNaN(Date.parse(parsed.downloadedAt)) ||
      typeof parsed.source?.dataType !== "string" ||
      !Number.isSafeInteger(parsed.source?.downloadId) ||
      typeof parsed.source?.lastUpdatedRaw !== "string" ||
      typeof parsed.fileName !== "string" ||
      !Number.isSafeInteger(parsed.zipBytes) ||
      parsed.zipBytes! <= 0 ||
      typeof parsed.zipSha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(parsed.zipSha256) ||
      typeof parsed.csvSha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(parsed.csvSha256) ||
      !Number.isSafeInteger(parsed.recordCount) ||
      parsed.recordCount! < 0 ||
      !Number.isSafeInteger(parsed.quarantinedCount) ||
      parsed.quarantinedCount! < 0
    ) {
      return null;
    }
    return parsed as AlabamaFcpaArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" || error instanceof SyntaxError) {
      return null;
    }
    throw error;
  }
}

function sha256(bytes: Uint8Array | string): string {
  return createHash("sha256").update(bytes).digest("hex");
}

async function chmodIfExists(path: string, mode: number): Promise<void> {
  try {
    await chmod(path, mode);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

/**
 * Validate downloaded bytes as a well-formed extract for the kind: single-CSV
 * zip, header unchanged (the parser throws on drift). Returns content stats
 * for the metadata; the tolerant parser quarantines ragged rows, it does not
 * reject the file over them.
 */
function validateAlabamaExtractZip(
  bytes: Uint8Array,
  kind: AlabamaExtractKind,
  label: string
): { fileName: string; csvSha256: string; recordCount: number; quarantinedCount: number } {
  const { fileName, csvText } = unzipAlabamaExtract(bytes, label);
  const parsed =
    kind === "cash" ? parseAlabamaCashExtract(csvText) : parseAlabamaExpenditureExtract(csvText);
  return {
    fileName,
    csvSha256: sha256(csvText),
    recordCount: parsed.recordCount,
    quarantinedCount: parsed.quarantined.length,
  };
}

async function cachedFileMatches(input: {
  artifact: AlabamaFcpaArtifactIdentity;
  filePath: string;
  metadataPath: string;
  metadata: AlabamaFcpaArtifactCacheMetadata | null;
}): Promise<boolean> {
  const metadata = input.metadata;
  if (
    !metadata ||
    metadata.artifact.kind !== input.artifact.kind ||
    metadata.artifact.year !== input.artifact.year ||
    metadata.filePath !== input.filePath ||
    metadata.metadataPath !== input.metadataPath ||
    metadata.source.dataType !== ALABAMA_EXTRACT_KIND_DATATYPE[input.artifact.kind]
  ) {
    return false;
  }
  try {
    const fileStat = await stat(input.filePath);
    return (
      fileStat.isFile() &&
      fileStat.size === metadata.zipBytes &&
      sha256(await readFile(input.filePath)) === metadata.zipSha256
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

async function writeFileAtomically(path: string, data: Uint8Array | string): Promise<void> {
  const tmpPath = `${path}.tmp-${process.pid}-${Date.now()}`;
  try {
    await writeFile(tmpPath, data, { mode: CACHE_FILE_MODE });
    await chmod(tmpPath, CACHE_FILE_MODE);
    await rename(tmpPath, path);
    await chmod(path, CACHE_FILE_MODE);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }
}

export async function refreshAlabamaFcpaArtifactCache(input: {
  kind: AlabamaExtractKind;
  year: number;
  cacheDir: string;
  /** Reuse one catalog read across several artifacts in a run. */
  catalog?: AlabamaExtractCatalogRow[];
  force?: boolean;
  clientOptions?: AlabamaFcpaClientOptions;
  now?: Date;
}): Promise<AlabamaFcpaArtifactRefreshResult> {
  const artifact: AlabamaFcpaArtifactIdentity = {
    kind: input.kind,
    year: normalizeAlabamaExtractYear(input.year),
  };
  const downloadedAt = input.now ?? new Date();
  if (Number.isNaN(downloadedAt.getTime())) {
    throw new Error("Invalid Alabama FCPA artifact refresh timestamp");
  }
  const paths = getAlabamaFcpaArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  await mkdir(paths.cacheDir, { recursive: true, mode: CACHE_DIRECTORY_MODE });
  // mkdir's mode is ignored when the directory already exists.
  await chmod(paths.cacheDir, CACHE_DIRECTORY_MODE);
  await chmodIfExists(paths.filePath, CACHE_FILE_MODE);
  await chmodIfExists(paths.metadataPath, CACHE_FILE_MODE);

  const previous = await readAlabamaFcpaArtifactCacheMetadata(paths.metadataPath);
  const previousFileValid = await cachedFileMatches({
    artifact,
    filePath: paths.filePath,
    metadataPath: paths.metadataPath,
    metadata: previous,
  });

  const dataType = ALABAMA_EXTRACT_KIND_DATATYPE[artifact.kind];
  const catalog = input.catalog ?? (await getAlabamaExtractCatalog(input.clientOptions));
  const entry = catalog.find((row) => row.DATATYPE === dataType && row.YEAR === artifact.year);
  if (!entry) {
    throw new Error(`Alabama FCPA extract catalog has no ${dataType} ${artifact.year} entry`);
  }

  const bytes = await downloadAlabamaExtractZipBytes(entry.DOWNLOAD, input.clientOptions);
  const validated = validateAlabamaExtractZip(bytes, artifact.kind, `${dataType} ${artifact.year}`);

  // Zips recompress on the portal's daily regeneration; identical CSV content
  // means the cached artifact is still current even when zip bytes differ.
  if (!input.force && previous && previousFileValid && previous.csvSha256 === validated.csvSha256) {
    return { status: "unchanged", ...paths, previous, current: previous };
  }

  await writeFileAtomically(paths.filePath, bytes);
  const current: AlabamaFcpaArtifactCacheMetadata = {
    version: 1,
    artifact,
    filePath: paths.filePath,
    metadataPath: paths.metadataPath,
    downloadedAt: downloadedAt.toISOString(),
    source: {
      dataType,
      downloadId: entry.DOWNLOAD,
      lastUpdatedRaw: entry.LASTUPDATEDRAW,
    },
    fileName: validated.fileName,
    zipBytes: bytes.byteLength,
    zipSha256: sha256(bytes),
    csvSha256: validated.csvSha256,
    recordCount: validated.recordCount,
    quarantinedCount: validated.quarantinedCount,
  };
  await writeFileAtomically(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`);

  return { status: "downloaded", ...paths, previous, current };
}

/**
 * Read one cached artifact back to CSV text, verifying integrity end to end
 * (zip checksum, then CSV checksum after unzip). Throws when the artifact is
 * missing or corrupt — sync consumers fail closed rather than aggregate a
 * damaged file.
 */
export async function readAlabamaFcpaArtifact(input: {
  kind: AlabamaExtractKind;
  year: number;
  cacheDir: string;
}): Promise<{ metadata: AlabamaFcpaArtifactCacheMetadata; csvText: string }> {
  const artifact: AlabamaFcpaArtifactIdentity = {
    kind: input.kind,
    year: normalizeAlabamaExtractYear(input.year),
  };
  const paths = getAlabamaFcpaArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  const metadata = await readAlabamaFcpaArtifactCacheMetadata(paths.metadataPath);
  const label = `${artifact.kind} ${artifact.year}`;
  if (!metadata) {
    throw new Error(`Alabama FCPA artifact ${label} has no cached metadata at ${paths.metadataPath}`);
  }
  if (!(await cachedFileMatches({ ...paths, artifact, metadata }))) {
    throw new Error(`Alabama FCPA artifact ${label} is missing or corrupt at ${paths.filePath}`);
  }
  const { csvText } = unzipAlabamaExtract(await readFile(paths.filePath), label);
  if (sha256(csvText) !== metadata.csvSha256) {
    throw new Error(`Alabama FCPA artifact ${label} CSV content does not match its metadata checksum`);
  }
  return { metadata, csvText };
}
