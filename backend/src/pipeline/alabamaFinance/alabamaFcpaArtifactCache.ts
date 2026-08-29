// On-disk cache of Alabama FCPA bulk-extract artifacts, keyed by
// (extract kind, year). Stores the raw zip exactly as the portal served it,
// with integrity metadata alongside (pattern: newHampshireCfsArtifactCache.ts).
// Catalog download ids are unstable rows, so every refresh resolves the id
// from a fresh catalog read (plan-alabama-finance.md, gotcha 2). A failed or
// rejected download never disturbs the last good artifact: zip files are
// content-addressed (`STEM.<csvSha prefix>.zip`) and the metadata file is the
// atomic commit pointer, so a crash between the two writes leaves the old
// pair fully intact; stale zip versions are swept after a successful commit.

import { createHash } from "node:crypto";
import { basename, resolve } from "node:path";
import { chmod, mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";

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
/** Superseded zips younger than this survive the sweep (concurrent-refresh guard). */
export const SWEEP_MIN_AGE_MS = 60 * 60 * 1000;

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
}): { cacheDir: string; stem: string; metadataPath: string } {
  const year = normalizeAlabamaExtractYear(input.year);
  const cacheDir = resolve(input.cacheDir);
  const stem = `${input.kind === "cash" ? "CASH" : "EXP"}_${year}`;
  return {
    cacheDir,
    stem,
    metadataPath: resolve(cacheDir, `${stem}.metadata.json`),
  };
}

/** Content-addressed zip path; the metadata file is what commits it. */
function alabamaFcpaArtifactZipPath(cacheDir: string, stem: string, csvSha256: string): string {
  return resolve(cacheDir, `${stem}.${csvSha256.slice(0, 12)}.zip`);
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
  cacheDir: string;
  stem: string;
  metadataPath: string;
  metadata: AlabamaFcpaArtifactCacheMetadata | null;
}): Promise<boolean> {
  const metadata = input.metadata;
  if (
    !metadata ||
    metadata.artifact.kind !== input.artifact.kind ||
    metadata.artifact.year !== input.artifact.year ||
    metadata.metadataPath !== input.metadataPath ||
    metadata.source.dataType !== ALABAMA_EXTRACT_KIND_DATATYPE[input.artifact.kind] ||
    // The zip file is content-addressed and named by the metadata pointer;
    // sanity-check the pointer stays inside the cache under this stem.
    metadata.filePath !== alabamaFcpaArtifactZipPath(input.cacheDir, input.stem, metadata.csvSha256)
  ) {
    return false;
  }
  try {
    const fileStat = await stat(metadata.filePath);
    return (
      fileStat.isFile() &&
      fileStat.size === metadata.zipBytes &&
      sha256(await readFile(metadata.filePath)) === metadata.zipSha256
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
  await chmodIfExists(paths.metadataPath, CACHE_FILE_MODE);

  const previous = await readAlabamaFcpaArtifactCacheMetadata(paths.metadataPath);
  if (previous) await chmodIfExists(previous.filePath, CACHE_FILE_MODE);
  const previousFileValid = await cachedFileMatches({ ...paths, artifact, metadata: previous });

  const dataType = ALABAMA_EXTRACT_KIND_DATATYPE[artifact.kind];
  const catalog = input.catalog ?? (await getAlabamaExtractCatalog(input.clientOptions));
  const entry = catalog.find((row) => row.DATATYPE === dataType && row.YEAR === artifact.year);
  if (!entry) {
    throw new Error(`Alabama FCPA extract catalog has no ${dataType} ${artifact.year} entry`);
  }

  const bytes = await downloadAlabamaExtractZipBytes(entry.DOWNLOAD, input.clientOptions);
  const validated = validateAlabamaExtractZip(bytes, artifact.kind, `${dataType} ${artifact.year}`);

  // A well-formed header with zero data rows is a truncated portal artifact
  // far more often than a genuinely empty year; never let it displace
  // populated data (force overrides for a deliberate accept).
  if (
    !input.force &&
    validated.recordCount === 0 &&
    previous &&
    previousFileValid &&
    previous.recordCount > 0
  ) {
    throw new Error(
      `Alabama FCPA ${dataType} ${artifact.year} extract returned 0 data rows; ` +
        `keeping the last good artifact (${previous.recordCount} rows). Pass force to accept it.`
    );
  }

  // Zips recompress on the portal's daily regeneration; identical CSV content
  // means the cached artifact is still current even when zip bytes differ.
  if (!input.force && previous && previousFileValid && previous.csvSha256 === validated.csvSha256) {
    return {
      status: "unchanged",
      cacheDir: paths.cacheDir,
      filePath: previous.filePath,
      metadataPath: paths.metadataPath,
      previous,
      current: previous,
    };
  }

  // Two-step commit: the content-addressed zip lands first, then the metadata
  // pointer commits it atomically. A crash between the writes leaves the old
  // zip + metadata pair fully intact.
  const zipPath = alabamaFcpaArtifactZipPath(paths.cacheDir, paths.stem, validated.csvSha256);
  await writeFileAtomically(zipPath, bytes);
  const current: AlabamaFcpaArtifactCacheMetadata = {
    version: 1,
    artifact,
    filePath: zipPath,
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

  // Best-effort sweep of superseded zip versions (and pre-versioning names).
  // Only versions older than SWEEP_MIN_AGE_MS are removed: a zip written
  // seconds ago may belong to a concurrent refresh whose metadata commit is
  // about to land — deleting it would break that refresh's committed pair.
  // Fresh orphans are harmless and get swept by a later refresh.
  try {
    for (const name of await readdir(paths.cacheDir)) {
      if (
        !name.startsWith(`${paths.stem}.`) ||
        !name.endsWith(".zip") ||
        name === basename(zipPath)
      ) {
        continue;
      }
      const stalePath = resolve(paths.cacheDir, name);
      const staleStat = await stat(stalePath);
      if (Date.now() - staleStat.mtimeMs >= SWEEP_MIN_AGE_MS) {
        await rm(stalePath, { force: true });
      }
    }
  } catch {
    // Stale versions are harmless; the next successful refresh sweeps again.
  }

  return {
    status: "downloaded",
    cacheDir: paths.cacheDir,
    filePath: zipPath,
    metadataPath: paths.metadataPath,
    previous,
    current,
  };
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
    throw new Error(`Alabama FCPA artifact ${label} is missing or corrupt at ${metadata.filePath}`);
  }
  const { csvText } = unzipAlabamaExtract(await readFile(metadata.filePath), label);
  if (sha256(csvText) !== metadata.csvSha256) {
    throw new Error(`Alabama FCPA artifact ${label} CSV content does not match its metadata checksum`);
  }
  return { metadata, csvText };
}
