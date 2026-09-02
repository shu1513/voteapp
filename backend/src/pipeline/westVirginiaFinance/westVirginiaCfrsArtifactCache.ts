// On-disk cache of WV CFRS artifacts, keyed by (kind, year), in the Alabama
// shape: content-addressed files (`STEM.<sha prefix>.csv|json`) plus a
// metadata pointer (`STEM.metadata.json`) that commits them atomically — a
// crash between the two writes leaves the last good pair intact, and stale
// versions are swept after a successful commit.
//
// Two artifact families:
// - bulk CSVs from the nightly data-download catalog (Contributions,
//   Expenditures, Reporting Schedules), stored as the raw cp1252 bytes the
//   presigned URL served;
// - the org-101 CON transaction-search harvest for one year (the API rows
//   that carry occupation), stored as JSON with rows sorted by transactionID
//   so identical data hashes identically.
// Every refresh re-parses the body before committing it; a body the parser
// rejects (header drift, row errors) never displaces the last good artifact.
//
// PII: contribution files carry contributor street addresses — directories
// are 0700, files 0600, and the cache directory must never enter git. The
// sync reads this cache only; the refresh CLI (its own flag) is the only
// live-portal caller.

import { createHash } from "node:crypto";
import { chmod, mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";

import {
  WEST_VIRGINIA_ORG_TYPE_CODES,
  downloadWestVirginiaPresignedFile,
  getAllWestVirginiaTransactions,
  getWestVirginiaDataDownloadCatalog,
  getWestVirginiaDataDownloadFileUrl,
  type WestVirginiaCfrsClientOptions,
  type WestVirginiaDataDownloadCatalogRow,
  type WestVirginiaTransactionRow,
} from "./westVirginiaCfrsClient.js";
import {
  decodeWestVirginiaCsvBytes,
  parseWestVirginiaContributionCsv,
  parseWestVirginiaExpenditureCsv,
  parseWestVirginiaReportingScheduleCsv,
  type WestVirginiaCsvParseError,
} from "./westVirginiaCfrsCsv.js";

export const DEFAULT_WEST_VIRGINIA_CFRS_CACHE_DIR = "scratch/west-virginia-campaign-finance/cfrs";

export type WestVirginiaBulkArtifactKind = "contributions" | "expenditures" | "reporting_schedules";
export type WestVirginiaArtifactKind = WestVirginiaBulkArtifactKind | "api_contributions";

/** Catalog dataType strings, verified live 2026-08-27. */
export const WEST_VIRGINIA_BULK_ARTIFACT_DATA_TYPE: Record<WestVirginiaBulkArtifactKind, string> = {
  contributions: "Contributions",
  expenditures: "Expenditures",
  reporting_schedules: "Reporting Schedules",
};

const ARTIFACT_STEM: Record<WestVirginiaArtifactKind, string> = {
  contributions: "CON",
  expenditures: "EXP",
  reporting_schedules: "REPS",
  api_contributions: "APICON",
};

const ARTIFACT_EXTENSION: Record<WestVirginiaArtifactKind, "csv" | "json"> = {
  contributions: "csv",
  expenditures: "csv",
  reporting_schedules: "csv",
  api_contributions: "json",
};

export const WEST_VIRGINIA_API_ARTIFACT_VERSION = 1;

export type WestVirginiaApiContributionsArtifactBody = {
  version: typeof WEST_VIRGINIA_API_ARTIFACT_VERSION;
  request: { orgTypeCode: string; transactionCategory: "CON"; transactionYear: number };
  rows: WestVirginiaTransactionRow[];
};

export type WestVirginiaArtifactIdentity = { kind: WestVirginiaArtifactKind; year: number };

export type WestVirginiaArtifactCacheMetadata = {
  version: 1;
  artifact: WestVirginiaArtifactIdentity;
  filePath: string;
  metadataPath: string;
  downloadedAt: string;
  /** Catalog row (bulk) or the search request (API) the artifact came from. */
  source: {
    catalogId: number | null;
    s3ReportFilePath: string | null;
    dataType: string | null;
  };
  bytes: number;
  sha256: string;
  recordCount: number;
  recoveredRowCount: number;
};

export type WestVirginiaArtifactRefreshResult = {
  status: "downloaded" | "unchanged";
  cacheDir: string;
  filePath: string;
  metadataPath: string;
  previous: WestVirginiaArtifactCacheMetadata | null;
  current: WestVirginiaArtifactCacheMetadata;
};

const CACHE_DIRECTORY_MODE = 0o700;
const CACHE_FILE_MODE = 0o600;
/** Superseded versions younger than this survive the sweep (concurrent-refresh guard). */
export const SWEEP_MIN_AGE_MS = 60 * 60 * 1000;

export function normalizeWestVirginiaArtifactYear(year: number): number {
  // The catalog reaches back to 2015; Reporting Schedules start at 2025.
  if (!Number.isInteger(year) || year < 2015 || year > 2100) {
    throw new Error(`Invalid West Virginia CFRS artifact year: ${year}`);
  }
  return year;
}

export function getWestVirginiaArtifactCachePaths(input: {
  cacheDir: string;
  kind: WestVirginiaArtifactKind;
  year: number;
}): { cacheDir: string; stem: string; extension: "csv" | "json"; metadataPath: string } {
  const year = normalizeWestVirginiaArtifactYear(input.year);
  const cacheDir = resolve(input.cacheDir);
  const stem = `${ARTIFACT_STEM[input.kind]}_${year}`;
  return {
    cacheDir,
    stem,
    extension: ARTIFACT_EXTENSION[input.kind],
    metadataPath: resolve(cacheDir, `${stem}.metadata.json`),
  };
}

function contentPath(cacheDir: string, stem: string, extension: string, sha256: string): string {
  return resolve(cacheDir, `${stem}.${sha256.slice(0, 12)}.${extension}`);
}

function sha256Of(bytes: Uint8Array | string): string {
  return createHash("sha256").update(bytes).digest("hex");
}

export async function readWestVirginiaArtifactCacheMetadata(
  metadataPath: string
): Promise<WestVirginiaArtifactCacheMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<WestVirginiaArtifactCacheMetadata>;
    if (
      parsed.version !== 1 ||
      typeof parsed.artifact?.kind !== "string" ||
      !(parsed.artifact.kind in ARTIFACT_STEM) ||
      typeof parsed.artifact?.year !== "number" ||
      typeof parsed.filePath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.downloadedAt !== "string" ||
      Number.isNaN(Date.parse(parsed.downloadedAt)) ||
      typeof parsed.source !== "object" ||
      parsed.source === null ||
      !Number.isSafeInteger(parsed.bytes) ||
      parsed.bytes! <= 0 ||
      typeof parsed.sha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(parsed.sha256) ||
      !Number.isSafeInteger(parsed.recordCount) ||
      parsed.recordCount! < 0 ||
      !Number.isSafeInteger(parsed.recoveredRowCount) ||
      parsed.recoveredRowCount! < 0
    ) {
      return null;
    }
    return parsed as WestVirginiaArtifactCacheMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" || error instanceof SyntaxError) {
      return null;
    }
    throw error;
  }
}

async function chmodIfExists(path: string, mode: number): Promise<void> {
  try {
    await chmod(path, mode);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

async function cachedFileMatches(input: {
  artifact: WestVirginiaArtifactIdentity;
  cacheDir: string;
  stem: string;
  extension: string;
  metadataPath: string;
  metadata: WestVirginiaArtifactCacheMetadata | null;
}): Promise<boolean> {
  const metadata = input.metadata;
  if (
    !metadata ||
    metadata.artifact.kind !== input.artifact.kind ||
    metadata.artifact.year !== input.artifact.year ||
    metadata.metadataPath !== input.metadataPath ||
    metadata.filePath !== contentPath(input.cacheDir, input.stem, input.extension, metadata.sha256)
  ) {
    return false;
  }
  try {
    const fileStat = await stat(metadata.filePath);
    return (
      fileStat.isFile() &&
      fileStat.size === metadata.bytes &&
      sha256Of(await readFile(metadata.filePath)) === metadata.sha256
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

function requireCleanParse(
  label: string,
  result: { rows: unknown[]; errors: WestVirginiaCsvParseError[]; recoveredRowCount: number }
): { recordCount: number; recoveredRowCount: number } {
  if (result.errors.length > 0) {
    const sample = result.errors
      .slice(0, 3)
      .map((error) => `line ${error.line}: ${error.reason}`)
      .join("; ");
    throw new Error(`West Virginia ${label} parse produced ${result.errors.length} row errors (${sample})`);
  }
  return { recordCount: result.rows.length, recoveredRowCount: result.recoveredRowCount };
}

/** Parses bulk CSV bytes for the kind; throws on header drift or row errors. */
export function parseWestVirginiaBulkArtifactBytes(kind: WestVirginiaBulkArtifactKind, bytes: Uint8Array) {
  const text = decodeWestVirginiaCsvBytes(bytes);
  switch (kind) {
    case "contributions":
      return parseWestVirginiaContributionCsv(text);
    case "expenditures":
      return parseWestVirginiaExpenditureCsv(text);
    case "reporting_schedules":
      return parseWestVirginiaReportingScheduleCsv(text);
  }
}

function parseApiArtifactBytes(bytes: Uint8Array, year: number): WestVirginiaApiContributionsArtifactBody {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error(`West Virginia API artifact ${year} is not valid JSON`);
  }
  const body = parsed as Partial<WestVirginiaApiContributionsArtifactBody>;
  if (
    body?.version !== WEST_VIRGINIA_API_ARTIFACT_VERSION ||
    body.request?.transactionYear !== year ||
    body.request.transactionCategory !== "CON" ||
    !Array.isArray(body.rows)
  ) {
    throw new Error(`West Virginia API artifact ${year} has an unexpected shape`);
  }
  return body as WestVirginiaApiContributionsArtifactBody;
}

type StagedArtifact = {
  bytes: Uint8Array;
  sha256: string;
  recordCount: number;
  recoveredRowCount: number;
  source: WestVirginiaArtifactCacheMetadata["source"];
};

/**
 * Shared commit path: 0-row guard, unchanged short-circuit, content-addressed
 * write, metadata pointer, sweep. `stagedPath` (bulk downloads land on disk
 * first) is moved into place; otherwise the bytes are written.
 */
async function commitArtifact(input: {
  artifact: WestVirginiaArtifactIdentity;
  cacheDir: string;
  staged: StagedArtifact;
  stagedPath?: string;
  force: boolean;
  now: Date;
}): Promise<WestVirginiaArtifactRefreshResult> {
  const paths = getWestVirginiaArtifactCachePaths({ ...input.artifact, cacheDir: input.cacheDir });
  const previous = await readWestVirginiaArtifactCacheMetadata(paths.metadataPath);
  if (previous) await chmodIfExists(previous.filePath, CACHE_FILE_MODE);
  const previousFileValid = await cachedFileMatches({ ...paths, artifact: input.artifact, metadata: previous });
  const label = `${input.artifact.kind} ${input.artifact.year}`;

  // A well-formed header with zero data rows is a truncated artifact far
  // more often than a genuinely empty year; never let it displace populated
  // data (force overrides for a deliberate accept). A rejected or unchanged
  // staged download is removed by the caller.
  if (!input.force && input.staged.recordCount === 0 && previous && previousFileValid && previous.recordCount > 0) {
    throw new Error(
      `West Virginia ${label} artifact returned 0 data rows; keeping the last good artifact ` +
        `(${previous.recordCount} rows). Pass force to accept it.`
    );
  }
  if (!input.force && previous && previousFileValid && previous.sha256 === input.staged.sha256) {
    return {
      status: "unchanged",
      cacheDir: paths.cacheDir,
      filePath: previous.filePath,
      metadataPath: paths.metadataPath,
      previous,
      current: previous,
    };
  }

  // Two-step commit: the content-addressed file lands first, then the
  // metadata pointer commits it atomically.
  const filePath = contentPath(paths.cacheDir, paths.stem, paths.extension, input.staged.sha256);
  if (input.stagedPath) {
    await rename(input.stagedPath, filePath);
    await chmod(filePath, CACHE_FILE_MODE);
  } else {
    await writeFileAtomically(filePath, input.staged.bytes);
  }
  const current: WestVirginiaArtifactCacheMetadata = {
    version: 1,
    artifact: input.artifact,
    filePath,
    metadataPath: paths.metadataPath,
    downloadedAt: input.now.toISOString(),
    source: input.staged.source,
    bytes: input.staged.bytes.byteLength,
    sha256: input.staged.sha256,
    recordCount: input.staged.recordCount,
    recoveredRowCount: input.staged.recoveredRowCount,
  };
  await writeFileAtomically(paths.metadataPath, `${JSON.stringify(current, null, 2)}\n`);

  // Best-effort sweep of superseded content versions older than
  // SWEEP_MIN_AGE_MS (a fresh one may belong to a concurrent refresh whose
  // metadata commit is about to land).
  try {
    const versionPattern = new RegExp(`^${paths.stem}\\.[a-f0-9]{12}\\.${paths.extension}$`);
    for (const name of await readdir(paths.cacheDir)) {
      if (!versionPattern.test(name) || name === basename(filePath)) continue;
      const stalePath = resolve(paths.cacheDir, name);
      const staleStat = await stat(stalePath);
      if (Date.now() - staleStat.mtimeMs >= SWEEP_MIN_AGE_MS) {
        await rm(stalePath, { force: true });
      }
    }
  } catch {
    // Stale versions are harmless; the next successful refresh sweeps again.
  }

  return { status: "downloaded", cacheDir: paths.cacheDir, filePath, metadataPath: paths.metadataPath, previous, current };
}

async function prepareCacheDir(cacheDir: string, metadataPath: string): Promise<void> {
  await mkdir(cacheDir, { recursive: true, mode: CACHE_DIRECTORY_MODE });
  // mkdir's mode is ignored when the directory already exists.
  await chmod(cacheDir, CACHE_DIRECTORY_MODE);
  await chmodIfExists(metadataPath, CACHE_FILE_MODE);
}

function requireTimestamp(now: Date | undefined): Date {
  const value = now ?? new Date();
  if (Number.isNaN(value.getTime())) {
    throw new Error("Invalid West Virginia CFRS artifact refresh timestamp");
  }
  return value;
}

/** Downloads one nightly bulk CSV through the catalog and commits it. */
export async function refreshWestVirginiaBulkArtifact(input: {
  kind: WestVirginiaBulkArtifactKind;
  year: number;
  cacheDir: string;
  /** Reuse one catalog read across several artifacts in a run. */
  catalog?: WestVirginiaDataDownloadCatalogRow[];
  force?: boolean;
  clientOptions?: WestVirginiaCfrsClientOptions;
  now?: Date;
}): Promise<WestVirginiaArtifactRefreshResult> {
  const kind = input.kind;
  const artifact: WestVirginiaArtifactIdentity = { kind, year: normalizeWestVirginiaArtifactYear(input.year) };
  const now = requireTimestamp(input.now);
  const paths = getWestVirginiaArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  await prepareCacheDir(paths.cacheDir, paths.metadataPath);

  const dataType = WEST_VIRGINIA_BULK_ARTIFACT_DATA_TYPE[kind];
  const catalog = input.catalog ?? (await getWestVirginiaDataDownloadCatalog({}, input.clientOptions));
  const entry = catalog.find((row) => row.dataType === dataType && row.year === String(artifact.year));
  if (!entry) {
    throw new Error(`West Virginia CFRS catalog has no ${dataType} ${artifact.year} entry`);
  }

  // Mint -> fetch atomically (presigned URLs expire); the body streams to a
  // staging file in the cache (0600) and is only promoted once it parses.
  const stagedPath = resolve(paths.cacheDir, `${paths.stem}.download-${process.pid}-${Date.now()}.tmp`);
  try {
    const url = await getWestVirginiaDataDownloadFileUrl(entry.id, input.clientOptions);
    const download = await downloadWestVirginiaPresignedFile({ url, outputPath: stagedPath }, input.clientOptions);
    const bytes = new Uint8Array(await readFile(stagedPath));
    const parsed = requireCleanParse(`${dataType} ${artifact.year}`, parseWestVirginiaBulkArtifactBytes(kind, bytes));
    const result = await commitArtifact({
      artifact,
      cacheDir: input.cacheDir,
      staged: {
        bytes,
        sha256: download.sha256,
        ...parsed,
        source: { catalogId: entry.id, s3ReportFilePath: entry.s3ReportFilePath, dataType },
      },
      stagedPath,
      force: input.force === true,
      now,
    });
    return result;
  } finally {
    await rm(stagedPath, { force: true }).catch(() => {});
  }
}

/** Harvests the org-101 CON transaction search for one year and commits it. */
export async function refreshWestVirginiaApiContributionsArtifact(input: {
  year: number;
  cacheDir: string;
  force?: boolean;
  clientOptions?: WestVirginiaCfrsClientOptions;
  pageSize?: number;
  now?: Date;
}): Promise<WestVirginiaArtifactRefreshResult> {
  const artifact: WestVirginiaArtifactIdentity = {
    kind: "api_contributions",
    year: normalizeWestVirginiaArtifactYear(input.year),
  };
  const now = requireTimestamp(input.now);
  const paths = getWestVirginiaArtifactCachePaths({ ...artifact, cacheDir: input.cacheDir });
  await prepareCacheDir(paths.cacheDir, paths.metadataPath);

  const request = {
    orgTypeCode: WEST_VIRGINIA_ORG_TYPE_CODES.stateCandidate,
    transactionCategory: "CON" as const,
    transactionYear: artifact.year,
  };
  const rows = await getAllWestVirginiaTransactions({ ...request, pageSize: input.pageSize ?? 5_000 }, input.clientOptions);
  rows.sort((left, right) => left.transactionID - right.transactionID);
  const body: WestVirginiaApiContributionsArtifactBody = { version: WEST_VIRGINIA_API_ARTIFACT_VERSION, request, rows };
  const bytes = new TextEncoder().encode(`${JSON.stringify(body)}\n`);
  return commitArtifact({
    artifact,
    cacheDir: input.cacheDir,
    staged: {
      bytes,
      sha256: sha256Of(bytes),
      recordCount: rows.length,
      recoveredRowCount: 0,
      source: { catalogId: null, s3ReportFilePath: null, dataType: null },
    },
    force: input.force === true,
    now,
  });
}

async function readVerifiedArtifact(input: {
  artifact: WestVirginiaArtifactIdentity;
  cacheDir: string;
}): Promise<{ metadata: WestVirginiaArtifactCacheMetadata; bytes: Uint8Array }> {
  const paths = getWestVirginiaArtifactCachePaths({ ...input.artifact, cacheDir: input.cacheDir });
  const metadata = await readWestVirginiaArtifactCacheMetadata(paths.metadataPath);
  const label = `${input.artifact.kind} ${input.artifact.year}`;
  if (!metadata) {
    throw new Error(`West Virginia CFRS artifact ${label} has no cached metadata at ${paths.metadataPath}`);
  }
  if (!(await cachedFileMatches({ ...paths, artifact: input.artifact, metadata }))) {
    throw new Error(`West Virginia CFRS artifact ${label} is missing or corrupt at ${metadata.filePath}`);
  }
  return { metadata, bytes: new Uint8Array(await readFile(metadata.filePath)) };
}

/**
 * Read one cached bulk artifact back as decoded CSV text, verifying size and
 * checksum. Throws when missing or corrupt — sync consumers fail closed.
 */
export async function readWestVirginiaBulkArtifact(input: {
  kind: WestVirginiaBulkArtifactKind;
  year: number;
  cacheDir: string;
}): Promise<{ metadata: WestVirginiaArtifactCacheMetadata; csvText: string }> {
  const { metadata, bytes } = await readVerifiedArtifact({
    artifact: { kind: input.kind, year: normalizeWestVirginiaArtifactYear(input.year) },
    cacheDir: input.cacheDir,
  });
  return { metadata, csvText: decodeWestVirginiaCsvBytes(bytes) };
}

/** Read one cached API harvest back as transaction rows (checksum-verified). */
export async function readWestVirginiaApiContributionsArtifact(input: {
  year: number;
  cacheDir: string;
}): Promise<{ metadata: WestVirginiaArtifactCacheMetadata; rows: WestVirginiaTransactionRow[] }> {
  const year = normalizeWestVirginiaArtifactYear(input.year);
  const { metadata, bytes } = await readVerifiedArtifact({
    artifact: { kind: "api_contributions", year },
    cacheDir: input.cacheDir,
  });
  return { metadata, rows: parseApiArtifactBytes(bytes, year).rows };
}
