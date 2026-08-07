import { createHash } from "node:crypto";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import {
  parseNcsbeCommitteeSearchPage,
  parseNcsbeDocumentListPage,
  parseNcsbeExpendituresPage,
  parseNcsbeReceiptsPage,
  parseNcsbeReportDetailPage,
  NCSBE_PARSER_VERSION,
} from "./northCarolinaNcsbeParsers.js";

// Artifact cache for NCSBE portal fetches. Retrieval and parsing stay
// separate (north_carolina_plan.md decision 10): the acquisition hands each
// fetched body to this module, which re-validates it against the pinned
// parser for its artifact type, hashes it, and atomically installs it with a
// manifest (decision 15). The finance sync reads the cache only and never
// touches the portal. Unlike Ohio's eleven bulk products, NC artifacts are
// many small per-report fetches, so the cache is keyed by artifact type +
// portal ids rather than a fixed product list.

export const DEFAULT_NCSBE_CACHE_DIR = "scratch/north-carolina-campaign-finance/ncsbe";

// Bumped whenever a manifest field changes, so a stale snapshot is
// re-validated rather than trusted.
export const NCSBE_ARTIFACT_SCHEMA_VERSION = 1;

export type NcsbeArtifactKey =
  | { type: "committee_search"; query: string }
  | { type: "document_inventory"; sboeId: string }
  | { type: "ie_doc_type_inventory"; year: number }
  | { type: "report_cover"; reportId: string }
  | { type: "report_transactions"; reportId: string; kind: "receipts" | "expenditures"; page: number };

function slugifyNcsbeQuery(query: string): string {
  const slug = query
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (slug.length === 0) {
    throw new Error(`NCSBE committee-search query has no cacheable slug: ${JSON.stringify(query)}`);
  }
  return slug;
}

function requireSafePathSegment(value: string, label: string): string {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value)) {
    throw new Error(`NCSBE ${label} is not filesystem-safe: ${JSON.stringify(value)}`);
  }
  return value;
}

// Relative artifact path under the cache directory. Every id that reaches a
// path is validated — SBoEIDs and report ids come from parsed portal data,
// but a path traversal must be structurally impossible anyway.
export function ncsbeArtifactRelativePath(key: NcsbeArtifactKey): string {
  switch (key.type) {
    case "committee_search":
      return `committee-search/${slugifyNcsbeQuery(key.query)}.html`;
    case "document_inventory":
      return `document-inventory/${requireSafePathSegment(key.sboeId, "SBoEID")}.html`;
    case "ie_doc_type_inventory":
      if (!Number.isInteger(key.year) || key.year < 1990 || key.year > 2100) {
        throw new Error(`NCSBE ie_doc_type_inventory year is invalid: ${key.year}`);
      }
      return `ie-doc-type-inventory/${key.year}.html`;
    case "report_cover":
      return `report/${requireSafePathSegment(key.reportId, "report id")}/cover.html`;
    case "report_transactions": {
      if (!Number.isInteger(key.page) || key.page < 0) {
        throw new Error(`NCSBE report_transactions page is invalid: ${key.page}`);
      }
      const kind = key.kind === "receipts" ? "receipts" : "expenditures";
      return `report/${requireSafePathSegment(key.reportId, "report id")}/${kind}-p${key.page}.json`;
    }
  }
}

export type NcsbeArtifactPaths = {
  cacheDir: string;
  filePath: string;
  manifestPath: string;
};

export function getNcsbeArtifactPaths(input: { cacheDir: string; key: NcsbeArtifactKey }): NcsbeArtifactPaths {
  const cacheDir = resolve(input.cacheDir);
  const filePath = resolve(cacheDir, ncsbeArtifactRelativePath(input.key));
  return { cacheDir, filePath, manifestPath: `${filePath}.manifest.json` };
}

// Inventory-row metadata recorded with a report artifact (decision 15:
// amendment flag + import dates ride in the manifest). Raw portal strings —
// interpretation happens at selection time, not here.
export type NcsbeSourceDocumentMetadata = {
  committeeName: string;
  sboeId: string | null;
  reportYear: number;
  documentType: string;
  reportType: string | null;
  isAmendment: boolean | null;
  imageReceiptDate: string;
  dataImportDate: string;
  periodStartDate: string;
  periodEndDate: string;
};

export type NcsbeArtifactManifest = {
  version: typeof NCSBE_ARTIFACT_SCHEMA_VERSION;
  parserVersion: number;
  key: NcsbeArtifactKey;
  // Full route + query the body was fetched from (decision 15).
  url: string;
  filePath: string;
  manifestPath: string;
  retrievedAt: string;
  sha256: string;
  byteSize: number;
  // Rows validated in this artifact: search rows, inventory rows, summary
  // sections for covers, transaction rows for a transaction page.
  rowCount: number;
  // Transaction pages only: the report-level total this page's fetch loop
  // reconciled against (decision 9).
  recordCountKey: number | null;
  sourceDocument: NcsbeSourceDocumentMetadata | null;
};

// Re-validates the body against the pinned parser for its artifact type and
// returns the validated row count. A body that stopped matching the pinned
// shape can never be installed.
export function validateNcsbeArtifactBody(
  key: NcsbeArtifactKey,
  body: string
): { rowCount: number; recordCountKey: number | null } {
  switch (key.type) {
    case "committee_search":
      return { rowCount: parseNcsbeCommitteeSearchPage(body).length, recordCountKey: null };
    case "document_inventory":
    case "ie_doc_type_inventory":
      return { rowCount: parseNcsbeDocumentListPage(body).length, recordCountKey: null };
    case "report_cover":
      return { rowCount: parseNcsbeReportDetailPage(body).summarySections.length, recordCountKey: null };
    case "report_transactions": {
      const page =
        key.kind === "receipts" ? parseNcsbeReceiptsPage(body) : parseNcsbeExpendituresPage(body);
      return { rowCount: page.rows.length, recordCountKey: page.recordCount };
    }
  }
}

export function hashNcsbeArtifactBody(body: string): { sha256: string; byteSize: number } {
  const buffer = Buffer.from(body, "utf8");
  return { sha256: createHash("sha256").update(buffer).digest("hex"), byteSize: buffer.byteLength };
}

async function writeFileAtomically(path: string, content: string): Promise<void> {
  const tmpPath = `${path}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(tmpPath, content, "utf8");
  try {
    await rename(tmpPath, path);
  } catch (error) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw error;
  }
}

// Validates the body, then atomically installs bytes + manifest. Validation
// runs before anything is replaced, so a bad fetch can never destroy a good
// snapshot.
export async function storeNcsbeArtifact(input: {
  cacheDir: string;
  key: NcsbeArtifactKey;
  url: string;
  body: string;
  sourceDocument?: NcsbeSourceDocumentMetadata | null;
  retrievedAt?: Date;
}): Promise<NcsbeArtifactManifest> {
  const retrievedAt = input.retrievedAt ?? new Date();
  if (Number.isNaN(retrievedAt.getTime())) {
    throw new Error("Invalid NCSBE artifact retrieval timestamp");
  }
  const { rowCount, recordCountKey } = validateNcsbeArtifactBody(input.key, input.body);
  const { sha256, byteSize } = hashNcsbeArtifactBody(input.body);
  const paths = getNcsbeArtifactPaths(input);

  await mkdir(dirname(paths.filePath), { recursive: true });
  await writeFileAtomically(paths.filePath, input.body);

  const manifest: NcsbeArtifactManifest = {
    version: NCSBE_ARTIFACT_SCHEMA_VERSION,
    parserVersion: NCSBE_PARSER_VERSION,
    key: input.key,
    url: input.url,
    filePath: paths.filePath,
    manifestPath: paths.manifestPath,
    retrievedAt: retrievedAt.toISOString(),
    sha256,
    byteSize,
    rowCount,
    recordCountKey,
    sourceDocument: input.sourceDocument ?? null,
  };
  // Two renames, so a crash between them leaves new bytes with the old
  // manifest; the size check in getNcsbeArtifactStatus surfaces that as
  // "stale" (same trade-off as the Ohio cache, and these files are small).
  await writeFileAtomically(paths.manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  return manifest;
}

export async function readNcsbeArtifactManifest(manifestPath: string): Promise<NcsbeArtifactManifest | null> {
  try {
    const parsed = JSON.parse(await readFile(manifestPath, "utf8")) as Partial<NcsbeArtifactManifest>;
    if (
      parsed.version !== NCSBE_ARTIFACT_SCHEMA_VERSION ||
      typeof parsed.parserVersion !== "number" ||
      typeof parsed.url !== "string" ||
      typeof parsed.sha256 !== "string" ||
      typeof parsed.byteSize !== "number" ||
      typeof parsed.rowCount !== "number" ||
      typeof parsed.retrievedAt !== "string" ||
      typeof parsed.key !== "object" ||
      parsed.key === null
    ) {
      return null;
    }
    return parsed as NcsbeArtifactManifest;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Unexpected error reading NCSBE artifact manifest at ${manifestPath}:`, error);
    return null;
  }
}

export type NcsbeArtifactStatus = {
  key: NcsbeArtifactKey;
  filePath: string;
  // "ready" — file and manifest agree and were validated by the current
  // parser; "stale" — bytes and manifest disagree, or the artifact was
  // validated by an older parser version; "missing" — not fetched yet.
  status: "ready" | "stale" | "missing";
  manifest: NcsbeArtifactManifest | null;
};

export async function getNcsbeArtifactStatus(input: {
  cacheDir: string;
  key: NcsbeArtifactKey;
}): Promise<NcsbeArtifactStatus> {
  const paths = getNcsbeArtifactPaths(input);
  const manifest = await readNcsbeArtifactManifest(paths.manifestPath);
  let fileStat: Awaited<ReturnType<typeof stat>> | null = null;
  try {
    fileStat = await stat(paths.filePath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }
  const base = { key: input.key, filePath: paths.filePath };
  if (!manifest || !fileStat?.isFile()) {
    return { ...base, status: "missing", manifest };
  }
  if (fileStat.size !== manifest.byteSize || manifest.parserVersion !== NCSBE_PARSER_VERSION) {
    return { ...base, status: "stale", manifest };
  }
  return { ...base, status: "ready", manifest };
}

// Reads a cached artifact for the sync. Fail-closed: a missing or stale
// artifact throws — the sync must never aggregate from bytes the manifest no
// longer vouches for.
export async function readNcsbeArtifact(input: {
  cacheDir: string;
  key: NcsbeArtifactKey;
}): Promise<{ body: string; manifest: NcsbeArtifactManifest }> {
  const status = await getNcsbeArtifactStatus(input);
  if (status.status !== "ready" || !status.manifest) {
    throw new Error(`NCSBE artifact ${ncsbeArtifactRelativePath(input.key)} is ${status.status}`);
  }
  return { body: await readFile(status.filePath, "utf8"), manifest: status.manifest };
}
