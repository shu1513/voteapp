// Montana CERS artifact cache (docs/plans/montana-finance.md, Phase 1).
// Copies the Missouri MEC cache pattern: content-addressed artifacts
// (sha256 + manifest, atomic tmp-rename writes, 0700/0600 modes), parser
// validation on both store and read, and mixed-vintage detection on
// bundles. Artifacts carry donor PII (street addresses) — restricted modes
// are load-bearing, and the cache dir is gitignored
// (scratch/montana-campaign-finance/).

import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import {
  MONTANA_CERS_PARSER_VERSION,
  parseMontanaCersContributionExport,
  parseMontanaCersExpenditureExport,
  parseMontanaCersReportDetailArtifact,
  parseMontanaCersReportInventory,
} from "./montanaCersParsers.js";

export const DEFAULT_MONTANA_CERS_CACHE_DIR = "scratch/montana-campaign-finance/cers";
export const MONTANA_CERS_ARTIFACT_SCHEMA_VERSION = 1;

export type MontanaCersArtifactType =
  | "report_inventory"
  | "report_detail"
  | "contributions_export"
  | "expenditures_export";

export type MontanaCersArtifactKey =
  | { type: "report_inventory" | "contributions_export" | "expenditures_export"; candidateId: number; year: number }
  | { type: "report_detail"; candidateId: number; year: number; reportId: number };

export type MontanaCersArtifactManifest = {
  version: typeof MONTANA_CERS_ARTIFACT_SCHEMA_VERSION;
  parserVersion: number;
  key: MontanaCersArtifactKey;
  sourceUrl: string;
  retrievedAt: string;
  sha256: string;
  byteSize: number;
  rowCount: number;
};

function requirePositiveInt(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid Montana CERS artifact ${label}: ${value}`);
  }
}

function normalizeKey(key: MontanaCersArtifactKey): MontanaCersArtifactKey {
  requirePositiveInt(key.candidateId, "candidateId");
  if (!Number.isSafeInteger(key.year) || key.year < 2020 || key.year > 2100) {
    throw new Error(`Invalid Montana CERS artifact year: ${key.year}`);
  }
  if (key.type === "report_detail") {
    requirePositiveInt(key.reportId, "reportId");
  }
  return key;
}

function artifactRelativePath(key: MontanaCersArtifactKey): string {
  const normalized = normalizeKey(key);
  if (normalized.type === "report_detail") {
    return `${normalized.candidateId}/${normalized.year}/report_detail_${normalized.reportId}.json`;
  }
  const extension = normalized.type === "report_inventory" ? "json" : "csv";
  return `${normalized.candidateId}/${normalized.year}/${normalized.type}.${extension}`;
}

function artifactPaths(cacheDir: string, key: MontanaCersArtifactKey): { file: string; manifest: string } {
  const file = resolve(cacheDir, artifactRelativePath(key));
  return { file, manifest: `${file}.manifest.json` };
}

function artifactKeyLabel(key: MontanaCersArtifactKey): string {
  return key.type === "report_detail"
    ? `${key.type} ${key.candidateId} ${key.year} report ${key.reportId}`
    : `${key.type} ${key.candidateId} ${key.year}`;
}

/**
 * Parses AND identity-checks an artifact body against its cache key. CERS's
 * documented stale-session behavior can serve the PREVIOUS entity's data,
 * so every row that names its entity must name the key's candidate —
 * otherwise money would be attributed to the wrong candidate. (Report
 * detail rows carry no entity id; their identity is pinned transitively:
 * the reportId is checked here and the report->candidate binding comes
 * from the identity-checked inventory.)
 */
function validateBody(key: MontanaCersArtifactKey, body: string): number {
  switch (key.type) {
    case "report_inventory": {
      const rows = parseMontanaCersReportInventory(body);
      const foreign = rows.find((row) => row.entitySubId !== key.candidateId);
      if (foreign !== undefined) {
        throw new Error(
          `Montana CERS report inventory row for entity ${foreign.entitySubId} under candidate ${key.candidateId} — stale-session cross-entity data`
        );
      }
      return rows.length;
    }
    case "report_detail": {
      const artifact = parseMontanaCersReportDetailArtifact(body);
      if (artifact.reportId !== key.reportId) {
        throw new Error(
          `Montana CERS report detail artifact is for report ${artifact.reportId}, expected ${key.reportId}`
        );
      }
      return Object.values(artifact.lists).reduce((sum, rows) => sum + rows.length, 0);
    }
    case "contributions_export":
    case "expenditures_export": {
      const rows =
        key.type === "contributions_export"
          ? parseMontanaCersContributionExport(body)
          : parseMontanaCersExpenditureExport(body);
      const foreign = rows.find((row) => row.candidateId !== key.candidateId);
      if (foreign !== undefined) {
        throw new Error(
          `Montana CERS ${key.type} row for candidate ${foreign.candidateId} under candidate ${key.candidateId} — stale-session cross-entity data`
        );
      }
      return rows.length;
    }
  }
}

async function atomicWrite(path: string, body: string, mode: number): Promise<void> {
  const temporary = `${path}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(temporary, body, { encoding: "utf8", mode });
  try {
    await rename(temporary, path);
    await chmod(path, mode);
  } catch (error) {
    await rm(temporary, { force: true }).catch(() => {});
    throw error;
  }
}

export async function storeMontanaCersArtifact(input: {
  cacheDir?: string;
  key: MontanaCersArtifactKey;
  sourceUrl: string;
  body: string;
  retrievedAt?: Date;
}): Promise<MontanaCersArtifactManifest> {
  const key = normalizeKey(input.key);
  const retrievedAt = input.retrievedAt ?? new Date();
  if (Number.isNaN(retrievedAt.getTime())) {
    throw new Error("Invalid Montana CERS artifact timestamp");
  }
  const rowCount = validateBody(key, input.body);
  const bytes = Buffer.from(input.body, "utf8");
  const paths = artifactPaths(input.cacheDir ?? DEFAULT_MONTANA_CERS_CACHE_DIR, key);
  await mkdir(dirname(paths.file), { recursive: true, mode: 0o700 });
  await chmod(dirname(paths.file), 0o700);
  await atomicWrite(paths.file, input.body, 0o600);
  const manifest: MontanaCersArtifactManifest = {
    version: MONTANA_CERS_ARTIFACT_SCHEMA_VERSION,
    parserVersion: MONTANA_CERS_PARSER_VERSION,
    key,
    sourceUrl: input.sourceUrl,
    retrievedAt: retrievedAt.toISOString(),
    sha256: createHash("sha256").update(bytes).digest("hex"),
    byteSize: bytes.byteLength,
    rowCount,
  };
  await atomicWrite(paths.manifest, `${JSON.stringify(manifest, null, 2)}\n`, 0o600);
  return manifest;
}

function sameKey(left: MontanaCersArtifactKey, right: MontanaCersArtifactKey): boolean {
  const a = normalizeKey(left);
  const b = normalizeKey(right);
  if (a.type !== b.type || a.candidateId !== b.candidateId || a.year !== b.year) {
    return false;
  }
  if (a.type === "report_detail") {
    return b.type === "report_detail" && a.reportId === b.reportId;
  }
  return true;
}

export async function readMontanaCersArtifact(input: {
  cacheDir?: string;
  key: MontanaCersArtifactKey;
}): Promise<{ body: string; manifest: MontanaCersArtifactManifest }> {
  const key = normalizeKey(input.key);
  const paths = artifactPaths(input.cacheDir ?? DEFAULT_MONTANA_CERS_CACHE_DIR, key);
  let body: string;
  let manifest: MontanaCersArtifactManifest;
  try {
    [body, manifest] = await Promise.all([
      readFile(paths.file, "utf8"),
      readFile(paths.manifest, "utf8").then((value) => JSON.parse(value) as MontanaCersArtifactManifest),
    ]);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Missing Montana CERS artifact: ${artifactKeyLabel(key)}`);
    }
    throw error;
  }
  const bytes = Buffer.from(body, "utf8");
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (
    manifest.version !== MONTANA_CERS_ARTIFACT_SCHEMA_VERSION ||
    manifest.parserVersion !== MONTANA_CERS_PARSER_VERSION ||
    !sameKey(manifest.key, key) ||
    manifest.sha256 !== sha256 ||
    manifest.byteSize !== bytes.byteLength ||
    manifest.rowCount !== validateBody(key, body)
  ) {
    throw new Error(`Stale or invalid Montana CERS artifact: ${artifactKeyLabel(key)}`);
  }
  return { body, manifest };
}

/**
 * Reads the candidate-scope artifact bundle for one sync pass: report
 * inventory + both exports (the per-report detail artifacts are read
 * separately, keyed by report id). All three must come from the same
 * harvest run — mixed vintages mean a partial refresh and fail closed.
 */
export async function readMontanaCersCandidateFinanceArtifacts(input: {
  cacheDir?: string;
  candidateId: number;
  year: number;
}): Promise<{
  inventory: ReturnType<typeof parseMontanaCersReportInventory>;
  contributionRows: ReturnType<typeof parseMontanaCersContributionExport>;
  expenditureRows: ReturnType<typeof parseMontanaCersExpenditureExport>;
  contributionSourceUrl: string;
  inventorySourceUrl: string;
}> {
  const types = ["report_inventory", "contributions_export", "expenditures_export"] as const;
  const artifacts = await Promise.all(
    types.map((type) =>
      readMontanaCersArtifact({
        ...input,
        key: { type, candidateId: input.candidateId, year: input.year },
      })
    )
  );
  const retrievalTimes = new Set(artifacts.map((artifact) => artifact.manifest.retrievedAt));
  if (retrievalTimes.size !== 1) {
    throw new Error(`Mixed-vintage Montana CERS artifact bundle: ${input.candidateId} ${input.year}`);
  }
  const [inventory, contributions, expenditures] = artifacts;
  return {
    inventory: parseMontanaCersReportInventory(inventory!.body),
    contributionRows: parseMontanaCersContributionExport(contributions!.body),
    expenditureRows: parseMontanaCersExpenditureExport(expenditures!.body),
    contributionSourceUrl: contributions!.manifest.sourceUrl,
    inventorySourceUrl: inventory!.manifest.sourceUrl,
  };
}
