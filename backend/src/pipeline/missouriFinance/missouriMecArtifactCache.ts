import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import {
  MISSOURI_MEC_PARSER_VERSION,
  parseMissouriMecCommitteeInfo,
  parseMissouriMecContributionExport,
  parseMissouriMecExpenditureExport,
  parseMissouriMecReportInventory,
} from "./missouriMecParsers.js";

export const DEFAULT_MISSOURI_MEC_CACHE_DIR = "scratch/missouri-campaign-finance/mec";
export const MISSOURI_MEC_ARTIFACT_SCHEMA_VERSION = 1;

export type MissouriMecArtifactType =
  | "committee_info"
  | "report_inventory"
  | "contributions"
  | "expenditures";

export type MissouriMecArtifactKey = {
  type: MissouriMecArtifactType;
  mecid: string;
  year: number;
};

export type MissouriMecArtifactManifest = {
  version: typeof MISSOURI_MEC_ARTIFACT_SCHEMA_VERSION;
  parserVersion: number;
  key: MissouriMecArtifactKey;
  sourceUrl: string;
  retrievedAt: string;
  sha256: string;
  byteSize: number;
  rowCount: number;
};

function normalizeKey(key: MissouriMecArtifactKey): MissouriMecArtifactKey {
  const mecid = key.mecid.trim().toUpperCase();
  if (!/^[A-Z]\d{6}$/.test(mecid)) throw new Error(`Invalid Missouri MEC artifact MECID: ${key.mecid}`);
  if (!Number.isSafeInteger(key.year) || key.year < 2002 || key.year > 2100) {
    throw new Error(`Invalid Missouri MEC artifact year: ${key.year}`);
  }
  return { ...key, mecid };
}

function artifactRelativePath(key: MissouriMecArtifactKey): string {
  const normalized = normalizeKey(key);
  const extension = normalized.type === "contributions" || normalized.type === "expenditures" ? "xls.html" : "html";
  return `${normalized.mecid}/${normalized.year}/${normalized.type}.${extension}`;
}

function artifactPaths(cacheDir: string, key: MissouriMecArtifactKey): { file: string; manifest: string } {
  const file = resolve(cacheDir, artifactRelativePath(key));
  return { file, manifest: `${file}.manifest.json` };
}

function validateBody(key: MissouriMecArtifactKey, body: string): number {
  switch (key.type) {
    case "committee_info":
      return parseMissouriMecCommitteeInfo(body).electionHistory.length;
    case "report_inventory":
      return parseMissouriMecReportInventory(body).length;
    case "contributions":
      return parseMissouriMecContributionExport(body).length;
    case "expenditures":
      return parseMissouriMecExpenditureExport(body).length;
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

export async function storeMissouriMecArtifact(input: {
  cacheDir?: string;
  key: MissouriMecArtifactKey;
  sourceUrl: string;
  body: string;
  retrievedAt?: Date;
}): Promise<MissouriMecArtifactManifest> {
  const key = normalizeKey(input.key);
  const retrievedAt = input.retrievedAt ?? new Date();
  if (Number.isNaN(retrievedAt.getTime())) throw new Error("Invalid Missouri MEC artifact timestamp");
  const rowCount = validateBody(key, input.body);
  const bytes = Buffer.from(input.body, "utf8");
  const paths = artifactPaths(input.cacheDir ?? DEFAULT_MISSOURI_MEC_CACHE_DIR, key);
  await mkdir(dirname(paths.file), { recursive: true, mode: 0o700 });
  await chmod(dirname(paths.file), 0o700);
  await atomicWrite(paths.file, input.body, 0o600);
  const manifest: MissouriMecArtifactManifest = {
    version: MISSOURI_MEC_ARTIFACT_SCHEMA_VERSION,
    parserVersion: MISSOURI_MEC_PARSER_VERSION,
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

function sameKey(left: MissouriMecArtifactKey, right: MissouriMecArtifactKey): boolean {
  const a = normalizeKey(left);
  const b = normalizeKey(right);
  return a.type === b.type && a.mecid === b.mecid && a.year === b.year;
}

export async function readMissouriMecArtifact(input: {
  cacheDir?: string;
  key: MissouriMecArtifactKey;
}): Promise<{ body: string; manifest: MissouriMecArtifactManifest }> {
  const key = normalizeKey(input.key);
  const paths = artifactPaths(input.cacheDir ?? DEFAULT_MISSOURI_MEC_CACHE_DIR, key);
  let body: string;
  let manifest: MissouriMecArtifactManifest;
  try {
    [body, manifest] = await Promise.all([
      readFile(paths.file, "utf8"),
      readFile(paths.manifest, "utf8").then((value) => JSON.parse(value) as MissouriMecArtifactManifest),
    ]);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Missing Missouri MEC artifact: ${key.type} ${key.mecid} ${key.year}`);
    }
    throw error;
  }
  const bytes = Buffer.from(body, "utf8");
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (
    manifest.version !== MISSOURI_MEC_ARTIFACT_SCHEMA_VERSION ||
    manifest.parserVersion !== MISSOURI_MEC_PARSER_VERSION ||
    !sameKey(manifest.key, key) ||
    manifest.sha256 !== sha256 ||
    manifest.byteSize !== bytes.byteLength ||
    manifest.rowCount !== validateBody(key, body)
  ) {
    throw new Error(`Stale or invalid Missouri MEC artifact: ${key.type} ${key.mecid} ${key.year}`);
  }
  return { body, manifest };
}

export async function readMissouriMecCandidateFinanceArtifacts(input: {
  cacheDir?: string;
  mecid: string;
  year: number;
}): Promise<{
  committeeInfo: Awaited<ReturnType<typeof parseMissouriMecCommitteeInfo>>;
  inventory: ReturnType<typeof parseMissouriMecReportInventory>;
  contributionRows: ReturnType<typeof parseMissouriMecContributionExport>;
  expenditureRows: ReturnType<typeof parseMissouriMecExpenditureExport>;
  contributionSourceUrl: string;
  expenditureSourceUrl: string;
}> {
  const types: MissouriMecArtifactType[] = ["committee_info", "report_inventory", "contributions", "expenditures"];
  const artifacts = await Promise.all(types.map((type) => readMissouriMecArtifact({ ...input, key: { type, mecid: input.mecid, year: input.year } })));
  const retrievalTimes = new Set(artifacts.map((artifact) => artifact.manifest.retrievedAt));
  if (retrievalTimes.size !== 1) {
    throw new Error(`Mixed-vintage Missouri MEC artifact bundle: ${input.mecid} ${input.year}`);
  }
  const [committeeInfo, inventory, contributions, expenditures] = artifacts;
  return {
    committeeInfo: parseMissouriMecCommitteeInfo(committeeInfo!.body),
    inventory: parseMissouriMecReportInventory(inventory!.body),
    contributionRows: parseMissouriMecContributionExport(contributions!.body),
    expenditureRows: parseMissouriMecExpenditureExport(expenditures!.body),
    contributionSourceUrl: contributions!.manifest.sourceUrl,
    expenditureSourceUrl: expenditures!.manifest.sourceUrl,
  };
}
