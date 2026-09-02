// Idaho registration artifact cache (docs/plans/idaho-finance.md, Phase 1).
//
// One JSON artifact per registration guid: the grid row plus the
// registration's contribution rows and the IE rows that target it — the raw
// evidence every snapshot is computed from. Missouri/Montana pattern: sha256
// manifest, atomic tmp-rename writes, 0700/0600 modes, gitignored scratch
// dir. Search rows carry donor names and cities, so the restricted modes are
// load-bearing and the directory is never committed.
//
// Identity is checked on store AND read: every contribution row must carry
// the key's guid as filerRegistrationGuid and every IE row must carry it as
// candidateMeasureFilerRegistrationGuid, so money can never be attributed to
// the wrong registration through a mislabelled file.

import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  normalizeIdahoRegistrationGuid,
  type IdahoCandidateRegistrationRow,
  type IdahoContributionRow,
  type IdahoIndependentExpenditureRow,
} from "./idahoCfsClient.js";

export const DEFAULT_IDAHO_FINANCE_CACHE_DIR = "scratch/idaho-campaign-finance/registrations";
export const IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION = 1;

export type IdahoRegistrationArtifact = {
  version: typeof IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION;
  registration: IdahoCandidateRegistrationRow;
  contributions: IdahoContributionRow[];
  independentExpenditures: IdahoIndependentExpenditureRow[];
};

export type IdahoRegistrationArtifactManifest = {
  version: typeof IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION;
  registrationGuid: string;
  sourceUrl: string;
  retrievedAt: string;
  sha256: string;
  byteSize: number;
  contributionCount: number;
  independentExpenditureCount: number;
};

function artifactPaths(cacheDir: string, registrationGuid: string): { file: string; manifest: string } {
  const file = resolve(cacheDir, `${registrationGuid}.json`);
  return { file, manifest: `${file}.manifest.json` };
}

function sha256Hex(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function validateArtifact(registrationGuid: string, artifact: IdahoRegistrationArtifact): void {
  if (artifact.version !== IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION) {
    throw new Error(`Unsupported Idaho registration artifact version: ${String(artifact.version)}`);
  }
  if (normalizeIdahoRegistrationGuid(artifact.registration.registrationGuid) !== registrationGuid) {
    throw new Error(
      `Idaho registration artifact ${registrationGuid} carries grid row ${artifact.registration.registrationGuid}`
    );
  }
  const foreignContribution = artifact.contributions.find(
    (row) => normalizeIdahoRegistrationGuid(row.filerRegistrationGuid) !== registrationGuid
  );
  if (foreignContribution !== undefined) {
    throw new Error(
      `Idaho registration artifact ${registrationGuid} carries contribution ${foreignContribution.transactionId} of registration ${foreignContribution.filerRegistrationGuid}`
    );
  }
  const foreignExpenditure = artifact.independentExpenditures.find(
    (row) =>
      row.candidateMeasureFilerRegistrationGuid === null ||
      normalizeIdahoRegistrationGuid(row.candidateMeasureFilerRegistrationGuid) !== registrationGuid
  );
  if (foreignExpenditure !== undefined) {
    throw new Error(
      `Idaho registration artifact ${registrationGuid} carries independent expenditure ${foreignExpenditure.guid} targeting ${foreignExpenditure.candidateMeasureFilerRegistrationGuid ?? "a name-only candidate"}`
    );
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

export async function storeIdahoRegistrationArtifact(input: {
  cacheDir?: string;
  registrationGuid: string;
  artifact: IdahoRegistrationArtifact;
  sourceUrl: string;
  retrievedAt?: Date;
}): Promise<IdahoRegistrationArtifactManifest> {
  const registrationGuid = normalizeIdahoRegistrationGuid(input.registrationGuid);
  const retrievedAt = input.retrievedAt ?? new Date();
  if (Number.isNaN(retrievedAt.getTime())) {
    throw new Error("Invalid Idaho registration artifact timestamp");
  }
  validateArtifact(registrationGuid, input.artifact);
  const body = `${JSON.stringify(input.artifact, null, 2)}\n`;
  const bytes = Buffer.from(body, "utf8");
  const cacheDir = resolve(input.cacheDir ?? DEFAULT_IDAHO_FINANCE_CACHE_DIR);
  const paths = artifactPaths(cacheDir, registrationGuid);
  await mkdir(cacheDir, { recursive: true, mode: 0o700 });
  // mkdir's mode is ignored when the directory already exists.
  await chmod(cacheDir, 0o700);
  await atomicWrite(paths.file, body, 0o600);
  const manifest: IdahoRegistrationArtifactManifest = {
    version: IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION,
    registrationGuid,
    sourceUrl: input.sourceUrl,
    retrievedAt: retrievedAt.toISOString(),
    sha256: sha256Hex(bytes),
    byteSize: bytes.byteLength,
    contributionCount: input.artifact.contributions.length,
    independentExpenditureCount: input.artifact.independentExpenditures.length,
  };
  await atomicWrite(paths.manifest, `${JSON.stringify(manifest, null, 2)}\n`, 0o600);
  return manifest;
}

/** Read one registration's artifact; the manifest sha256 and identity are verified before it is returned. */
export async function readIdahoRegistrationArtifact(input: {
  cacheDir?: string;
  registrationGuid: string;
}): Promise<{ artifact: IdahoRegistrationArtifact; manifest: IdahoRegistrationArtifactManifest }> {
  const registrationGuid = normalizeIdahoRegistrationGuid(input.registrationGuid);
  const paths = artifactPaths(resolve(input.cacheDir ?? DEFAULT_IDAHO_FINANCE_CACHE_DIR), registrationGuid);
  let bytes: Buffer;
  let manifest: IdahoRegistrationArtifactManifest;
  try {
    [bytes, manifest] = await Promise.all([
      readFile(paths.file),
      readFile(paths.manifest, "utf8").then((value) => JSON.parse(value) as IdahoRegistrationArtifactManifest),
    ]);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Missing Idaho registration artifact: ${registrationGuid}`);
    }
    throw error;
  }
  if (
    manifest.version !== IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION ||
    manifest.registrationGuid !== registrationGuid ||
    manifest.byteSize !== bytes.byteLength ||
    manifest.sha256 !== sha256Hex(bytes)
  ) {
    throw new Error(`Corrupt Idaho registration artifact: ${registrationGuid}`);
  }
  const artifact = JSON.parse(bytes.toString("utf8")) as IdahoRegistrationArtifact;
  validateArtifact(registrationGuid, artifact);
  return { artifact, manifest };
}
