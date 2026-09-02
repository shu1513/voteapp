// On-disk artifact for one file year of eCRIS independent-expenditure rows.
// Lives beside the receipt CSVs in the eCRIS cache directory; the due sync
// reads it and never searches eCRIS per candidate.

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import type {
  ConnecticutEcrisIndependentExpenditureFetchResult,
  ConnecticutEcrisIndependentExpenditureSearchWindow,
} from "./connecticutEcrisIndependentExpenditureClient.js";
import type { ConnecticutEcrisIndependentExpenditureRow } from "./connecticutEcrisIndependentExpenditureParsers.js";

export type ConnecticutEcrisIndependentExpenditureArtifact = {
  version: 1;
  year: number;
  fetchedAt: string;
  sourceUrl: string;
  searchWindows: ConnecticutEcrisIndependentExpenditureSearchWindow[];
  rowCount: number;
  rows: ConnecticutEcrisIndependentExpenditureRow[];
};

function normalizeYear(year: number): number {
  if (!Number.isInteger(year) || year < 2008 || year > 2100) {
    throw new Error(`Invalid Connecticut eCRIS independent expenditure year: ${year}`);
  }
  return year;
}

export function getConnecticutEcrisIndependentExpenditureCachePath(input: { cacheDir: string; year: number }): string {
  return resolve(input.cacheDir, `${normalizeYear(input.year)}_independent_expenditures.json`);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isRow(value: unknown): value is ConnecticutEcrisIndependentExpenditureRow {
  if (typeof value !== "object" || value === null) return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.rootExpenditureId === "string" &&
    typeof row.committeeName === "string" &&
    (row.formTag === null || typeof row.formTag === "string") &&
    (row.documentUrl === null || typeof row.documentUrl === "string") &&
    typeof row.reportType === "string" &&
    typeof row.documentType === "string" &&
    typeof row.payee === "string" &&
    (row.receivedDate === null || typeof row.receivedDate === "string") &&
    typeof row.fileYear === "number" &&
    (row.periodStartDate === null || typeof row.periodStartDate === "string") &&
    (row.periodEndDate === null || typeof row.periodEndDate === "string") &&
    (row.amountCents === null || Number.isSafeInteger(row.amountCents)) &&
    typeof row.formSection === "string" &&
    isStringArray(row.supportingCandidates) &&
    isStringArray(row.supportingOffices) &&
    isStringArray(row.opposingCandidates) &&
    isStringArray(row.opposingOffices) &&
    typeof row.dataSource === "string"
  );
}

export async function writeConnecticutEcrisIndependentExpenditureCache(input: {
  cacheDir: string;
  fetchResult: ConnecticutEcrisIndependentExpenditureFetchResult;
  now?: Date;
}): Promise<{ filePath: string; artifact: ConnecticutEcrisIndependentExpenditureArtifact }> {
  const fetchedAt = input.now ?? new Date();
  if (Number.isNaN(fetchedAt.getTime())) {
    throw new Error("Invalid Connecticut eCRIS independent expenditure cache timestamp");
  }
  const filePath = getConnecticutEcrisIndependentExpenditureCachePath({ cacheDir: input.cacheDir, year: input.fetchResult.year });
  const artifact: ConnecticutEcrisIndependentExpenditureArtifact = {
    version: 1,
    year: input.fetchResult.year,
    fetchedAt: fetchedAt.toISOString(),
    sourceUrl: input.fetchResult.sourceUrl,
    searchWindows: input.fetchResult.searchWindows,
    rowCount: input.fetchResult.rows.length,
    rows: input.fetchResult.rows,
  };
  await mkdir(resolve(input.cacheDir), { recursive: true });
  const tmpPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(tmpPath, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");
  await rename(tmpPath, filePath);
  return { filePath, artifact };
}

/** Null when no artifact exists for the year; a malformed artifact throws. */
export async function readConnecticutEcrisIndependentExpenditureCache(input: {
  cacheDir: string;
  year: number;
}): Promise<ConnecticutEcrisIndependentExpenditureArtifact | null> {
  const year = normalizeYear(input.year);
  const filePath = getConnecticutEcrisIndependentExpenditureCachePath({ cacheDir: input.cacheDir, year });
  let raw: string;
  try {
    raw = await readFile(filePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    throw error;
  }
  const parsed = JSON.parse(raw) as Partial<ConnecticutEcrisIndependentExpenditureArtifact>;
  if (
    parsed.version !== 1 ||
    parsed.year !== year ||
    typeof parsed.fetchedAt !== "string" ||
    typeof parsed.sourceUrl !== "string" ||
    !Array.isArray(parsed.searchWindows) ||
    !Array.isArray(parsed.rows) ||
    parsed.rowCount !== parsed.rows.length ||
    !parsed.rows.every(isRow)
  ) {
    throw new Error(`Malformed Connecticut eCRIS independent expenditure artifact: ${filePath}`);
  }
  return parsed as ConnecticutEcrisIndependentExpenditureArtifact;
}
