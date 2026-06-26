import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import {
  buildUtahGenerateReportUrl,
  fetchUtahGeneratedReportCsv,
  parseUtahDisclosuresTransactionRows,
  UTAH_DISCLOSURES_ENTITY_TYPES,
  type UtahDisclosuresClientOptions,
  type UtahDisclosuresEntityType,
  type UtahDisclosuresGenerateReportInput,
  type UtahDisclosuresTransactionRow,
} from "./utahDisclosuresClient.js";

export const DEFAULT_UTAH_DISCLOSURES_CSV_CACHE_DIR = "scratch/utah-campaign-finance/disclosures";

export type UtahDisclosuresCsvCacheInput = UtahDisclosuresGenerateReportInput & {
  cacheDir?: string;
};

export type UtahDisclosuresCachedCsv = {
  csv: string;
  cachePath: string;
  sourceUrl: string;
  cacheHit: boolean;
};

export type UtahDisclosuresCachedRows = {
  rows: UtahDisclosuresTransactionRow[];
  cachePath: string;
  sourceUrl: string;
  cacheHit: boolean;
};

const ENTITY_TYPES = new Set<string>(UTAH_DISCLOSURES_ENTITY_TYPES);

function normalizeReportYear(value: number): number {
  if (!Number.isInteger(value) || value < 1998 || value > 2100) {
    throw new Error(`Invalid Utah disclosures cache report year: ${value}`);
  }
  return value;
}

function normalizeEntityType(value: UtahDisclosuresEntityType | null | undefined): UtahDisclosuresEntityType {
  if (value && ENTITY_TYPES.has(value)) {
    return value;
  }
  throw new Error(`Invalid Utah disclosures cache entity type: ${value ?? ""}`);
}

function normalizeFolderId(value: string | number | null | undefined): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const folderId = String(value).trim();
  if (!/^\d+$/.test(folderId)) {
    throw new Error(`Invalid Utah disclosures cache folder id: ${folderId}`);
  }
  return folderId;
}

function normalizeCacheDir(value: string | undefined): string {
  const cacheDir = value?.trim() || process.env.UTAH_DISCLOSURES_CSV_CACHE_DIR?.trim();
  return cacheDir || DEFAULT_UTAH_DISCLOSURES_CSV_CACHE_DIR;
}

export function getUtahDisclosuresCsvCachePath(input: UtahDisclosuresCsvCacheInput): string {
  const reportYear = normalizeReportYear(input.reportYear);
  const folderId = normalizeFolderId(input.folderId);
  const fileName = folderId ? `folder-${folderId}.csv` : `entity-${normalizeEntityType(input.entityType)}.csv`;
  return join(normalizeCacheDir(input.cacheDir), String(reportYear), fileName);
}

async function readCachedCsv(cachePath: string): Promise<string | null> {
  try {
    return await readFile(cachePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function writeCachedCsv(cachePath: string, csv: string): Promise<void> {
  await mkdir(dirname(cachePath), { recursive: true });
  await writeFile(cachePath, csv, "utf8");
}

export async function downloadUtahGeneratedReportCsvWithCache(
  input: UtahDisclosuresCsvCacheInput,
  options: UtahDisclosuresClientOptions & { refreshCache?: boolean } = {}
): Promise<UtahDisclosuresCachedCsv> {
  const cachePath = getUtahDisclosuresCsvCachePath(input);
  const sourceUrl = buildUtahGenerateReportUrl(input, options.baseUrl);

  if (options.refreshCache !== true) {
    const cached = await readCachedCsv(cachePath);
    if (cached !== null) {
      return {
        csv: cached,
        cachePath,
        sourceUrl,
        cacheHit: true,
      };
    }
  }

  const csv = await fetchUtahGeneratedReportCsv(input, options);
  await writeCachedCsv(cachePath, csv);
  return {
    csv,
    cachePath,
    sourceUrl,
    cacheHit: false,
  };
}

export async function downloadUtahGeneratedReportRowsWithCache(
  input: UtahDisclosuresCsvCacheInput,
  options: UtahDisclosuresClientOptions & { refreshCache?: boolean } = {}
): Promise<UtahDisclosuresCachedRows> {
  const result = await downloadUtahGeneratedReportCsvWithCache(input, options);
  return {
    rows: parseUtahDisclosuresTransactionRows(result.csv),
    cachePath: result.cachePath,
    sourceUrl: result.sourceUrl,
    cacheHit: result.cacheHit,
  };
}
