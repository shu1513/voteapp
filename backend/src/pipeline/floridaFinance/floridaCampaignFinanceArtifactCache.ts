import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import {
  type FloridaContributionExportRowsResult,
  type NormalizedFloridaContributionExportQuery,
} from "./floridaCampaignFinanceClient.js";
import { parseFloridaContributionTsv, type FloridaContributionRow } from "./floridaCampaignFinanceRows.js";

export const DEFAULT_FLORIDA_CAMPAIGN_FINANCE_CACHE_DIR = "scratch/florida-campaign-finance/dos";

export type FloridaContributionExportArtifactMetadata = {
  version: 1;
  cacheKey: string;
  tsvPath: string;
  metadataPath: string;
  request: NormalizedFloridaContributionExportQuery;
  exportUrl: string;
  sourceUrl: string;
  retrievedAt: string;
  rowCount: number;
  formData: Record<string, string>;
};

export type FloridaContributionExportArtifactPaths = {
  cacheDir: string;
  cacheKey: string;
  tsvPath: string;
  metadataPath: string;
};

export type FloridaContributionExportArtifact = {
  metadata: FloridaContributionExportArtifactMetadata;
  tsv: string;
  rows: FloridaContributionRow[];
};

function validateFloridaContributionExportCacheKey(cacheKey: string): string {
  if (!/^[a-z0-9][a-z0-9._-]{0,179}$/.test(cacheKey)) {
    throw new Error(`Invalid Florida contribution export cache key: ${cacheKey}`);
  }
  return cacheKey;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

export function getFloridaContributionExportArtifactPaths(input: {
  cacheDir: string;
  cacheKey: string;
}): FloridaContributionExportArtifactPaths {
  const cacheKey = validateFloridaContributionExportCacheKey(input.cacheKey);
  const cacheDir = resolve(input.cacheDir);
  return {
    cacheDir,
    cacheKey,
    tsvPath: resolve(cacheDir, `${cacheKey}.tsv`),
    metadataPath: resolve(cacheDir, `${cacheKey}.metadata.json`),
  };
}

export async function readFloridaContributionExportArtifactMetadata(
  metadataPath: string
): Promise<FloridaContributionExportArtifactMetadata | null> {
  try {
    const parsed = JSON.parse(await readFile(metadataPath, "utf8")) as Partial<
      FloridaContributionExportArtifactMetadata
    >;
    if (
      parsed.version !== 1 ||
      typeof parsed.cacheKey !== "string" ||
      typeof parsed.tsvPath !== "string" ||
      typeof parsed.metadataPath !== "string" ||
      typeof parsed.exportUrl !== "string" ||
      typeof parsed.sourceUrl !== "string" ||
      typeof parsed.retrievedAt !== "string" ||
      typeof parsed.rowCount !== "number" ||
      typeof parsed.request?.searchType !== "string" ||
      typeof parsed.request?.rowLimit !== "number" ||
      !parsed.formData ||
      typeof parsed.formData !== "object" ||
      Array.isArray(parsed.formData)
    ) {
      return null;
    }
    validateFloridaContributionExportCacheKey(parsed.cacheKey);
    return parsed as FloridaContributionExportArtifactMetadata;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    console.warn(`Failed to read Florida contribution export metadata at ${metadataPath}`, error);
    return null;
  }
}

export async function writeFloridaContributionExportArtifact(input: {
  cacheDir: string;
  result: FloridaContributionExportRowsResult;
}): Promise<FloridaContributionExportArtifactMetadata> {
  const paths = getFloridaContributionExportArtifactPaths({
    cacheDir: input.cacheDir,
    cacheKey: input.result.cacheKey,
  });
  if (Number.isNaN(input.result.retrievedAt.getTime())) {
    throw new Error("Invalid Florida contribution export artifact retrieval timestamp");
  }

  await mkdir(paths.cacheDir, { recursive: true });
  await writeFile(paths.tsvPath, input.result.tsv, "utf8");
  const metadata: FloridaContributionExportArtifactMetadata = {
    version: 1,
    cacheKey: paths.cacheKey,
    tsvPath: paths.tsvPath,
    metadataPath: paths.metadataPath,
    request: input.result.query,
    exportUrl: input.result.exportUrl,
    sourceUrl: input.result.sourceUrl,
    retrievedAt: input.result.retrievedAt.toISOString(),
    rowCount: input.result.rowCount,
    formData: input.result.formData,
  };
  await writeFile(paths.metadataPath, `${JSON.stringify(metadata, null, 2)}\n`, "utf8");
  return metadata;
}

export async function readFloridaContributionExportArtifact(input: {
  cacheDir: string;
  cacheKey: string;
}): Promise<FloridaContributionExportArtifact | null> {
  const paths = getFloridaContributionExportArtifactPaths(input);
  const metadata = await readFloridaContributionExportArtifactMetadata(paths.metadataPath);
  if (!metadata || !(await pathExists(paths.tsvPath))) {
    return null;
  }

  const tsv = await readFile(paths.tsvPath, "utf8");
  const rows = parseFloridaContributionTsv(tsv, {
    electionCode: metadata.request.electionCode ?? undefined,
    sourceUrl: metadata.sourceUrl,
  });
  return {
    metadata,
    tsv,
    rows,
  };
}
