import { stat } from "node:fs/promises";

import {
  CAL_ACCESS_RAW_DATA_ZIP_URL,
  DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR,
  getCalAccessRawDataArtifactCachePaths,
  readCalAccessRawDataArtifactCacheMetadata,
} from "./calAccessRawDataArtifactCache.js";
import { CAL_ACCESS_RAW_DATA_TABLE_MANIFEST } from "./calAccessRawDataManifest.js";
import {
  readCalAccessRawDataTableRows,
  type CalAccessRawDataRow,
} from "./calAccessRawDataProbe.js";
import type {
  CalAccessCampaignCoverRow,
  CalAccessFilerNameRow,
} from "./californiaCandidateCommitteeResolver.js";
import type { CalAccessReceiptRow } from "./californiaDirectContributionAggregator.js";

export type CalAccessCommitteeResolutionData = {
  zipPath: string;
  sourceUrl: string;
  campaignCoverRows: CalAccessCampaignCoverRow[];
  filerNameRows: CalAccessFilerNameRow[];
};

export type CalAccessCommitteeReceiptData = {
  zipPath: string;
  sourceUrl: string;
  receiptRowsByCommitteeId: Map<string, CalAccessReceiptRow[]>;
  controlledCommitteeFilingIdsByCommitteeId: Map<string, string[]>;
};

function tableFileName(key: "campaign_cover" | "filer_names" | "receipts"): string {
  const entry = CAL_ACCESS_RAW_DATA_TABLE_MANIFEST.find((candidate) => candidate.key === key);
  if (!entry) {
    throw new Error(`CAL-ACCESS raw data table manifest missing ${key}`);
  }
  return entry.fileName;
}

async function fileExists(path: string): Promise<boolean> {
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

function normalizeId(value: string | null | undefined): string {
  return value?.trim().toUpperCase() ?? "";
}

function rowValue(row: CalAccessRawDataRow, key: string): string {
  return row[key]?.trim() ?? "";
}

function committeeIdFromCoverRow(row: CalAccessCampaignCoverRow): string {
  return normalizeId(row.FILER_ID || row.CMTTE_ID);
}

function filingIdFromCoverRow(row: CalAccessCampaignCoverRow): string {
  return normalizeId(row.FILING_ID);
}

async function resolveCachedZip(input?: {
  zipPath?: string;
  cacheDir?: string;
}): Promise<{ zipPath: string; sourceUrl: string } | null> {
  if (input?.zipPath) {
    return {
      zipPath: input.zipPath,
      sourceUrl: CAL_ACCESS_RAW_DATA_ZIP_URL,
    };
  }

  const paths = getCalAccessRawDataArtifactCachePaths(
    input?.cacheDir ?? process.env.CAL_ACCESS_RAW_DATA_CACHE_DIR?.trim() ?? DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR
  );
  if (!(await fileExists(paths.zipPath))) {
    return null;
  }
  const metadata = await readCalAccessRawDataArtifactCacheMetadata(paths.metadataPath);
  return {
    zipPath: paths.zipPath,
    sourceUrl: metadata?.remote.url ?? CAL_ACCESS_RAW_DATA_ZIP_URL,
  };
}

export async function loadCalAccessCommitteeResolutionData(input?: {
  zipPath?: string;
  cacheDir?: string;
}): Promise<CalAccessCommitteeResolutionData | null> {
  const resolved = await resolveCachedZip(input);
  if (!resolved) {
    return null;
  }

  const [campaignCoverRows, filerNameRows] = await Promise.all([
    readCalAccessRawDataTableRows({
      zipPath: resolved.zipPath,
      fileName: tableFileName("campaign_cover"),
    }) as Promise<CalAccessCampaignCoverRow[]>,
    readCalAccessRawDataTableRows({
      zipPath: resolved.zipPath,
      fileName: tableFileName("filer_names"),
    }) as Promise<CalAccessFilerNameRow[]>,
  ]);

  return {
    zipPath: resolved.zipPath,
    sourceUrl: resolved.sourceUrl,
    campaignCoverRows,
    filerNameRows,
  };
}

export async function loadCalAccessReceiptRowsForCommittees(input: {
  zipPath?: string;
  cacheDir?: string;
  sourceUrl?: string;
  committeeIds: readonly string[];
  campaignCoverRows: readonly CalAccessCampaignCoverRow[];
}): Promise<CalAccessCommitteeReceiptData | null> {
  const committeeIds = new Set(input.committeeIds.map(normalizeId).filter(Boolean));
  if (committeeIds.size === 0) {
    return null;
  }

  const resolved = await resolveCachedZip(input);
  if (!resolved) {
    return null;
  }

  const filingIdsByCommitteeId = new Map<string, Set<string>>();
  const committeeByFilingId = new Map<string, string>();
  for (const row of input.campaignCoverRows) {
    const committeeId = committeeIdFromCoverRow(row);
    if (!committeeIds.has(committeeId)) {
      continue;
    }
    const filingId = filingIdFromCoverRow(row);
    if (!filingId) {
      continue;
    }
    const filingIds = filingIdsByCommitteeId.get(committeeId) ?? new Set<string>();
    filingIds.add(filingId);
    filingIdsByCommitteeId.set(committeeId, filingIds);
    committeeByFilingId.set(filingId, committeeId);
  }

  const receiptRowsByCommitteeId = new Map<string, CalAccessReceiptRow[]>();
  const receiptRows = await readCalAccessRawDataTableRows({
    zipPath: resolved.zipPath,
    fileName: tableFileName("receipts"),
    predicate: (row) => {
      const committeeId = normalizeId(rowValue(row, "CMTE_ID"));
      if (committeeIds.has(committeeId)) {
        return true;
      }
      const filingId = normalizeId(rowValue(row, "FILING_ID"));
      return committeeByFilingId.has(filingId);
    },
  });

  for (const row of receiptRows) {
    const committeeId = normalizeId(rowValue(row, "CMTE_ID"));
    const filingId = normalizeId(rowValue(row, "FILING_ID"));
    const owningCommitteeId = committeeIds.has(committeeId) ? committeeId : committeeByFilingId.get(filingId);
    if (!owningCommitteeId) {
      continue;
    }
    const rows = receiptRowsByCommitteeId.get(owningCommitteeId) ?? [];
    rows.push(row);
    receiptRowsByCommitteeId.set(owningCommitteeId, rows);
  }

  return {
    zipPath: resolved.zipPath,
    sourceUrl: input.sourceUrl ?? resolved.sourceUrl,
    receiptRowsByCommitteeId,
    controlledCommitteeFilingIdsByCommitteeId: new Map(
      [...filingIdsByCommitteeId.entries()].map(([committeeId, filingIds]) => [committeeId, [...filingIds]])
    ),
  };
}
