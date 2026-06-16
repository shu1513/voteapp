import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { importHistoricalContestMarginsFromCsv } from "../pipeline/competitiveness/historicalContestCsvImport.js";
import { importHistoricalContestMarginsFromPrecinctCsv } from "../pipeline/competitiveness/historicalContestPrecinctCsvImport.js";
import {
  VERIFIED_HISTORICAL_CONTEST_SOURCES,
  type HistoricalContestSourceDefinition,
} from "../pipeline/competitiveness/historicalContestSources.js";
import { fetchHistoricalContestCsv } from "./importHistoricalContestMarginsCli.js";
import { parseVerifiedHistoricalContestMarginImportArgs } from "./importVerifiedHistoricalContestMarginsCli.js";

const dryRunDb: Pick<PoolClient, "query"> = {
  query: async () => {
    throw new Error("dry-run verified historical contest import should not execute database queries");
  },
};

export type VerifiedHistoricalContestSourceImportSummary = {
  preset: string;
  source: string;
  source_url: string;
  format: string;
  election_year: number;
  office_types: readonly string[];
  stale_after_redistricting: boolean;
  parsed_rows: number;
  aggregated_rows: number | null;
  normalized_records: number;
  skipped_rows: number;
  rows_written: number;
  skipped_reasons: Record<string, number>;
};

type DataverseDatasetFile = {
  url: string;
  label: string;
};

const DATAVERSE_DATASET_FETCH_TIMEOUT_MS = 30_000;

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function countSkippedReasons(
  skippedRows: readonly { reason: string }[]
): Record<string, number> {
  return Object.fromEntries(
    skippedRows.reduce((counts, skipped) => {
      counts.set(skipped.reason, (counts.get(skipped.reason) ?? 0) + 1);
      return counts;
    }, new Map<string, number>())
  );
}

type VerifiedImportResult = Awaited<ReturnType<typeof importHistoricalContestMarginsFromCsv>> & {
  aggregatedRows?: number;
};

async function importVerifiedSourceByFormat(input: {
  db: Pick<PoolClient, "query">;
  source: HistoricalContestSourceDefinition;
  csv: string;
  sourceUrl: string;
  dryRun: boolean;
  importedAt: Date;
}): Promise<VerifiedImportResult> {
  const options = {
    csv: input.csv,
    source: input.source.source,
    sourceUrl: input.sourceUrl,
    officeTypes: input.source.officeTypes,
    staleAfterRedistricting: input.source.staleAfterRedistricting,
    dryRun: input.dryRun,
    importedAt: input.importedAt,
  };

  if (input.source.format === "medsl_precinct_csv") {
    return await importHistoricalContestMarginsFromPrecinctCsv(input.db, options);
  }

  return await importHistoricalContestMarginsFromCsv(input.db, options);
}

async function rollbackQuietly(client: PoolClient, preset: string): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch (rollbackError) {
    console.error(`verified historical contest import rollback failed for ${preset}:`, rollbackError);
  }
}

export async function importVerifiedHistoricalContestSource(input: {
  db: Pick<PoolClient, "query">;
  source: HistoricalContestSourceDefinition;
  csv: string;
  sourceUrl: string;
  dryRun: boolean;
  importedAt: Date;
}): Promise<VerifiedHistoricalContestSourceImportSummary> {
  const result = await importVerifiedSourceByFormat({
    db: input.db,
    source: input.source,
    csv: input.csv,
    sourceUrl: input.sourceUrl,
    dryRun: input.dryRun,
    importedAt: input.importedAt,
  });

  return {
    preset: input.source.preset,
    source: input.source.source,
    source_url: input.source.sourceUrl,
    format: input.source.format,
    election_year: input.source.electionYear,
    office_types: input.source.officeTypes,
    stale_after_redistricting: input.source.staleAfterRedistricting,
    parsed_rows: result.parsedRows,
    aggregated_rows: result.aggregatedRows ?? null,
    normalized_records: result.normalizedRecords,
    skipped_rows: result.skippedRows.length,
    rows_written: result.writeResult?.rowsWritten ?? 0,
    skipped_reasons: countSkippedReasons(result.skippedRows),
  };
}

function mergeSourceSummaries(
  summaries: readonly VerifiedHistoricalContestSourceImportSummary[]
): VerifiedHistoricalContestSourceImportSummary {
  const [first] = summaries;
  if (!first) {
    throw new Error("Cannot merge empty historical contest source summaries");
  }

  return {
    ...first,
    parsed_rows: summaries.reduce((sum, summary) => sum + summary.parsed_rows, 0),
    aggregated_rows: summaries.some((summary) => summary.aggregated_rows !== null)
      ? summaries.reduce((sum, summary) => sum + (summary.aggregated_rows ?? 0), 0)
      : null,
    normalized_records: summaries.reduce((sum, summary) => sum + summary.normalized_records, 0),
    skipped_rows: summaries.reduce((sum, summary) => sum + summary.skipped_rows, 0),
    rows_written: summaries.reduce((sum, summary) => sum + summary.rows_written, 0),
    skipped_reasons: summaries.reduce<Record<string, number>>((counts, summary) => {
      for (const [reason, count] of Object.entries(summary.skipped_reasons)) {
        counts[reason] = (counts[reason] ?? 0) + count;
      }
      return counts;
    }, {}),
  };
}

function isHistoricalContestDataFile(label: string): boolean {
  return /\.(csv|tsv|tab)$/i.test(label.trim());
}

function normalizeDataverseDataFileUrl(fileId: unknown): string | null {
  const parsed = typeof fileId === "number" ? fileId : typeof fileId === "string" ? Number(fileId) : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    return null;
  }
  return `https://dataverse.harvard.edu/api/access/datafile/${parsed}`;
}

async function loadDataverseDatasetFiles(persistentId: string): Promise<DataverseDatasetFile[]> {
  const url = new URL("https://dataverse.harvard.edu/api/datasets/:persistentId");
  url.searchParams.set("persistentId", persistentId);
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), DATAVERSE_DATASET_FETCH_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(url, {
      headers: {
        accept: "application/json",
      },
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(
        `Failed to load Dataverse dataset ${persistentId}: request timed out after ${DATAVERSE_DATASET_FETCH_TIMEOUT_MS}ms`
      );
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
  if (!response.ok) {
    throw new Error(`Failed to load Dataverse dataset ${persistentId}: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as {
    data?: {
      latestVersion?: {
        files?: Array<{
          label?: unknown;
          dataFile?: {
            id?: unknown;
          };
        }>;
      };
    };
  };

  return (payload.data?.latestVersion?.files ?? []).flatMap((file): DataverseDatasetFile[] => {
    const label = typeof file.label === "string" ? file.label : "";
    const url = normalizeDataverseDataFileUrl(file.dataFile?.id);
    return url && isHistoricalContestDataFile(label) ? [{ url, label }] : [];
  });
}

async function resolveSourceFileUrls(source: HistoricalContestSourceDefinition): Promise<string[]> {
  if (source.sourceFiles?.length) {
    return [...source.sourceFiles];
  }

  const discovery = source.sourceFileDiscovery;
  if (!discovery) {
    return [source.sourceUrl];
  }

  const files = (
    await Promise.all(discovery.dataverseDatasetPersistentIds.map((persistentId) => loadDataverseDatasetFiles(persistentId)))
  ).flat();
  if (files.length === 0) {
    throw new Error(`No Dataverse source files found for ${source.preset}`);
  }
  return files
    .sort((left, right) => left.label.localeCompare(right.label))
    .map((file) => file.url);
}

async function importVerifiedSourceWithTransaction(input: {
  pool: Pool | null;
  source: HistoricalContestSourceDefinition;
  dryRun: boolean;
  importedAt: Date;
}): Promise<VerifiedHistoricalContestSourceImportSummary> {
  const sourceFiles = await resolveSourceFileUrls(input.source);
  const summaries: VerifiedHistoricalContestSourceImportSummary[] = [];

  for (const sourceUrl of sourceFiles) {
    const csv = await fetchHistoricalContestCsv(sourceUrl, {
      downloadMode: input.source.downloadMode,
    });
    let client: PoolClient | undefined;
    try {
      if (input.pool) {
        client = await input.pool.connect();
        await client.query("BEGIN");
      }

      const summary = await importVerifiedHistoricalContestSource({
        db: client ?? dryRunDb,
        source: input.source,
        csv,
        sourceUrl,
        dryRun: input.dryRun,
        importedAt: input.importedAt,
      });

      if (client) {
        await client.query("COMMIT");
      }

      summaries.push(summary);
    } catch (error) {
      if (client) {
        await rollbackQuietly(client, input.source.preset);
      }
      throw error;
    } finally {
      client?.release();
    }
  }

  return mergeSourceSummaries(summaries);
}

async function main(): Promise<void> {
  const args = parseVerifiedHistoricalContestMarginImportArgs(process.argv.slice(2));
  const env = args.dryRun ? null : getPipelineEnv();
  const pool = env ? new Pool({ connectionString: env.DATABASE_URL }) : null;
  const startedAt = new Date();
  const summaries: VerifiedHistoricalContestSourceImportSummary[] = [];
  const sources = args.preset
    ? VERIFIED_HISTORICAL_CONTEST_SOURCES.filter((source) => source.preset === args.preset)
    : VERIFIED_HISTORICAL_CONTEST_SOURCES;

  try {
    for (const source of sources) {
      summaries.push(
        await importVerifiedSourceWithTransaction({
          pool,
          source,
          dryRun: args.dryRun,
          importedAt: startedAt,
        })
      );
    }

    console.log(
      JSON.stringify(
        {
          type: "verified_historical_contest_margins_import",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          dry_run: args.dryRun,
          source_count: summaries.length,
          parsed_rows: summaries.reduce((sum, summary) => sum + summary.parsed_rows, 0),
          aggregated_rows: summaries.reduce((sum, summary) => sum + (summary.aggregated_rows ?? 0), 0),
          normalized_records: summaries.reduce((sum, summary) => sum + summary.normalized_records, 0),
          skipped_rows: summaries.reduce((sum, summary) => sum + summary.skipped_rows, 0),
          rows_written: summaries.reduce((sum, summary) => sum + summary.rows_written, 0),
          sources: summaries,
        },
        null,
        2
      )
    );
  } finally {
    await pool?.end();
  }
}

main().catch((error) => {
  console.error("verified historical contest margin import failed:", error);
  process.exit(1);
});
