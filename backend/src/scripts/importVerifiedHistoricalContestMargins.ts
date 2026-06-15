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

type SourceImportSummary = {
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
  dryRun: boolean;
  importedAt: Date;
}): Promise<VerifiedImportResult> {
  const options = {
    csv: input.csv,
    source: input.source.source,
    sourceUrl: input.source.sourceUrl,
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

async function importVerifiedSource(input: {
  db: Pick<PoolClient, "query">;
  source: HistoricalContestSourceDefinition;
  csv: string;
  dryRun: boolean;
  importedAt: Date;
}): Promise<SourceImportSummary> {
  const result = await importVerifiedSourceByFormat({
    db: input.db,
    source: input.source,
    csv: input.csv,
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

async function importVerifiedSourceWithTransaction(input: {
  pool: Pool | null;
  source: HistoricalContestSourceDefinition;
  dryRun: boolean;
  importedAt: Date;
}): Promise<SourceImportSummary> {
  const csv = await fetchHistoricalContestCsv(input.source.sourceUrl);
  let client: PoolClient | undefined;
  try {
    if (input.pool) {
      client = await input.pool.connect();
      await client.query("BEGIN");
    }

    const summary = await importVerifiedSource({
      db: client ?? dryRunDb,
      source: input.source,
      csv,
      dryRun: input.dryRun,
      importedAt: input.importedAt,
    });

    if (client) {
      await client.query("COMMIT");
    }

    return summary;
  } catch (error) {
    if (client) {
      await rollbackQuietly(client, input.source.preset);
    }
    throw error;
  } finally {
    client?.release();
  }
}

async function main(): Promise<void> {
  const args = parseVerifiedHistoricalContestMarginImportArgs(process.argv.slice(2));
  const env = args.dryRun ? null : getPipelineEnv();
  const pool = env ? new Pool({ connectionString: env.DATABASE_URL }) : null;
  const startedAt = new Date();
  const summaries: SourceImportSummary[] = [];

  try {
    for (const source of VERIFIED_HISTORICAL_CONTEST_SOURCES) {
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
