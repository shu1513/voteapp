import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { importHistoricalContestMarginsFromCsv } from "../pipeline/competitiveness/historicalContestCsvImport.js";
import {
  loadHistoricalContestMarginImportInput,
  parseHistoricalContestMarginImportArgs,
} from "./importHistoricalContestMarginsCli.js";

async function rollbackQuietly(client: PoolClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch (rollbackError) {
    console.error("historical contest margin import rollback failed:", rollbackError);
  }
}

async function main(): Promise<void> {
  const args = parseHistoricalContestMarginImportArgs(process.argv.slice(2));
  const input = await loadHistoricalContestMarginImportInput(args);
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = new Date();

  let client: PoolClient | undefined;
  try {
    client = await pool.connect();
    if (!args.dryRun) {
      await client.query("BEGIN");
    }

    const result = await importHistoricalContestMarginsFromCsv(client, {
      csv: input.csv,
      source: args.source,
      sourceUrl: input.sourceUrl,
      staleAfterRedistricting: args.staleAfterRedistricting,
      dryRun: args.dryRun,
      importedAt: startedAt,
    });

    if (!args.dryRun) {
      await client.query("COMMIT");
    }

    console.log(
      JSON.stringify(
        {
          type: "historical_contest_margins_import",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          input_kind: args.inputKind,
          input: input.inputLabel,
          preset: args.preset,
          source: args.source,
          source_url: input.sourceUrl,
          dry_run: args.dryRun,
          stale_after_redistricting: args.staleAfterRedistricting,
          parsed_rows: result.parsedRows,
          normalized_records: result.normalizedRecords,
          skipped_rows: result.skippedRows.length,
          rows_written: result.writeResult?.rowsWritten ?? 0,
          skipped_reasons: Object.fromEntries(
            result.skippedRows.reduce((counts, skipped) => {
              counts.set(skipped.reason, (counts.get(skipped.reason) ?? 0) + 1);
              return counts;
            }, new Map<string, number>())
          ),
        },
        null,
        2
      )
    );
  } catch (error) {
    if (client && !args.dryRun) {
      await rollbackQuietly(client);
    }
    throw error;
  } finally {
    client?.release();
    await pool.end();
  }
}

main().catch((error) => {
  console.error("historical contest margin import failed:", error);
  process.exit(1);
});
