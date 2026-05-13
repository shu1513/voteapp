import { Pool } from 'pg';

import { getPipelineEnv } from './src/config/env.ts';
import { runStateResourcesProducer } from './src/pipeline/producers/stateResourcesProducer.ts';
import { runStateResourcesEnricher } from './src/pipeline/enrichers/stateResourcesEnricher.ts';
import { runStateResourcesValidator } from './src/pipeline/validators/stateResourcesValidator.ts';
import { runStateResourcesWriter } from './src/pipeline/writers/stateResourcesWriter.ts';
import { runStateResourcesRetrySweeper } from './src/pipeline/retries/stateResourcesRetry.ts';

type StatusCounts = {
  pending: number;
  validated: number;
  written: number;
  failed: number;
  rejected: number;
  requeueing: number;
  raw: Record<string, number>;
};

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = Date.now();

  try {
    await runStateResourcesProducer({ force: true, dryRun: false });

    const runRow = await pool.query<{ run_id: string; total: string }>(
      `
      SELECT run_id, COUNT(*)::text AS total
      FROM staging_items
      WHERE item_type = 'state_resources'
        AND run_id LIKE 'state_resources_%'
      GROUP BY run_id
      ORDER BY MAX(updated_at) DESC
      LIMIT 1
      `
    );

    const runId = runRow.rows[0]?.run_id;
    const runCount = Number.parseInt(runRow.rows[0]?.total ?? '0', 10);
    if (!runId) {
      throw new Error('Unable to determine producer run_id from staging_items');
    }

    const targetCount = runCount;

    const fetchStatus = async (): Promise<StatusCounts> => {
      const result = await pool.query<{ status: string; count: string }>(
        `
        SELECT status, COUNT(*)::text AS count
        FROM staging_items
        WHERE item_type = 'state_resources'
          AND run_id = $1
        GROUP BY status
        ORDER BY status
        `,
        [runId]
      );

      const map = new Map<string, number>();
      for (const row of result.rows) {
        map.set(row.status, Number.parseInt(row.count, 10));
      }

      return {
        pending: map.get('pending') ?? 0,
        validated: map.get('validated') ?? 0,
        written: map.get('written') ?? 0,
        failed: map.get('failed') ?? 0,
        rejected: map.get('rejected') ?? 0,
        requeueing: map.get('requeueing') ?? 0,
        raw: Object.fromEntries(map.entries()),
      };
    };

    console.log(
      JSON.stringify({
        type: 'producer_seeded',
        runId,
        targetCount,
        totalElapsedMs: Date.now() - startedAt,
      })
    );

    const maxRounds = 20;
    for (let round = 1; round <= maxRounds; round += 1) {
      const roundStartedAt = Date.now();

      await runStateResourcesEnricher({ once: true, batchSize: 100, blockMs: 1000 });
      await runStateResourcesValidator({ once: true, batchSize: 100, blockMs: 1000 });
      await runStateResourcesWriter({ once: true, batchSize: 100, blockMs: 1000 });

      const status = await fetchStatus();
      const done =
        status.written === targetCount &&
        status.pending === 0 &&
        status.validated === 0 &&
        status.failed === 0 &&
        status.rejected === 0 &&
        status.requeueing === 0;

      console.log(
        JSON.stringify({
          type: 'round_status',
          runId,
          round,
          roundElapsedMs: Date.now() - roundStartedAt,
          totalElapsedMs: Date.now() - startedAt,
          status,
        })
      );

      if (done) {
        break;
      }

      if (status.failed > 0 || status.rejected > 0 || status.requeueing > 0) {
        const retry = await runStateResourcesRetrySweeper({ maxItems: 500 });
        console.log(JSON.stringify({ type: 'retry_sweep', runId, round, retry }));
      }
    }

    const finalStatus = await fetchStatus();
    const modelUsage = await pool.query<{ model: string | null; count: string }>(
      `
      SELECT model, COUNT(*)::text AS count
      FROM staging_items
      WHERE item_type = 'state_resources'
        AND run_id = $1
      GROUP BY model
      ORDER BY COUNT(*) DESC
      `,
      [runId]
    );

    console.log(
      JSON.stringify({
        type: 'final_status',
        runId,
        totalElapsedMs: Date.now() - startedAt,
        finalStatus,
        modelUsage: modelUsage.rows.map((r) => ({ model: r.model, count: Number.parseInt(r.count, 10) })),
      })
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
