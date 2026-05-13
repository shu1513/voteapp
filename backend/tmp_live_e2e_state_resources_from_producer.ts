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
    const produced = await runStateResourcesProducer({ force: true, dryRun: false });
    const runId = produced.runId;
    const targetCount = produced.enqueued;
    if (targetCount <= 0) {
      throw new Error(`Producer run ${runId} did not enqueue any items`);
    }

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
    let converged = false;
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
        converged = true;
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

    const finalDone =
      finalStatus.written === targetCount &&
      finalStatus.pending === 0 &&
      finalStatus.validated === 0 &&
      finalStatus.failed === 0 &&
      finalStatus.rejected === 0 &&
      finalStatus.requeueing === 0;

    if (!converged || !finalDone) {
      throw new Error(
        `Pipeline did not converge within ${maxRounds} rounds for run_id=${runId}. Final status=${JSON.stringify(finalStatus)}`
      );
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
