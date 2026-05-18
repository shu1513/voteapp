import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "./src/config/env.js";
import {
  STAGING_ELECTIONS_VALIDATOR_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
} from "./src/config/electionsPipeline.js";
import { ELECTION_ENRICHMENT_SCHEMA_VERSION, ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import { runElectionsValidator } from "./src/pipeline/validators/electionsValidator.js";

const RECLAIM_MIN_IDLE_MS = 240_000;

type PendingEntry = {
  id: string;
  consumer: string;
  idleMs: number;
  deliveries: number;
};

function toReason(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, "0", { MKSTREAM: true });
  } catch (error) {
    const reason = toReason(error);
    if (!reason.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

async function listPendingForConsumer(
  redis: ReturnType<typeof createClient>,
  consumer: string
): Promise<PendingEntry[]> {
  const raw = await redis.sendCommand([
    "XPENDING",
    STAGING_PENDING_STREAM,
    STAGING_ELECTIONS_VALIDATOR_GROUP,
    "-",
    "+",
    "100",
    consumer,
  ]);
  if (!Array.isArray(raw)) {
    return [];
  }

  return raw
    .filter((row): row is Array<string | number> => Array.isArray(row) && row.length >= 4)
    .map((row) => ({
      id: String(row[0]),
      consumer: String(row[1]),
      idleMs: Number(row[2]),
      deliveries: Number(row[3]),
    }));
}

async function claimWithIdleOverride(
  redis: ReturnType<typeof createClient>,
  messageId: string,
  consumer: string,
  idleMs: number
): Promise<void> {
  await redis.sendCommand([
    "XCLAIM",
    STAGING_PENDING_STREAM,
    STAGING_ELECTIONS_VALIDATOR_GROUP,
    consumer,
    "0",
    messageId,
    "IDLE",
    String(idleMs),
  ]);
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  const runId = `validator_reclaim_sim_${new Date().toISOString()}`;
  const ingestKey = `${runId}:row`;
  const crashConsumer = `sim_crash_${Date.now()}`;

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const payload = {
      district_id: "sim-district-id",
      district_name: "Simulation District",
      district_type: "place",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Mayor",
          // Intentionally in the past so validator hard-fails this row and it never reaches writer.
          election_date: "2000-01-01",
          description: "Simulation entry for reclaim validation (expected reject).",
          race_type: "office",
          sources: ["https://example.gov/elections"],
        },
      ],
    };

    await pool.query(
      `
        INSERT INTO staging_items
          (ingest_key, item_type, payload, status, reason, run_id, model, schema_version, prompt_version, failure_debug)
        VALUES
          ($1, $2, $3::jsonb, 'pending', NULL, $4, $5, $6, $7, NULL)
        ON CONFLICT (ingest_key) DO UPDATE SET
          payload = EXCLUDED.payload,
          status = 'pending',
          reason = NULL,
          run_id = EXCLUDED.run_id,
          model = EXCLUDED.model,
          schema_version = EXCLUDED.schema_version,
          prompt_version = EXCLUDED.prompt_version,
          failure_debug = NULL,
          validated_at = NULL,
          written_at = NULL,
          updated_at = now()
      `,
      [
        ingestKey,
        STAGING_ITEM_TYPE_ELECTION,
        JSON.stringify(payload),
        runId,
        "sim:reclaim",
        ELECTION_ENRICHMENT_SCHEMA_VERSION,
        ELECTION_PROMPT_VERSION,
      ]
    );

    const streamId = await redis.xAdd(STAGING_PENDING_STREAM, "*", {
      ingest_key: ingestKey,
      item_type: STAGING_ITEM_TYPE_ELECTION,
      run_id: runId,
      payload: JSON.stringify(payload),
    });

    let deliveredId: string | null = null;
    for (let i = 0; i < 10; i += 1) {
      const batches = await redis.xReadGroup(
        STAGING_ELECTIONS_VALIDATOR_GROUP,
        crashConsumer,
        [{ key: STAGING_PENDING_STREAM, id: ">" }],
        { COUNT: 50, BLOCK: 250 }
      );
      if (!batches) {
        continue;
      }
      for (const batch of batches) {
        for (const message of batch.messages) {
          if (message.message.ingest_key === ingestKey) {
            deliveredId = message.id;
            break;
          }
        }
        if (deliveredId) {
          break;
        }
      }
      if (deliveredId) {
        break;
      }
    }

    if (!deliveredId) {
      throw new Error("failed to deliver simulation message to crash consumer");
    }

    await claimWithIdleOverride(redis, deliveredId, crashConsumer, RECLAIM_MIN_IDLE_MS + 60_000);

    const pendingBefore = await listPendingForConsumer(redis, crashConsumer);
    const beforeHasTarget = pendingBefore.some((entry) => entry.id === deliveredId);
    if (!beforeHasTarget) {
      throw new Error("simulation message not found in crash consumer PEL before validator run");
    }

    await runElectionsValidator({ once: true, batchSize: 50, blockMs: 500 });

    const statusRow = await pool.query<{ status: string; reason: string | null }>(
      `
        SELECT status, reason
        FROM staging_items
        WHERE ingest_key = $1
          AND item_type = $2
      `,
      [ingestKey, STAGING_ITEM_TYPE_ELECTION]
    );

    const pendingAfter = await listPendingForConsumer(redis, crashConsumer);
    const afterHasTarget = pendingAfter.some((entry) => entry.id === deliveredId);

    const cleanupDb = await pool.query(
      `
        DELETE FROM staging_items
        WHERE ingest_key = $1
          AND item_type = $2
      `,
      [ingestKey, STAGING_ITEM_TYPE_ELECTION]
    );
    const cleanupStream = await redis.xDel(STAGING_PENDING_STREAM, deliveredId);

    console.log(
      JSON.stringify(
        {
          ok: true,
          runId,
          ingestKey,
          streamId,
          deliveredId,
          before: {
            crashConsumer,
            pendingCount: pendingBefore.length,
            targetInPending: beforeHasTarget,
          },
          after: {
            pendingCount: pendingAfter.length,
            targetInPending: afterHasTarget,
            stagingStatus: statusRow.rows[0]?.status ?? null,
            stagingReason: statusRow.rows[0]?.reason ?? null,
          },
          cleanup: {
            stagingRowsDeleted: cleanupDb.rowCount ?? 0,
            streamEntriesDeleted: cleanupStream,
          },
        },
        null,
        2
      )
    );
  } finally {
    if (redis.isOpen) {
      await redis.quit();
    }
    await pool.end();
  }
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        ok: false,
        reason: toReason(error),
      },
      null,
      2
    )
  );
  process.exitCode = 1;
});
