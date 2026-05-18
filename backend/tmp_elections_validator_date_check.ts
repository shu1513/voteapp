import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "./src/config/env.js";
import { runElectionsValidator } from "./src/pipeline/validators/electionsValidator.js";
import {
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
} from "./src/config/electionsPipeline.js";
import { ELECTION_ENRICHMENT_SCHEMA_VERSION, ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";

function toYmd(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function plusDays(base: Date, delta: number): Date {
  const d = new Date(base);
  d.setUTCDate(d.getUTCDate() + delta);
  return d;
}

function payloadFor(ingestKey: string, electionDate: string, secondDate?: string) {
  return {
    district_id: `${ingestKey}-district`,
    district_name: "Vermont",
    district_type: "statewide",
    state: "VT",
    entries: [
      {
        official_ballot_title: "Governor",
        election_date: electionDate,
        description: "Statewide gubernatorial election.",
        race_type: "office",
        sources: ["https://www.sec.state.vt.us/elections.aspx"],
      },
      ...(secondDate
        ? [
            {
              official_ballot_title: "Lieutenant Governor",
              election_date: secondDate,
              description: "Statewide lieutenant governor election.",
              race_type: "office",
              sources: ["https://www.sec.state.vt.us/elections.aspx"],
            },
          ]
        : []),
    ],
    review_decision: "approve",
    review_reason: "scope valid",
  };
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  await redis.connect();

  const today = new Date();
  const todayYmd = toYmd(today);
  const yesterdayYmd = toYmd(plusDays(today, -1));
  const tomorrowYmd = toYmd(plusDays(today, 1));

  const runId = `validator_date_check_${new Date().toISOString()}`;

  const cases = [
    {
      key: `${runId}:past_only`,
      payload: payloadFor(`${runId}:past_only`, yesterdayYmd),
      expectStatus: "rejected",
    },
    {
      key: `${runId}:today_only`,
      payload: payloadFor(`${runId}:today_only`, todayYmd),
      expectStatus: "validated",
    },
    {
      key: `${runId}:future_only`,
      payload: payloadFor(`${runId}:future_only`, tomorrowYmd),
      expectStatus: "validated",
    },
    {
      key: `${runId}:mixed_past_future`,
      payload: payloadFor(`${runId}:mixed_past_future`, tomorrowYmd, yesterdayYmd),
      expectStatus: "rejected",
    },
  ] as const;

  try {
    for (const item of cases) {
      await pool.query(
        `
          INSERT INTO staging_items
            (ingest_key, item_type, payload, status, reason, run_id, model, schema_version, prompt_version, failure_debug, ai_raw_debug, validated_at, written_at)
          VALUES
            ($1, $2, $3::jsonb, 'pending', NULL, $4, $5, $6, $7, NULL, NULL, NULL, NULL)
          ON CONFLICT (ingest_key) DO UPDATE SET
            item_type = EXCLUDED.item_type,
            payload = EXCLUDED.payload,
            status = 'pending',
            reason = NULL,
            run_id = EXCLUDED.run_id,
            model = EXCLUDED.model,
            schema_version = EXCLUDED.schema_version,
            prompt_version = EXCLUDED.prompt_version,
            failure_debug = NULL,
            ai_raw_debug = NULL,
            validated_at = NULL,
            written_at = NULL,
            updated_at = now()
        `,
        [
          item.key,
          STAGING_ITEM_TYPE_ELECTION,
          JSON.stringify(item.payload),
          runId,
          "manual:test",
          ELECTION_ENRICHMENT_SCHEMA_VERSION,
          ELECTION_PROMPT_VERSION,
        ]
      );

      await redis.xAdd(STAGING_PENDING_STREAM, "*", {
        ingest_key: item.key,
        item_type: STAGING_ITEM_TYPE_ELECTION,
        run_id: runId,
        payload: JSON.stringify(item.payload),
      });
    }

    await runElectionsValidator({ once: true, batchSize: 50, blockMs: 1000 });

    const result = await pool.query<{
      ingest_key: string;
      status: string;
      reason: string | null;
      validated_at: string | null;
    }>(
      `
        SELECT ingest_key, status, reason, validated_at::text
        FROM staging_items
        WHERE ingest_key = ANY($1::text[])
        ORDER BY ingest_key
      `,
      [cases.map((c) => c.key)]
    );

    const byKey = new Map(result.rows.map((r) => [r.ingest_key, r]));

    const checks = cases.map((c) => {
      const row = byKey.get(c.key);
      return {
        ingest_key: c.key,
        expected: c.expectStatus,
        actual: row?.status ?? "<missing>",
        pass: row?.status === c.expectStatus,
        reason: row?.reason ?? null,
        validated_at: row?.validated_at ?? null,
      };
    });

    console.log(
      JSON.stringify(
        {
          type: "elections_validator_date_check",
          todayUtc: todayYmd,
          yesterdayUtc: yesterdayYmd,
          tomorrowUtc: tomorrowYmd,
          checks,
        },
        null,
        2
      )
    );
  } finally {
    await redis.quit();
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
