import { Pool } from "pg";
import { buildEnrichElectionsConfigFromEnv, enrichElections } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import { getPipelineEnv } from "./src/config/env.js";
import type { ElectionDraftPayload } from "./src/types/election.js";

const MODELS = ["gpt-5.4-mini", "gpt-5.2", "gpt-5.5"] as const;

type DistrictRow = { id: string; name: string; district_type: ElectionDraftPayload["district_type"]; state: string };

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const config = buildEnrichElectionsConfigFromEnv();

  try {
    const rows = await pool.query<DistrictRow>(
      `SELECT id, name, district_type, state
       FROM public.districts
       WHERE (district_type='statewide' AND state='VT' AND lower(name)='vermont')
          OR (district_type='county' AND state='CA' AND lower(name) LIKE '%los angeles county%')
       ORDER BY district_type`
    );

    const out: unknown[] = [];
    for (const row of rows.rows) {
      for (const model of MODELS) {
        const draft: ElectionDraftPayload = {
          district_id: row.id,
          district_name: row.name,
          district_type: row.district_type,
          state: row.state,
        };
        const started = Date.now();
        const res = await enrichElections(
          {
            ingestKey: `openai-model-compare:${row.id}:${model}`,
            draft,
            promptVersion: ELECTION_PROMPT_VERSION,
            softRetryCount: 0,
            reviewFeedback: [],
          },
          config,
          [{ provider: "openai", model }]
        );

        if (!res.ok) {
          out.push({ district_name: row.name, district_type: row.district_type, model, ok: false, errorCode: res.errorCode, reason: res.reason, elapsedMs: Date.now() - started });
        } else {
          out.push({ district_name: row.name, district_type: row.district_type, model, ok: true, entriesCount: res.payload.entries.length, reviewDecision: res.payload.review_decision ?? null, reviewReason: res.payload.review_reason ?? null, elapsedMs: Date.now() - started });
        }
      }
    }

    console.log(JSON.stringify({ type: "openai_model_compare_small", out }, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
