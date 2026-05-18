import { Pool } from "pg";
import { buildEnrichElectionsConfigFromEnv, enrichElections } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import { getPipelineEnv } from "./src/config/env.js";
import type { ElectionDraftPayload } from "./src/types/election.js";

type DistrictRow = { id: string; name: string; district_type: ElectionDraftPayload["district_type"]; state: string };

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const config = buildEnrichElectionsConfigFromEnv();

  try {
    const rows = await pool.query<DistrictRow>(`
      WITH picks AS (
        SELECT DISTINCT ON (district_type)
          id, name, district_type, state
        FROM public.districts
        ORDER BY district_type, state_fips, geoid_compact
      )
      SELECT * FROM picks
      ORDER BY district_type
      LIMIT 9
    `);

    const out: Array<Record<string, unknown>> = [];
    let empty = 0;

    for (const row of rows.rows) {
      const draft: ElectionDraftPayload = {
        district_id: row.id,
        district_name: row.name,
        district_type: row.district_type,
        state: row.state,
      };
      const startedAt = Date.now();
      const res = await enrichElections(
        {
          ingestKey: `openai-empty-rate:${row.id}`,
          draft,
          promptVersion: ELECTION_PROMPT_VERSION,
          softRetryCount: 0,
          reviewFeedback: [],
        },
        config,
        [{ provider: "openai", model: "gpt-5.4-mini" }]
      );

      if (!res.ok) {
        out.push({ district_type: row.district_type, state: row.state, district_name: row.name, ok: false, errorCode: res.errorCode, reason: res.reason, elapsedMs: Date.now() - startedAt });
      } else {
        const entriesCount = res.payload.entries.length;
        if (entriesCount === 0) empty += 1;
        out.push({ district_type: row.district_type, state: row.state, district_name: row.name, ok: true, entriesCount, reviewDecision: res.payload.review_decision ?? null, reviewReason: res.payload.review_reason ?? null, elapsedMs: Date.now() - startedAt });
      }
    }

    console.log(JSON.stringify({ type: "openai_empty_rate_sample", sampleSize: out.length, emptyCount: empty, out }, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
