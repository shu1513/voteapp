import { Pool } from "pg";
import { buildEnrichElectionsConfigFromEnv, enrichElections } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import { getPipelineEnv } from "./src/config/env.js";
import type { ElectionDraftPayload } from "./src/types/election.js";

const TARGETS = [
  { district_type: "statewide", state: "CA", name: "california" },
  { district_type: "county", state: "CA", name: "los angeles county" },
  { district_type: "statewide", state: "VT", name: "vermont" },
] as const;

type DistrictRow = { id: string; name: string; district_type: ElectionDraftPayload["district_type"]; state: string };

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const config = buildEnrichElectionsConfigFromEnv();

  try {
    const out: unknown[] = [];
    for (const t of TARGETS) {
      const q = await pool.query<DistrictRow>(
        `SELECT id, name, district_type, state FROM public.districts WHERE district_type=$1 AND state=$2 AND lower(name) LIKE '%' || $3 || '%' ORDER BY name LIMIT 1`,
        [t.district_type, t.state, t.name]
      );
      const row = q.rows[0];
      if (!row) {
        out.push({ target: t, ok: false, reason: "district row not found" });
        continue;
      }

      const draft: ElectionDraftPayload = {
        district_id: row.id,
        district_name: row.name,
        district_type: row.district_type,
        state: row.state,
      };

      const res = await enrichElections(
        {
          ingestKey: `openai-investigate:${row.id}`,
          draft,
          promptVersion: ELECTION_PROMPT_VERSION,
          softRetryCount: 0,
          reviewFeedback: [],
        },
        config,
        [{ provider: "openai", model: "gpt-5.4-mini" }]
      );

      if (!res.ok) {
        out.push({ target: row, ok: false, errorCode: res.errorCode, reason: res.reason, retryable: res.retryable, failureDebug: res.failureDebug ?? null });
      } else {
        out.push({
          target: row,
          ok: true,
          provider: res.provider,
          model: res.model,
          entriesCount: res.payload.entries.length,
          reviewDecision: res.payload.review_decision ?? null,
          reviewReason: res.payload.review_reason ?? null,
          entries: res.payload.entries,
          aiRawDebug: res.aiRawDebug,
        });
      }
    }

    console.log(JSON.stringify({ type: "openai_empty_investigation", out }, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
