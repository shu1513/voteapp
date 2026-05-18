import { Pool } from "pg";

import { enrichElections, buildEnrichElectionsConfigFromEnv } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload } from "./src/types/election.js";
import { getPipelineEnv } from "./src/config/env.js";
import type { AiCandidate } from "./src/ai/aiCandidates.js";

const CANDIDATES: readonly AiCandidate[] = [
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

type DistrictRow = { id: string; name: string; district_type: string; state: string };

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const rows = await pool.query<DistrictRow>(
      `SELECT id, name, district_type, state
       FROM public.districts
       WHERE (district_type='statewide' AND state='CA' AND lower(name)='california')
          OR (district_type='county' AND state='CA' AND lower(name) LIKE '%los angeles county%')
       ORDER BY district_type`
    );

    const drafts: ElectionDraftPayload[] = rows.rows.map((r) => ({
      district_id: r.id,
      district_name: r.name,
      district_type: r.district_type as ElectionDraftPayload["district_type"],
      state: r.state,
    }));

    const out: unknown[] = [];
    for (const draft of drafts) {
      for (const c of CANDIDATES) {
        const started = Date.now();
        const res = await enrichElections(
          {
            ingestKey: `matrix:${draft.district_type}:${draft.state}:${draft.district_id}:${c.provider}:${c.model}`,
            draft,
            promptVersion: ELECTION_PROMPT_VERSION,
            softRetryCount: 0,
            reviewFeedback: [],
          },
          config,
          [c]
        );
        if (!res.ok) {
          out.push({
            district_name: draft.district_name,
            district_type: draft.district_type,
            provider: c.provider,
            model: c.model,
            ok: false,
            elapsedMs: Date.now() - started,
            errorCode: res.errorCode,
            reason: res.reason,
          });
        } else {
          out.push({
            district_name: draft.district_name,
            district_type: draft.district_type,
            provider: c.provider,
            model: c.model,
            ok: true,
            elapsedMs: Date.now() - started,
            entriesCount: res.payload.entries.length,
            sampleTitles: res.payload.entries.slice(0, 3).map((e) => e.official_ballot_title),
          });
        }
      }
    }

    console.log(JSON.stringify({ type: "elections_model_matrix", out }, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
