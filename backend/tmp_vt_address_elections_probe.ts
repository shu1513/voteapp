import { Pool } from "pg";

import { buildEnrichElectionsConfigFromEnv, enrichElections } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload } from "./src/types/election.js";
import { getPipelineEnv } from "./src/config/env.js";

const TARGET_IDS = [
  // statewide
  "9360fb02-6976-4c28-a130-5ce08e125436",
  // state_upper (Windham Senatorial District)
  "2873de4c-828b-4a5d-9de1-599db79b7183",
  // state_lower (Windham-7)
  "535bd547-5b06-47bb-ac0f-71b11a5257df",
  // us_house (VT at-large)
  "e4eb3e0e-254e-46dd-9043-a71a0fb6225e",
  // place (West Brattleboro CDP from GEOID 5078850 in your payload)
  "58cfde64-359d-4220-a698-4147edadaafe",
  // school_unified (Windham Southeast Unified Union SD 96)
  "864c53d7-ffe6-4647-8d8b-75e22a385785",
] as const;

type DistrictRow = {
  id: string;
  name: string;
  district_type: ElectionDraftPayload["district_type"];
  state: string;
};

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const rows = await pool.query<DistrictRow>(
      `SELECT id, name, district_type, state
       FROM public.districts
       WHERE id = ANY($1::uuid[])
       ORDER BY array_position($1::uuid[], id)`,
      [TARGET_IDS]
    );

    const report: unknown[] = [];
    const foundIds = new Set(rows.rows.map((r) => r.id));
    for (const id of TARGET_IDS) {
      if (!foundIds.has(id)) {
        report.push({
          district: { id },
          ok: false,
          reason: "district row not found",
        });
      }
    }

    for (const row of rows.rows) {
      const draft: ElectionDraftPayload = {
        district_id: row.id,
        district_name: row.name,
        district_type: row.district_type,
        state: row.state,
      };

      const startedAt = Date.now();
      const result = await enrichElections(
        {
          ingestKey: `vt-address-probe:${row.id}`,
          draft,
          promptVersion: ELECTION_PROMPT_VERSION,
          softRetryCount: 0,
          reviewFeedback: [],
        },
        config,
        [{ provider: "claude", model: "claude-sonnet-4-6" }]
      );

      if (!result.ok) {
        report.push({
          district: row,
          ok: false,
          elapsedMs: Date.now() - startedAt,
          errorCode: result.errorCode,
          reason: result.reason,
          retryable: result.retryable,
        });
        continue;
      }

      report.push({
        district: row,
        ok: true,
        provider: result.provider,
        model: result.model,
        elapsedMs: Date.now() - startedAt,
        entriesCount: result.payload.entries.length,
        entries: result.payload.entries,
      });
    }

    console.log(
      JSON.stringify(
        {
          type: "vt_address_elections_probe",
          expectedTargetCount: TARGET_IDS.length,
          foundTargetCount: rows.rows.length,
          total: report.length,
          report,
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
