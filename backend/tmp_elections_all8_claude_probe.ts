import { Pool } from "pg";

import { enrichElections, buildEnrichElectionsConfigFromEnv } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload, ElectionDistrictType } from "./src/types/election.js";
import { getPipelineEnv } from "./src/config/env.js";

const CANDIDATE = { provider: "claude", model: "claude-sonnet-4-6" } as const;

type RequestedTarget = {
  label: string;
  finderSql?: string;
  finderArgs?: string[];
  syntheticDraft?: ElectionDraftPayload;
};

type DistrictRow = { id: string; name: string; district_type: ElectionDistrictType; state: string };

const TARGETS: RequestedTarget[] = [
  {
    label: "Los Angeles CA county supervisor district 1",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND lower(name) LIKE '%county supervisor district 1%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
    syntheticDraft: {
      district_id: "adhoc-los-angeles-county-supervisor-district-1",
      district_name: "Los Angeles CA county supervisor district 1",
      district_type: "county",
      state: "CA",
    },
  },
  {
    label: "Los Angeles County",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'county' AND lower(name) LIKE '%los angeles county%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California State Senate district 22",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'state_upper' AND lower(name) LIKE '%state senate district 22%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "Baldwin Park city",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'place' AND lower(name) LIKE '%baldwin park city%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California's 31st congressional district",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'us_house' AND lower(name) LIKE '%congressional district 31%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California Assembly district 48",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'state_lower' AND lower(name) LIKE '%assembly district 48%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'statewide' AND lower(name) = 'california' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "baldwin park unified",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'school_unified' AND lower(name) LIKE '%baldwin park unified%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
];

async function resolveDraft(pool: Pool, target: RequestedTarget): Promise<{ found: boolean; draft: ElectionDraftPayload; matchedName?: string }> {
  if (target.finderSql && target.finderArgs) {
    const rowResult = await pool.query<DistrictRow>(target.finderSql, target.finderArgs);
    const row = rowResult.rows[0];
    if (row) {
      return {
        found: true,
        matchedName: row.name,
        draft: {
          district_id: row.id,
          district_name: row.name,
          district_type: row.district_type,
          state: row.state,
        },
      };
    }
  }
  if (target.syntheticDraft) {
    return { found: false, draft: target.syntheticDraft };
  }
  throw new Error(`No district match: ${target.label}`);
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const out: unknown[] = [];
    for (const target of TARGETS) {
      const r = await resolveDraft(pool, target);
      const started = Date.now();
      const res = await enrichElections(
        {
          ingestKey: `all8-claude:${target.label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`,
          draft: r.draft,
          promptVersion: ELECTION_PROMPT_VERSION,
          softRetryCount: 0,
          reviewFeedback: [],
        },
        config,
        [CANDIDATE]
      );

      if (!res.ok) {
        out.push({ target: target.label, districtFoundInTable: r.found, matchedName: r.matchedName ?? null, ok: false, errorCode: res.errorCode, reason: res.reason, elapsedMs: Date.now() - started });
      } else {
        out.push({
          target: target.label,
          districtFoundInTable: r.found,
          matchedName: r.matchedName ?? null,
          ok: true,
          provider: res.provider,
          model: res.model,
          elapsedMs: Date.now() - started,
          entriesCount: res.payload.entries.length,
          entries: res.payload.entries,
        });
      }
    }

    console.log(JSON.stringify({ type: "elections_all8_claude_probe", out }, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exitCode = 1;
});
