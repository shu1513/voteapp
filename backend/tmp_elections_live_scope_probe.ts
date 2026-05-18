import { Pool } from "pg";

import { enrichElections, buildEnrichElectionsConfigFromEnv } from "./src/ai/enrichElections.js";
import { ELECTION_PROMPT_VERSION } from "./src/contracts/electionEnrichmentContract.js";
import type { ElectionDraftPayload, ElectionDistrictType } from "./src/types/election.js";
import { getPipelineEnv } from "./src/config/env.js";

type RequestedTarget = {
  label: string;
  expectedType: ElectionDistrictType;
  state: string;
  finderSql?: string;
  finderArgs?: string[];
  syntheticDraft?: ElectionDraftPayload;
};

type DistrictRow = {
  id: string;
  name: string;
  district_type: ElectionDistrictType;
  state: string;
};

const TARGETS: RequestedTarget[] = [
  {
    label: "Los Angeles CA county supervisor district 1",
    expectedType: "county",
    state: "CA",
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
    expectedType: "county",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'county' AND lower(name) LIKE '%los angeles county%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California State Senate district 22",
    expectedType: "state_upper",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'state_upper' AND lower(name) LIKE '%state senate district 22%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "Baldwin Park city",
    expectedType: "place",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'place' AND lower(name) LIKE '%baldwin park city%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California's 31st congressional district",
    expectedType: "us_house",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'us_house' AND lower(name) LIKE '%congressional district 31%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California Assembly district 48",
    expectedType: "state_lower",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'state_lower' AND lower(name) LIKE '%assembly district 48%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "California (statewide)",
    expectedType: "statewide",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'statewide' AND lower(name) = 'california' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
  {
    label: "Baldwin Park Unified",
    expectedType: "school_unified",
    state: "CA",
    finderSql:
      "SELECT id, name, district_type, state FROM public.districts WHERE state = $1 AND district_type = 'school_unified' AND lower(name) LIKE '%baldwin park unified%' ORDER BY name LIMIT 1",
    finderArgs: ["CA"],
  },
];

async function resolveDraft(pool: Pool, target: RequestedTarget): Promise<{ found: boolean; draft: ElectionDraftPayload; matchedName?: string; matchedType?: string }> {
  if (target.finderSql && target.finderArgs) {
    const rowResult = await pool.query<DistrictRow>(target.finderSql, target.finderArgs);
    const row = rowResult.rows[0];
    if (row) {
      return {
        found: true,
        matchedName: row.name,
        matchedType: row.district_type,
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

  throw new Error(`No district match and no synthetic fallback for target: ${target.label}`);
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const config = buildEnrichElectionsConfigFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = Date.now();

  try {
    const report: unknown[] = [];

    for (const target of TARGETS) {
      const resolved = await resolveDraft(pool, target);
      const callStartedAt = Date.now();
      const ingestKey = `manual-probe:${target.label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;

      const result = await enrichElections(
        {
          ingestKey,
          draft: resolved.draft,
          promptVersion: ELECTION_PROMPT_VERSION,
          softRetryCount: 0,
          reviewFeedback: [],
        },
        config
      );

      if (!result.ok) {
        report.push({
          target: target.label,
          expectedType: target.expectedType,
          districtFoundInTable: resolved.found,
          matchedName: resolved.matchedName ?? null,
          matchedType: resolved.matchedType ?? null,
          elapsedMs: Date.now() - callStartedAt,
          ok: false,
          reason: result.reason,
          errorCode: result.errorCode,
          retryable: result.retryable,
          failureDebug: result.failureDebug ?? null,
        });
        continue;
      }

      report.push({
        target: target.label,
        expectedType: target.expectedType,
        districtFoundInTable: resolved.found,
        matchedName: resolved.matchedName ?? null,
        matchedType: resolved.matchedType ?? null,
        elapsedMs: Date.now() - callStartedAt,
        ok: true,
        provider: result.provider,
        model: result.model,
        entriesCount: result.payload.entries.length,
        entries: result.payload.entries,
      });
    }

    console.log(
      JSON.stringify(
        {
          type: "elections_live_scope_probe",
          totalTargets: TARGETS.length,
          elapsedMs: Date.now() - startedAt,
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
