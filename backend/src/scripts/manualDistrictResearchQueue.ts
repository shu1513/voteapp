import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { readElectionsSearchCooldownDaysFromEnv } from "../pipeline/elections/electionsSearchPolicy.js";
import {
  claimNextManualDistrictResearchRequest,
  getManualDistrictResearchQueueStats,
  markManualDistrictResearchRequestFailed,
  markManualDistrictResearchRequestRunning,
  markManualDistrictResearchRequestSucceeded,
  releaseManualDistrictResearchRequest,
  releaseStaleManualDistrictResearchClaims,
  MANUAL_DISTRICT_RESEARCH_CURRENT_ELECTIONS_DEFERRAL_SQL,
  MANUAL_RESEARCH_AGENT_KINDS,
  type ManualResearchAgentKind,
} from "../pipeline/address/manualDistrictResearchRequests.js";

import { assertKnownCliFlags, type CliFlagSpec } from "./manualCliFlags.js";
const DEFAULT_MAX_CLAIM_HOURS = 6;

function usage(): string {
  return [
    "Manual district research queue CLI (agent-facing).",
    "",
    "Usage: npm run manual:district-research:<command> -- [flags]",
    "",
    "Commands:",
    "  claim     Claim the highest-priority queued request whose district is still stale",
    "            (each claim counts one attempt). --agent <name> [--agent-kind claude|codex|human|other]",
    "  start     Mark a claimed request running. --request-id <id>",
    "  complete  Mark a claimed/running request succeeded; requires this claim to",
    "            produce an elections-write stamp or future district-level elections deferral.",
    "            --request-id <id> --manifest-path <path> [--summary <text>]",
    "  fail      Mark a claimed/running request failed. --request-id <id> --error <text>",
    "  release   Return a claimed/running request to the queue. --request-id <id> [--note <text>]",
    "  sweep     Recover dead-session claims older than the hold limit. [--max-claim-hours <n>]",
    "  seed      Enqueue a request for a specific district. --district-id <id>",
    "  status    Print queue counts by status.",
  ].join("\n");
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function parseFlags(argv: readonly string[]): Map<string, string> {
  const flags = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      flags.set(key, "true");
    } else {
      flags.set(key, next);
      i += 1;
    }
  }
  return flags;
}

function requireFlag(flags: Map<string, string>, name: string): string {
  const value = flags.get(name);
  if (value === undefined || value.trim().length === 0 || value === "true") {
    throw new Error(`Missing required flag: --${name}`);
  }
  return value;
}

function print(payload: unknown): void {
  console.log(JSON.stringify(payload, null, 2));
}

async function runCommand(pool: Pool, command: string, flags: Map<string, string>): Promise<void> {
  switch (command) {
    case "claim": {
      const claimedBy = requireFlag(flags, "agent");
      const agentKindRaw = (flags.get("agent-kind") ?? "claude").toLowerCase();
      if (!MANUAL_RESEARCH_AGENT_KINDS.includes(agentKindRaw as ManualResearchAgentKind)) {
        throw new Error(
          `Invalid --agent-kind: ${agentKindRaw}. Expected one of ${MANUAL_RESEARCH_AGENT_KINDS.join(", ")}.`
        );
      }
      const claimed = await claimNextManualDistrictResearchRequest(pool, {
        claimedBy,
        agentKind: agentKindRaw as ManualResearchAgentKind,
        cooldownDays: readElectionsSearchCooldownDaysFromEnv(),
      });
      if (!claimed) {
        print({ claimed: false, reason: "no claimable requests" });
        return;
      }
      print({ claimed: true, ...claimed });
      return;
    }

    case "start": {
      const requestId = requireFlag(flags, "request-id");
      const ok = await markManualDistrictResearchRequestRunning(pool, requestId);
      if (!ok) {
        throw new Error(`Cannot start request ${requestId}: not found or not in 'claimed' status.`);
      }
      print({ request_id: requestId, status: "running" });
      return;
    }

    case "complete": {
      const requestId = requireFlag(flags, "request-id");
      const manifestPath = requireFlag(flags, "manifest-path");
      const summary = flags.get("summary") ?? null;

      // Friendly pre-check only — the domain layer enforces the same invariant.
      // Evidence must come from THIS claim: an old stamp/deferral proves nothing
      // about the current run.
      const completionCheck = await pool.query(
        `
          SELECT
            d.last_elections_searched_at,
            r.claimed_at,
            ${MANUAL_DISTRICT_RESEARCH_CURRENT_ELECTIONS_DEFERRAL_SQL} AS has_current_elections_deferral
          FROM public.manual_district_research_requests r
          JOIN public.districts d ON d.id = r.district_id
          WHERE r.id = $1
        `,
        [requestId]
      );
      if (completionCheck.rows.length === 0) {
        throw new Error(`Request ${requestId} not found.`);
      }
      const {
        last_elections_searched_at: stamp,
        claimed_at: claimedAt,
        has_current_elections_deferral: hasCurrentElectionsDeferral,
      } = completionCheck.rows[0] as {
        last_elections_searched_at: Date | null;
        claimed_at: Date | null;
        has_current_elections_deferral: boolean;
      };
      if (stamp === null && !hasCurrentElectionsDeferral) {
        throw new Error(
          `Refusing to complete request ${requestId}: this claim produced neither an elections-write stamp ` +
            "nor a future district-level elections deferral. Write/no-results the election payload, or record the deferral, before completing."
        );
      }
      if (stamp !== null && claimedAt !== null && stamp < claimedAt && !hasCurrentElectionsDeferral) {
        throw new Error(
          `Refusing to complete request ${requestId}: the district's stamp (${stamp.toISOString()}) predates ` +
            `this claim (${claimedAt.toISOString()}) — it is leftover from earlier research. ` +
            "Write/no-results the election payload, or record a new future district-level elections deferral, before completing."
        );
      }

      const ok = await markManualDistrictResearchRequestSucceeded(pool, {
        requestId,
        manifestPath,
        summary,
      });
      if (!ok) {
        throw new Error(
          `Cannot complete request ${requestId}: not found or not in 'claimed'/'running' status.`
        );
      }
      print({ request_id: requestId, status: "succeeded", manifest_path: manifestPath });
      return;
    }

    case "fail": {
      const requestId = requireFlag(flags, "request-id");
      const error = requireFlag(flags, "error");
      const ok = await markManualDistrictResearchRequestFailed(pool, { requestId, error });
      if (!ok) {
        throw new Error(`Cannot fail request ${requestId}: not found or not in 'claimed'/'running' status.`);
      }
      print({ request_id: requestId, status: "failed" });
      return;
    }

    case "release": {
      const requestId = requireFlag(flags, "request-id");
      const note = flags.get("note") ?? null;
      const ok = await releaseManualDistrictResearchRequest(pool, { requestId, note });
      if (!ok) {
        throw new Error(`Cannot release request ${requestId}: not found or not in 'claimed'/'running' status.`);
      }
      print({ request_id: requestId, status: "queued" });
      return;
    }

    case "sweep": {
      const maxClaimHoursRaw = flags.get("max-claim-hours");
      const maxClaimHours = maxClaimHoursRaw
        ? Number.parseInt(maxClaimHoursRaw, 10)
        : DEFAULT_MAX_CLAIM_HOURS;
      if (!Number.isFinite(maxClaimHours) || maxClaimHours <= 0) {
        throw new Error(`Invalid --max-claim-hours: ${maxClaimHoursRaw}. Expected a positive integer.`);
      }
      const result = await releaseStaleManualDistrictResearchClaims(pool, { maxClaimHours });
      print({ ...result, max_claim_hours: maxClaimHours });
      return;
    }

    case "seed": {
      const districtId = requireFlag(flags, "district-id");
      const districtRow = await pool.query(
        `
          SELECT
            d.id,
            d.name,
            d.district_type,
            d.state,
            d.last_elections_searched_at,
            d.canonical_district_id,
            owner.name AS canonical_name,
            owner.district_type AS canonical_district_type
          FROM public.districts AS d
          LEFT JOIN public.districts AS owner ON owner.id = d.canonical_district_id
          WHERE d.id = $1
        `,
        [districtId]
      );
      if (districtRow.rows.length === 0) {
        throw new Error(`District ${districtId} not found.`);
      }
      const district = districtRow.rows[0] as {
        id: string;
        name: string;
        district_type: string;
        state: string;
        last_elections_searched_at: string | null;
        canonical_district_id: string | null;
        canonical_name: string | null;
        canonical_district_type: string | null;
      };
      // Census lists some governments twice (a county-equivalent row and a
      // place row for the same city). Seeding the suppressed row would research
      // the same city a second time, so point the operator at the owner.
      if (district.canonical_district_id !== null) {
        throw new Error(
          `District ${districtId} (${district.name}, ${district.district_type}) is a duplicate Census row. ` +
            `Seed ${district.canonical_district_id} (${district.canonical_name}, ${district.canonical_district_type}) instead.`
        );
      }
      const seeded = await pool.query(
        `
          INSERT INTO public.manual_district_research_requests
            (district_id, district_name_snapshot, district_type_snapshot, state_snapshot,
             trigger_source, status, last_elections_searched_at_at_request)
          VALUES ($1, $2, $3, $4, 'manual_seed', 'queued', $5)
          ON CONFLICT (district_id) WHERE status IN ('queued', 'claimed', 'running')
          DO UPDATE SET
            request_count = public.manual_district_research_requests.request_count + 1,
            last_requested_at = now(),
            -- Seeding escalates an existing open request to the operator
            -- override so it bypasses the freshness gates (unlike the address
            -- trigger's bump, which preserves first-cause provenance).
            trigger_source = 'manual_seed',
            updated_at = now()
          RETURNING id, request_count
        `,
        [district.id, district.name, district.district_type, district.state, district.last_elections_searched_at]
      );
      const requestCount = Number(seeded.rows[0]?.request_count ?? 1);
      print({
        request_id: seeded.rows[0]?.id,
        district_id: district.id,
        district_name: district.name,
        enqueued: requestCount <= 1,
        bumped: requestCount > 1,
      });
      return;
    }

    case "status": {
      const stats = await getManualDistrictResearchQueueStats(pool);
      print({ queue: stats });
      return;
    }

    default:
      throw new Error(`Unknown command: ${command}\n\n${usage()}`);
  }
}

// Per-command flag sets: a union across commands would let a flag that is
// valid only for ANOTHER command pass validation and be silently ignored
// ("status --error ..." doing nothing) — the exact failure class this guard
// exists to remove. All values are space-form: parseFlags reads the next
// token and does not understand "--name=value".
const COMMAND_FLAG_SPECS: Record<string, readonly CliFlagSpec[]> = {
  claim: [
    { name: "--agent", value: "space" },
    { name: "--agent-kind", value: "space" },
  ],
  start: [{ name: "--request-id", value: "space" }],
  complete: [
    { name: "--request-id", value: "space" },
    { name: "--manifest-path", value: "space" },
    { name: "--summary", value: "space" },
  ],
  fail: [
    { name: "--request-id", value: "space" },
    { name: "--error", value: "space" },
  ],
  release: [
    { name: "--request-id", value: "space" },
    { name: "--note", value: "space" },
  ],
  sweep: [{ name: "--max-claim-hours", value: "space" }],
  seed: [{ name: "--district-id", value: "space" }],
  status: [],
};

async function main(): Promise<void> {
  const [command, ...rest] = process.argv.slice(2);
  if (!command || command === "--help" || command === "-h") {
    console.log(usage());
    return;
  }
  const specs = COMMAND_FLAG_SPECS[command];
  if (specs) {
    // Before loadProjectEnv so `--help` (handled inside the assert) stays
    // environment-independent, matching every other manual wrapper.
    assertKnownCliFlags(`manual:district-research ${command}`, rest, specs);
  }
  loadProjectEnv();

  const flags = parseFlags(rest);
  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  try {
    await runCommand(pool, command, flags);
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual district research queue command failed:", message);
  process.exitCode = 1;
});
