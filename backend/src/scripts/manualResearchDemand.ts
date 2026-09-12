import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  DEFAULT_MANUAL_RESEARCH_DEMAND_HORIZON_DAYS,
  listManualResearchDemand,
  MANUAL_RESEARCH_DEMAND_STAGES,
  pruneManualResearchDemand,
  type ManualResearchDemandStage,
} from "../pipeline/address/manualResearchDemand.js";
import { readStrictFlagValue, readStrictPositiveIntegerFlag } from "../utils/cliFlags.js";
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import { assertKnownCliFlags, type CliFlagSpec } from "./manualCliFlags.js";

// Read side of the research demand ledger (migration 279). Address lookups
// count every open gap on the ballot they produce; this CLI lists those gaps
// hottest first so a session works what real users are waiting on. Gaps are
// re-derived from live data on every read, so a row researched through any
// path shows as closed here without anyone updating the ledger.

const DEFAULT_LIMIT = 50;

function usage(): string {
  return [
    "Manual research demand ledger CLI (operator-facing).",
    "",
    "Usage: npm run manual:demand:<command> -- [flags]",
    "",
    "Commands:",
    "  status  List open gaps hottest first, plus a count of rows whose gap closed.",
    `          [--stage ${MANUAL_RESEARCH_DEMAND_STAGES.join("|")}] [--state XX] [--limit <n>] [--horizon-days <n>]`,
    "  prune   Delete ledger rows whose gap has closed. [--horizon-days <n>]",
  ].join("\n");
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function readStageFlag(argv: readonly string[]): ManualResearchDemandStage | undefined {
  const raw = readStrictFlagValue(argv, "--stage");
  if (raw === null) {
    return undefined;
  }
  const normalized = raw.trim().toLowerCase();
  if (!MANUAL_RESEARCH_DEMAND_STAGES.includes(normalized as ManualResearchDemandStage)) {
    throw new Error(`Invalid --stage: ${raw}. Expected one of ${MANUAL_RESEARCH_DEMAND_STAGES.join(", ")}.`);
  }
  return normalized as ManualResearchDemandStage;
}

function readStateFlag(argv: readonly string[]): string | undefined {
  const raw = readStrictFlagValue(argv, "--state");
  if (raw === null) {
    return undefined;
  }
  const normalized = raw.trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) {
    throw new Error(`--state must be a two-letter state code, got: ${raw}`);
  }
  return normalized;
}

function print(payload: unknown): void {
  console.log(JSON.stringify(payload, null, 2));
}

async function runCommand(pool: Pool, command: string, argv: readonly string[]): Promise<void> {
  const asOfDate = usLatestLocalDateIso();
  const horizonDays = readStrictPositiveIntegerFlag(argv, "--horizon-days") ?? DEFAULT_MANUAL_RESEARCH_DEMAND_HORIZON_DAYS;

  switch (command) {
    case "status": {
      const stage = readStageFlag(argv);
      const state = readStateFlag(argv);
      const limit = readStrictPositiveIntegerFlag(argv, "--limit") ?? DEFAULT_LIMIT;
      const { open, closed } = await listManualResearchDemand(pool, { asOfDate, horizonDays, stage, state, limit });
      const openByStage: Record<string, number> = {};
      for (const row of open) {
        openByStage[row.stage] = (openByStage[row.stage] ?? 0) + 1;
      }
      print({
        asOfDate,
        horizonDays,
        ...(stage ? { stage } : {}),
        ...(state ? { state } : {}),
        limit,
        openCount: open.length,
        openCountByStage: openByStage,
        closedCount: closed.length,
        closedHint: closed.length > 0 ? "run manual:demand:prune to drop rows whose gap has closed" : undefined,
        open,
      });
      return;
    }

    case "prune": {
      const { deleted } = await pruneManualResearchDemand(pool, { asOfDate, horizonDays });
      print({ asOfDate, horizonDays, deleted });
      return;
    }

    default:
      throw new Error(`Unknown command: ${command}\n\n${usage()}`);
  }
}

const FLAG_SPECS: Record<string, readonly CliFlagSpec[]> = {
  status: [
    { name: "--stage", value: "both" },
    { name: "--state", value: "both" },
    { name: "--limit", value: "both" },
    { name: "--horizon-days", value: "both" },
  ],
  prune: [{ name: "--horizon-days", value: "both" }],
};

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const command = argv.find((token) => !token.startsWith("--"));
  if (!command || argv.includes("--help") && !FLAG_SPECS[command]) {
    console.log(usage());
    return;
  }
  const specs = FLAG_SPECS[command];
  if (!specs) {
    throw new Error(`Unknown command: ${command}\n\n${usage()}`);
  }
  assertKnownCliFlags(`manual:demand:${command}`, argv.filter((token) => token !== command), specs);
  loadProjectEnv();

  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  try {
    await runCommand(pool, command, argv);
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual research demand CLI failed:", message);
  process.exitCode = 1;
});
