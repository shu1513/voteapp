import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  parseProbeHawaiiCandidateFinanceArgs,
  runProbeHawaiiCandidateFinance,
} from "./probeHawaiiCandidateFinance.js";

type HawaiiCandidateFinanceLiveSmokeCheck = {
  name: string;
  passed: boolean;
  detail?: string;
};

export type HawaiiCandidateFinanceLiveSmokeOutput = {
  type: "hawaii_candidate_finance_live_smoke";
  ts: string;
  ok: boolean;
  checks: HawaiiCandidateFinanceLiveSmokeCheck[];
  probe: Awaited<ReturnType<typeof runProbeHawaiiCandidateFinance>>;
};

type HawaiiCandidateFinanceLiveSmokeClient = Parameters<typeof runProbeHawaiiCandidateFinance>[0]["client"];

const DEFAULT_ARGS = [
  "--candidate-name=Josh Green",
  "--year=2022",
  "--office=Governor",
  "--scope=statewide",
  "--limit=5",
  "--funder-limit=10",
  "--min-industry-amount=25000",
  "--timeout-ms=30000",
] as const;

function normalizeText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function check(name: string, passed: boolean, detail?: string): HawaiiCandidateFinanceLiveSmokeCheck {
  return {
    name,
    passed,
    ...(detail ? { detail } : {}),
  };
}

export async function runHawaiiCandidateFinanceLiveSmoke(input: {
  args?: readonly string[];
  client?: HawaiiCandidateFinanceLiveSmokeClient;
  now?: Date;
} = {}): Promise<HawaiiCandidateFinanceLiveSmokeOutput> {
  const args = parseProbeHawaiiCandidateFinanceArgs(input.args && input.args.length > 0 ? input.args : DEFAULT_ARGS);
  const probe = await runProbeHawaiiCandidateFinance({
    args,
    client: input.client,
    now: input.now,
  });

  const topOccupationNames = new Set(
    probe.direct_campaign.top_occupations.map((occupation) => normalizeText(occupation.category_name))
  );
  const supportGroupNames = new Set(
    probe.outside_spending.top_supporting_groups.map((group) => normalizeText(group.committee_name))
  );
  const supportingIndustryNames = new Set(
    probe.outside_spending.top_supporting_industries.map((industry) => industry.industry_slug)
  );

  const checks = [
    check("matched_josh_green_committee", probe.ok && probe.resolution.status === "matched", probe.resolution.status),
    check("top_occupations_present", probe.direct_campaign.top_occupations.length > 0),
    check("attorney_occupation_present", topOccupationNames.has("ATTORNEY")),
    check("be_change_now_support_group_present", supportGroupNames.has("BE CHANGE NOW")),
    check("construction_support_industry_present", supportingIndustryNames.has("construction")),
    check("opposition_groups_present", probe.outside_spending.top_opposing_groups.length > 0),
  ];

  return {
    type: "hawaii_candidate_finance_live_smoke",
    ts: (input.now ?? new Date()).toISOString(),
    ok: checks.every((item) => item.passed),
    checks,
    probe,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runHawaiiCandidateFinanceLiveSmoke({ args: process.argv.slice(2) });
  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Hawaii candidate finance live smoke failed:", message);
    process.exitCode = 1;
  });
}
