import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  parseProbeIllinoisCandidateFinanceArgs,
  runProbeIllinoisCandidateFinance,
} from "./probeIllinoisCandidateFinance.js";

type IllinoisCandidateFinanceLiveSmokeCheck = {
  name: string;
  passed: boolean;
  detail?: string;
};

export type IllinoisCandidateFinanceLiveSmokeOutput = {
  type: "illinois_candidate_finance_live_smoke";
  ts: string;
  ok: boolean;
  checks: IllinoisCandidateFinanceLiveSmokeCheck[];
  probe: Awaited<ReturnType<typeof runProbeIllinoisCandidateFinance>>;
};

type IllinoisCandidateFinanceLiveSmokeClient = Parameters<typeof runProbeIllinoisCandidateFinance>[0]["client"];

const DEFAULT_ARGS = [
  "--candidate-name=JB Pritzker",
  "--year=2022",
  "--office=Governor",
  "--limit=5",
  "--funder-limit=10",
  "--min-industry-amount=25000",
  "--timeout-ms=30000",
] as const;

function check(name: string, passed: boolean, detail?: string): IllinoisCandidateFinanceLiveSmokeCheck {
  return {
    name,
    passed,
    ...(detail ? { detail } : {}),
  };
}

function flagName(arg: string): string {
  const separatorIndex = arg.indexOf("=");
  return separatorIndex > -1 ? arg.slice(0, separatorIndex) : arg;
}

function argsWithDefaultSmokeTarget(args: readonly string[] | undefined): readonly string[] {
  if (!args || args.length === 0) {
    return DEFAULT_ARGS;
  }
  const explicitFlags = new Set(args.filter((arg) => arg.startsWith("--")).map(flagName));
  return [...DEFAULT_ARGS.filter((arg) => !explicitFlags.has(flagName(arg))), ...args];
}

export async function runIllinoisCandidateFinanceLiveSmoke(input: {
  args?: readonly string[];
  client?: IllinoisCandidateFinanceLiveSmokeClient;
  now?: Date;
} = {}): Promise<IllinoisCandidateFinanceLiveSmokeOutput> {
  const args = parseProbeIllinoisCandidateFinanceArgs(argsWithDefaultSmokeTarget(input.args));
  const probe = await runProbeIllinoisCandidateFinance({
    args,
    client: input.client,
    now: input.now,
  });
  const outsideGroupCount =
    probe.outside_spending.top_supporting_groups.length + probe.outside_spending.top_opposing_groups.length;
  const supportingIndustriesWithEvidence = probe.outside_spending.top_supporting_industries.filter(
    (industry) => industry.evidence.length > 0
  ).length;

  const checks = [
    check("probe_ok", probe.ok),
    check("top_occupations_present", probe.direct_campaign.top_occupations.length > 0),
    check("contribution_size_buckets_present", probe.direct_campaign.contribution_size_buckets.length > 0),
    check("outside_groups_present", outsideGroupCount > 0, `outside_group_count=${outsideGroupCount}`),
    check(
      "supporting_industry_evidence_present",
      supportingIndustriesWithEvidence > 0,
      `supporting_industries_with_evidence=${supportingIndustriesWithEvidence}`
    ),
  ];

  return {
    type: "illinois_candidate_finance_live_smoke",
    ts: (input.now ?? new Date()).toISOString(),
    ok: checks.every((item) => item.passed),
    checks,
    probe,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runIllinoisCandidateFinanceLiveSmoke({ args: process.argv.slice(2) });
  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Illinois candidate finance live smoke failed:", message);
    process.exitCode = 1;
  });
}
