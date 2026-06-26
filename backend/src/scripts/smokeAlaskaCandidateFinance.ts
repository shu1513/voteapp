import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import type { AlaskaApocCsvFetchFn } from "../pipeline/alaskaFinance/alaskaApocClient.js";
import { loadAlaskaApocFinanceData } from "../pipeline/alaskaFinance/alaskaApocDataSource.js";
import {
  parseProbeAlaskaCandidateFinanceArgs,
  runProbeAlaskaCandidateFinance,
} from "./probeAlaskaCandidateFinance.js";

type AlaskaCandidateFinanceLiveSmokeCheck = {
  name: string;
  passed: boolean;
  detail?: string;
};

export type AlaskaCandidateFinanceLiveSmokeOutput = {
  type: "alaska_candidate_finance_live_smoke";
  ts: string;
  ok: boolean;
  skipped: boolean;
  checks: AlaskaCandidateFinanceLiveSmokeCheck[];
  data_source?: Awaited<ReturnType<typeof loadAlaskaApocFinanceData>>["metadata"];
  probe?: Awaited<ReturnType<typeof runProbeAlaskaCandidateFinance>>;
};

const DEFAULT_ARGS = [
  "--candidate-name=Mike Dunleavy",
  "--year=2022",
  "--candidate-filer-name=DUNLEAVY FOR GOVERNOR",
  "--limit=5",
  "--min-industry-amount=25000",
] as const;

function check(name: string, passed: boolean, detail?: string): AlaskaCandidateFinanceLiveSmokeCheck {
  return {
    name,
    passed,
    ...(detail ? { detail } : {}),
  };
}

function isCi(): boolean {
  const value = process.env.CI?.trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

export async function runAlaskaCandidateFinanceLiveSmoke(input: {
  args?: readonly string[];
  fetchFn?: AlaskaApocCsvFetchFn;
  now?: Date;
  allowInCi?: boolean;
} = {}): Promise<AlaskaCandidateFinanceLiveSmokeOutput> {
  const now = input.now ?? new Date();
  if (isCi() && input.allowInCi !== true && process.env.ALASKA_CAMPAIGN_FINANCE_LIVE_SMOKE_ALLOW_CI !== "true") {
    return {
      type: "alaska_candidate_finance_live_smoke",
      ts: now.toISOString(),
      ok: true,
      skipped: true,
      checks: [check("ci_guard", true, "Set ALASKA_CAMPAIGN_FINANCE_LIVE_SMOKE_ALLOW_CI=true to run in CI")],
    };
  }

  const args = parseProbeAlaskaCandidateFinanceArgs(input.args && input.args.length > 0 ? input.args : DEFAULT_ARGS);
  const loadedData = await loadAlaskaApocFinanceData(
    {
      mode: "live",
      incomeUrl: args.incomeUrl ?? undefined,
      independentExpendituresUrl: args.independentExpendituresUrl ?? undefined,
      independentContributionsUrl: args.independentContributionsUrl ?? undefined,
      timeoutMs: args.timeoutMs,
      retryCount: args.retryCount,
      retryDelayMs: args.retryDelayMs,
      requestSpacingMs: args.requestSpacingMs,
    },
    { fetchFn: input.fetchFn, logger: console }
  );
  const probe = await runProbeAlaskaCandidateFinance({
    args,
    datasets: {
      incomeRows: loadedData.apocData.incomeRows,
      independentExpenditureRows: loadedData.apocData.independentExpenditureRows,
      independentContributionRows: loadedData.apocData.independentContributionRows,
    },
    now,
  });

  const checks = [
    check("campaign_income_rows_present", loadedData.apocData.incomeRows.length > 0, `${loadedData.apocData.incomeRows.length}`),
    check(
      "independent_expenditure_rows_loaded",
      loadedData.apocData.independentExpenditureRows.length >= 0,
      `${loadedData.apocData.independentExpenditureRows.length}`
    ),
    check(
      "independent_contribution_rows_loaded",
      loadedData.apocData.independentContributionRows.length >= 0,
      `${loadedData.apocData.independentContributionRows.length}`
    ),
    check("candidate_probe_has_match", probe.ok, probe.candidate_match.candidate_name),
    check(
      "direct_or_outside_rows_present",
      probe.direct_campaign.matched_row_count > 0 || probe.outside_spending.matched_expenditure_row_count > 0,
      `direct=${probe.direct_campaign.matched_row_count} outside=${probe.outside_spending.matched_expenditure_row_count}`
    ),
  ];

  return {
    type: "alaska_candidate_finance_live_smoke",
    ts: now.toISOString(),
    ok: checks.every((item) => item.passed),
    skipped: false,
    checks,
    data_source: loadedData.metadata,
    probe,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runAlaskaCandidateFinanceLiveSmoke({ args: process.argv.slice(2) });
  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Alaska candidate finance live smoke failed:", message);
    process.exitCode = 1;
  });
}
