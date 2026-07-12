import { pathToFileURL } from "node:url";
import { loadProjectEnv } from "../config/env.js";
import { aggregateHoustonDirectContributions } from "../pipeline/houstonFinance/houstonDirectContributionAggregator.js";
import { loadHoustonCandidateFinanceReports } from "../pipeline/houstonFinance/houstonCampaignFinanceReportSource.js";
import { selectEffectiveHoustonCandidateReports } from "../pipeline/houstonFinance/houstonCampaignFinancePdfParser.js";
import { aggregateHoustonTexasGpacOutsideSpending } from "../pipeline/houstonFinance/houstonTexasGpacOutsideSpendingAggregator.js";
import { loadHoustonTexasTecData } from "../pipeline/houstonFinance/houstonTexasTecDataSource.js";
import { parseStoredHoustonFinanceOfficeTarget } from "../pipeline/houstonFinance/houstonFinanceOfficeTargets.js";

function value(args: string[], name: string): string {
  const inline = args.find((arg) => arg.startsWith(`${name}=`))?.slice(name.length + 1);
  const index = args.indexOf(name);
  const result = inline ?? (index >= 0 ? args[index + 1] : undefined);
  if (!result?.trim()) throw new Error(`${name} is required`);
  return result.trim();
}

function optionalValue(args: string[], name: string): string | undefined {
  const inline = args.find((arg) => arg.startsWith(`${name}=`))?.slice(name.length + 1);
  const index = args.indexOf(name);
  const result = inline ?? (index >= 0 ? args[index + 1] : undefined);
  return result?.trim() || undefined;
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = process.argv.slice(2);
  const candidateName = value(args, "--candidate-name");
  const firstName = value(args, "--first-name");
  const lastName = value(args, "--last-name");
  const electionYear = Number(value(args, "--election-year"));
  if (!Number.isInteger(electionYear)) throw new Error("--election-year must be an integer");
  const officeTarget = parseStoredHoustonFinanceOfficeTarget({
    officeName: optionalValue(args, "--office-name") ?? "Mayor",
    district: optionalValue(args, "--seat") ?? "Houston",
  });
  if (!officeTarget) throw new Error("--office-name and --seat do not identify a supported Houston office");
  const reports = selectEffectiveHoustonCandidateReports(await loadHoustonCandidateFinanceReports({
    candidateName, firstName, lastName, electionYear, officeTarget,
  }));
  const direct = aggregateHoustonDirectContributions({ reports });
  let outside: ReturnType<typeof aggregateHoustonTexasGpacOutsideSpending> | null = null;
  try {
    const tec = await loadHoustonTexasTecData({ candidates: [{ candidateName, electionYear }] });
    outside = aggregateHoustonTexasGpacOutsideSpending({ candidateName, electionYear, officeTarget, ...tec });
  } catch (error) {
    console.warn(error instanceof Error ? error.message : String(error));
  }
  console.log(JSON.stringify({
    candidate_name: candidateName,
    election_year: electionYear,
    office: officeTarget,
    reports: reports.map((report) => ({ source: report.index.sourceSystem, report_id: report.index.reportId,
      period_start: report.periodStart, period_end: report.periodEnd, contributions: report.contributions.length })),
    direct,
    outside,
  }, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) main().catch((error) => { console.error(error); process.exitCode = 1; });
