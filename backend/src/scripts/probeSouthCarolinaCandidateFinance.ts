// Live one-candidate diagnostic for the South Carolina finance pipeline.
// READ-ONLY: resolves the candidate against the live Ethics filer search,
// then runs the full fetch + aggregation path and prints JSON. Never touches
// the database. Free open JSON API — no AI involved.
//
// Usage:
//   npm run south-carolina-candidates:finance:live-probe -- \
//     --candidate-name "Pamela Evette" --election-date 2026-11-03
//   (add --filer-id 54395 to skip resolution and probe a known filer)

import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  loadSouthCarolinaFilerReportSets,
} from "../pipeline/southCarolinaFinance/southCarolinaCandidateFinanceAutoLink.js";
import {
  southCarolinaAcceptedElectionDates,
} from "../pipeline/southCarolinaFinance/southCarolinaCandidateFinanceSync.js";
import {
  resolveSouthCarolinaCandidateFiler,
  southCarolinaFilerSearchTerm,
} from "../pipeline/southCarolinaFinance/southCarolinaCandidateFilerResolver.js";
import {
  aggregateSouthCarolinaDirectFinance,
  selectSouthCarolinaAcceptedRuns,
  southCarolinaContributionYearsForRuns,
} from "../pipeline/southCarolinaFinance/southCarolinaDirectContributionAggregator.js";
import {
  getSouthCarolinaCandidateReports,
  getSouthCarolinaReportDetails,
  searchSouthCarolinaContributions,
  SOUTH_CAROLINA_ETHICS_PUBLIC_REPORTING_URL,
  type SouthCarolinaReportDetails,
} from "../pipeline/southCarolinaFinance/southCarolinaEthicsClient.js";
import {
  assertNoUnknownSouthCarolinaFinanceFlags,
  parseSouthCarolinaFinanceFlagValue,
  parseSouthCarolinaFinancePositiveIntegerFlag,
} from "./southCarolinaCandidateFinanceCliArgs.js";

type ProbeArgs = {
  candidateName: string;
  electionDate: string;
  filerId: number | undefined;
};

function parseProbeArgs(args: readonly string[]): ProbeArgs {
  assertNoUnknownSouthCarolinaFinanceFlags(args);
  const candidateName = parseSouthCarolinaFinanceFlagValue(args, "--candidate-name");
  const electionDate = parseSouthCarolinaFinanceFlagValue(args, "--election-date");
  if (!candidateName || !electionDate) {
    throw new Error("--candidate-name and --election-date are required");
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(electionDate)) {
    throw new Error(`--election-date must be ISO YYYY-MM-DD, got: ${electionDate}`);
  }
  return {
    candidateName,
    electionDate,
    filerId: parseSouthCarolinaFinancePositiveIntegerFlag(args, "--filer-id"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseProbeArgs(process.argv.slice(2));
  const electionYear = Number.parseInt(args.electionDate.slice(0, 4), 10);

  let filer: { candidateFilerId: number; filerName: string };
  let resolution: unknown = null;
  if (args.filerId !== undefined) {
    filer = { candidateFilerId: args.filerId, filerName: args.candidateName };
  } else {
    const { filerReportSets, skippedFilers } = await loadSouthCarolinaFilerReportSets({
      candidateName: args.candidateName,
      electionYear,
    });
    const skippedFilerIds = skippedFilers.map((filer) => filer.candidateFilerId);
    const resolved = resolveSouthCarolinaCandidateFiler({
      candidateName: args.candidateName,
      electionDate: args.electionDate,
      filerReportSets,
    });
    resolution = { ...resolved, skippedFilerIds };
    if (resolved.status !== "matched") {
      console.log(
        JSON.stringify(
          {
            type: "south_carolina_candidate_finance_live_probe",
            ts: new Date().toISOString(),
            args,
            ok: false,
            resolution,
          },
          null,
          2
        )
      );
      return;
    }
    const matchedSet = filerReportSets.find(
      (set) => set.filer.candidateFilerId === resolved.candidateFilerId
    );
    filer = { candidateFilerId: resolved.candidateFilerId, filerName: matchedSet?.filer.candidate ?? resolved.filerName };
  }

  const acceptedElectionDates = southCarolinaAcceptedElectionDates(electionYear, args.electionDate);
  const reports = await getSouthCarolinaCandidateReports(filer.candidateFilerId);
  const runs = selectSouthCarolinaAcceptedRuns(reports, electionYear, acceptedElectionDates);
  const detailsByReportId = new Map<number, SouthCarolinaReportDetails>();
  for (const run of runs) {
    for (const phaseFinal of run.phaseFinals) {
      detailsByReportId.set(phaseFinal.reportId, await getSouthCarolinaReportDetails(phaseFinal.reportId));
    }
  }
  const searchTerm = southCarolinaFilerSearchTerm(filer.filerName);
  const contributionYears = southCarolinaContributionYearsForRuns(reports, electionYear, acceptedElectionDates);
  const contributionRows = [];
  if (searchTerm !== null) {
    for (const year of contributionYears) {
      contributionRows.push(...(await searchSouthCarolinaContributions({ candidate: searchTerm, contributionYear: year })));
    }
  }

  const aggregation = aggregateSouthCarolinaDirectFinance({
    candidateFilerId: filer.candidateFilerId,
    electionYear,
    reports,
    detailsByReportId,
    contributionRows,
    acceptedElectionDates,
    sourceUrl: SOUTH_CAROLINA_ETHICS_PUBLIC_REPORTING_URL,
  });

  console.log(
    JSON.stringify(
      {
        type: "south_carolina_candidate_finance_live_probe",
        ts: new Date().toISOString(),
        args,
        ok: aggregation.status === "aggregated",
        resolution,
        filer,
        accepted_election_dates: acceptedElectionDates,
        report_count: reports.length,
        run_count: runs.length,
        contribution_years: contributionYears,
        contribution_row_count: contributionRows.length,
        aggregation,
      },
      null,
      2
    )
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("South Carolina candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
