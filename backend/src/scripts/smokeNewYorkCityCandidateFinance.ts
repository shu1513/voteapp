import { pathToFileURL } from "node:url";

import { syncDueNewYorkCityCandidateFinance } from "../pipeline/newYorkCityFinance/newYorkCityCandidateFinanceBatchSync.js";
import { getNewYorkCityCfbArtifactCachePaths } from "../pipeline/newYorkCityFinance/newYorkCityCfbArtifactCache.js";
import {
  readNewYorkCityCfbContributions,
  readNewYorkCityCfbFinancialAnalysis,
} from "../pipeline/newYorkCityFinance/newYorkCityCfbCsv.js";
import { resolveNewYorkCityCandidate } from "../pipeline/newYorkCityFinance/newYorkCityCandidateResolver.js";
import { aggregateNewYorkCityDirectContributions } from "../pipeline/newYorkCityFinance/newYorkCityDirectContributionAggregator.js";

const MOCK_CANDIDATE_ID = "00000000-0000-4000-8000-000000000001";
const MOCK_ELECTION_ID = "00000000-0000-4000-8000-000000000002";

function flag(args: readonly string[], name: string, fallback: string): string {
  return args.find((arg) => arg.startsWith(`${name}=`))?.slice(name.length + 1).trim() || fallback;
}
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const candidateName = flag(args, "--candidate", "Eric Adams");
  const electionYear = Number(flag(args, "--year", "2025"));
  const cacheDir = flag(args, "--cache-dir", "/tmp/voteapp-nyc-cfb-smoke");
  if (!Number.isInteger(electionYear)) throw new Error("--year must be an integer");

  const db = {
    query: async (): Promise<never> => { throw new Error("Mock live smoke attempted a DB query"); },
    connect: async (): Promise<never> => { throw new Error("Mock live smoke attempted a DB connection"); },
  };
  const batch = await syncDueNewYorkCityCandidateFinance({
    db: db as never,
    dryRun: true,
    cacheDir,
    dataSource: {
      listDueRows: async () => ({
        totalDueRows: 1,
        rows: [{
          candidateId: MOCK_CANDIDATE_ID,
          electionId: MOCK_ELECTION_ID,
          candidateName,
          electionYear,
          officeScope: "place",
          officeCanonicalName: "Mayor",
          districtGeoid: "3651000",
          cfbCandidateId: null,
        }],
      }),
    },
  });

  const analysisPath = getNewYorkCityCfbArtifactCachePaths({ cacheDir, electionYear, kind: "financial_analysis" }).filePath;
  const contributionPath = getNewYorkCityCfbArtifactCachePaths({ cacheDir, electionYear, kind: "contributions" }).filePath;
  const analysisRows = (await readNewYorkCityCfbFinancialAnalysis({ filePath: analysisPath })).rows;
  const resolution = resolveNewYorkCityCandidate({
    candidateName,
    electionYear,
    officeScope: "place",
    officeCanonicalName: "Mayor",
    districtGeoid: "3651000",
    analysisRows,
  });
  if (resolution.status !== "matched") throw new Error(`Mock candidate did not resolve: ${resolution.status}`);
  const contributions = await readNewYorkCityCfbContributions({
    filePath: contributionPath,
    candidateIds: new Set([resolution.cfbCandidateId]),
  });
  const direct = aggregateNewYorkCityDirectContributions({
    rows: contributions.rows,
    candidateId: resolution.cfbCandidateId,
    electionYear,
    officeCode: resolution.officeCode,
    maxBreakdownsPerCategory: 3,
  });

  console.log(JSON.stringify({
    type: "new_york_city_candidate_finance_mock_live_smoke",
    ok: batch.failedCandidateCount === 0 && batch.syncedCandidateCount === 1,
    database_writes: 0,
    mock_voteapp_candidate: { candidate_id: MOCK_CANDIDATE_ID, election_id: MOCK_ELECTION_ID, candidate_name: candidateName },
    resolution: {
      cfb_candidate_id: resolution.cfbCandidateId,
      cfb_candidate_name: resolution.cfbCandidateName,
      office_code: resolution.officeCode,
    },
    authoritative_summary: {
      private_contributions: resolution.summary.privateContributions,
      public_funds: resolution.summary.publicFunds,
      net_expenditures: resolution.summary.netExpenditures,
      outstanding_bills: resolution.summary.outstandingBills,
      through_statement: resolution.summary.toStatement,
    },
    contributions: {
      accepted_rows: direct.acceptedRowCount,
      ignored_rows: direct.ignoredRowCount,
      top_breakdowns: direct.breakdowns,
    },
    batch,
  }, null, 2));
}

if ((process.argv[1] ? pathToFileURL(process.argv[1]).href : null) === import.meta.url) {
  main().catch((error) => {
    console.error("NYC finance mock live smoke failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
