import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { refreshNewYorkCityCfbArtifact } from "../pipeline/newYorkCityFinance/newYorkCityCfbArtifactCache.js";
import {
  readNewYorkCityCfbContributions,
  readNewYorkCityCfbFinancialAnalysis,
} from "../pipeline/newYorkCityFinance/newYorkCityCfbCsv.js";

function parseYear(args: readonly string[]): number {
  const inline = args.find((arg) => arg.startsWith("--year="))?.slice(7);
  const index = args.indexOf("--year");
  const raw = inline ?? (index >= 0 ? args[index + 1] : undefined);
  const year = Number(raw ?? 2025);
  if (!Number.isInteger(year) || year < 2001 || year > 2100) throw new Error(`Invalid --year: ${raw}`);
  return year;
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = process.argv.slice(2);
  const electionYear = parseYear(args);
  const cacheDir = args.find((arg) => arg.startsWith("--cache-dir="))?.slice(12);
  const [contributions, analysis] = await Promise.all([
    refreshNewYorkCityCfbArtifact({ electionYear, kind: "contributions", cacheDir }),
    refreshNewYorkCityCfbArtifact({ electionYear, kind: "financial_analysis", cacheDir }),
  ]);
  if (contributions.status === "not_yet_published" || analysis.status === "not_yet_published") {
    console.log(JSON.stringify({
      type: "new_york_city_cfb_finance_probe",
      election_year: electionYear,
      status: "not_yet_published",
      artifacts: { contributions, financial_analysis: analysis },
    }, null, 2));
    return;
  }
  const analysisResult = await readNewYorkCityCfbFinancialAnalysis({ filePath: analysis.current.filePath });
  const candidateIds = new Set(analysisResult.rows.map((row) => row.candidateId));
  const contributionResult = await readNewYorkCityCfbContributions({
    filePath: contributions.current.filePath,
    candidateIds,
  });
  const officeCounts = new Map<string, number>();
  for (const row of analysisResult.rows) officeCounts.set(row.officeCode, (officeCounts.get(row.officeCode) ?? 0) + 1);
  console.log(JSON.stringify({
    type: "new_york_city_cfb_finance_probe",
    election_year: electionYear,
    status: "available",
    artifacts: { contributions, financial_analysis: analysis },
    analysis: {
      raw_rows: analysisResult.rawRowCount,
      accepted_rows: analysisResult.rows.length,
      malformed_rows: analysisResult.malformedRowCount,
      distinct_candidates: candidateIds.size,
      office_counts: Object.fromEntries(officeCounts),
      latest_to_statement: Math.max(0, ...analysisResult.rows.map((row) => row.toStatement)),
    },
    contributions: {
      raw_rows: contributionResult.rawRowCount,
      accepted_rows: contributionResult.rows.length,
      malformed_rows: contributionResult.malformedRowCount,
    },
  }, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("NYC CFB finance probe failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
