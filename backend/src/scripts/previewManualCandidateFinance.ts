import { pathToFileURL } from "node:url";

import { compileManualCandidateFinancePreview } from "../pipeline/finance/manualCandidateFinancePreview.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { validateManualCandidateFinanceFile } from "./validateManualCandidateFinance.js";

const SCRIPT_LABEL = "manual:candidate-finance:preview";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-finance:preview -- --file filing-a.json [--file filing-b.json ...]",
    "",
    "Validates and compiles one or more manual_candidate_finance.v1 filings.",
    "The preview is read-only: no environment, network, or database access.",
  ].join("\n");
}

function readFileFlags(argv: readonly string[]): string[] {
  const files: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] !== "--file") {
      continue;
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for --file.\n${usage()}`);
    }
    files.push(value.trim());
    index += 1;
  }
  if (files.length === 0) {
    throw new Error(`At least one --file is required.\n${usage()}`);
  }
  return files;
}

export async function runPreviewManualCandidateFinance(argv: readonly string[]): Promise<void> {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [{ name: "--file", value: "space" }]);
  const files = readFileFlags(argv);
  const payloads = await Promise.all(files.map((file) => validateManualCandidateFinanceFile(file)));
  const preview = compileManualCandidateFinancePreview(payloads);
  console.log(JSON.stringify({ dryRun: true, files, preview }, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runPreviewManualCandidateFinance(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
