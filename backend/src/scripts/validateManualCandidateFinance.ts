import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import {
  parseManualCandidateFinancePayload,
  type ManualCandidateFinancePayload,
} from "../contracts/manualCandidateFinancePayloadContract.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

const SCRIPT_LABEL = "manual:candidate-finance:validate";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-finance:validate -- --file payload.json",
    "",
    "Validates one manual_candidate_finance.v1 payload and prints a dry-run summary.",
    "This command never loads environment variables and never writes to a database.",
  ].join("\n");
}

function readFileFlag(argv: readonly string[]): string {
  const index = argv.indexOf("--file");
  const value = index >= 0 ? argv[index + 1] : undefined;
  if (!value || value.startsWith("--") || value.trim().length === 0) {
    throw new Error(`Missing value for --file.\n${usage()}`);
  }
  return value.trim();
}

export async function validateManualCandidateFinanceFile(path: string): Promise<ManualCandidateFinancePayload> {
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Unable to read ${path}: ${reason}`);
  }

  let payload: unknown;
  try {
    payload = JSON.parse(raw) as unknown;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Invalid JSON in ${path}: ${reason}`);
  }

  const parsed = parseManualCandidateFinancePayload(payload);
  if (!parsed.ok) {
    throw new Error(`${path}: ${parsed.reason}`);
  }
  return parsed.payload;
}

export async function runValidateManualCandidateFinance(argv: readonly string[]): Promise<void> {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [{ name: "--file", value: "space" }]);
  const file = readFileFlag(argv);
  const payload = await validateManualCandidateFinanceFile(file);

  console.log(
    JSON.stringify(
      {
        dryRun: true,
        file,
        schemaVersion: payload.schema_version,
        filingType: payload.filing_type,
        filingId: payload.filing_id,
        reportDate: payload.report_date,
        candidateEdges: payload.filing_type === "independent_expenditure" ? payload.candidate_edges.length : 0,
      },
      null,
      2
    )
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runValidateManualCandidateFinance(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
