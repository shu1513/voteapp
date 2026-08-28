import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isDelawareCampaignFinanceRawDataRefreshEnabled } from "../config/featureFlags.js";
import { acquireDelawareCfrsCommitteeArtifacts } from "../pipeline/delawareFinance/delawareCfrsArtifactAcquisition.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

const SCRIPT_LABEL = "delaware-candidates:finance:refresh-artifacts";

function usage(): string {
  return [
    "Usage:",
    "  npm run delaware-candidates:finance:refresh-artifacts -- --cf-id ID [--cf-id ID ...] [--cache-dir DIR] [--force]",
    "",
    "Fetches each committee's complete artifact bundle (receipts + expenses",
    "CSVs, filed-reports grid, report PDFs) from the live CFRS portal into",
    "the local artifact cache. No database access. Requires",
    "DELAWARE_CAMPAIGN_FINANCE_ENABLED plus",
    "DELAWARE_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED (--force stands in",
    "for the refresh flag, per-run).",
  ].join("\n");
}

export function parseDelawareRefreshArtifactsArgs(argv: readonly string[]): {
  cfIds: string[];
  cacheDir: string | undefined;
  force: boolean;
} {
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--cf-id", value: "space" },
    { name: "--cache-dir", value: "space" },
    { name: "--force", value: "none" },
  ]);
  const cfIds: string[] = [];
  let cacheDir: string | undefined;
  let force = false;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--force") {
      force = true;
      continue;
    }
    if (token === "--cf-id" || token === "--cache-dir") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        throw new Error(`Missing value for ${token}.\n${usage()}`);
      }
      if (token === "--cf-id") {
        if (!/^\d{8}$/.test(value)) {
          throw new Error(`Invalid --cf-id ${value} (expected 8 digits).\n${usage()}`);
        }
        if (!cfIds.includes(value)) {
          cfIds.push(value);
        }
      } else {
        cacheDir = value;
      }
      index += 1;
    }
  }
  if (cfIds.length === 0) {
    throw new Error(`At least one --cf-id is required.\n${usage()}`);
  }
  return { cfIds, cacheDir, force };
}

export async function runRefreshDelawareCfrsArtifacts(argv: readonly string[]): Promise<void> {
  const args = parseDelawareRefreshArtifactsArgs(argv);
  loadProjectEnv();
  if (!isDelawareCampaignFinanceRawDataRefreshEnabled(args.force)) {
    throw new Error(
      "Delaware live CFRS fetches are disabled: set DELAWARE_CAMPAIGN_FINANCE_ENABLED=true plus " +
        "DELAWARE_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED=true (or pass --force for this run)"
    );
  }
  let failures = 0;
  for (const cfId of args.cfIds) {
    try {
      const result = await acquireDelawareCfrsCommitteeArtifacts({ cfId, cacheDir: args.cacheDir });
      console.log(
        JSON.stringify({
          cfId,
          ok: true,
          committeeName: result.committeeName,
          receiptRowCount: result.receiptRowCount,
          expenseRowCount: result.expenseRowCount,
          filedReportCount: result.filedReportCount,
          reportPdfCount: result.reportPdfCount,
          retrievedAt: result.manifest.retrievedAt,
        })
      );
    } catch (error) {
      failures += 1;
      console.log(JSON.stringify({ cfId, ok: false, error: error instanceof Error ? error.message : String(error) }));
    }
  }
  if (failures > 0) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  runRefreshDelawareCfrsArtifacts(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
