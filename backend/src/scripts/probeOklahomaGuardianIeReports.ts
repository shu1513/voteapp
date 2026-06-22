import { pathToFileURL } from "node:url";

import {
  discoverOklahomaGuardianIeOutsideSpendingReports,
  type OklahomaGuardianIeOutsideSpendingDiscoveryResult,
} from "../pipeline/oklahomaFinance/oklahomaGuardianIeOutsideSpendingDiscovery.js";

export type ProbeOklahomaGuardianIeReportsScriptOptions = {
  candidateName: string;
  year: number;
  maxReports?: number;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0] ?? null;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function requireStringFlag(args: readonly string[], name: string): string {
  const value = parseFlagValue(args, name);
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

export function parseProbeOklahomaGuardianIeReportsScriptArgs(
  args: readonly string[]
): ProbeOklahomaGuardianIeReportsScriptOptions {
  const year = parsePositiveIntegerFlag(args, "--year");
  if (!year || year < 2000 || year > 2100) {
    throw new Error("--year must be an election year between 2000 and 2100");
  }
  return {
    candidateName: requireStringFlag(args, "--candidate-name"),
    year,
    maxReports: parsePositiveIntegerFlag(args, "--max-reports"),
  };
}

export function toProbeOklahomaGuardianIeReportsScriptOutput(input: {
  startedAt: Date;
  options: ProbeOklahomaGuardianIeReportsScriptOptions;
  result: OklahomaGuardianIeOutsideSpendingDiscoveryResult;
}) {
  return {
    type: "oklahoma_guardian_ie_report_probe",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    candidate_name: input.options.candidateName,
    election_year: input.options.year,
    max_reports: input.options.maxReports ?? 10,
    search: {
      source_url: input.result.search.sourceUrl,
      date_from: input.result.search.dateFrom,
      date_through: input.result.search.dateThrough,
      expenditure_type: input.result.search.expenditureType,
      result_count: input.result.search.rows.length,
    },
    reports_examined: input.result.reportsExamined,
    usable_reports: input.result.usableReports.map((report) => ({
      spender_name: report.spenderName,
      candidate_name: report.candidateName,
      office_name: report.officeName,
      support_oppose: report.supportOppose,
      amount: report.amount,
      report_description: report.reportDescription,
      reporting_period_begin: report.reportingPeriodBegin,
      reporting_period_end: report.reportingPeriodEnd,
      source_url: report.sourceUrl,
      pdf_byte_length: report.pdfByteLength,
    })),
    skipped_reports: input.result.skippedReports.map((report) => ({
      row_index: report.rowIndex,
      filer_name: report.sourceRow.filerName,
      report_description: report.sourceRow.reportDescription,
      reason: report.reason,
      error_message: report.errorMessage,
      matching_candidate_count: report.matchingCandidateStances?.length ?? 0,
    })),
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  const options = parseProbeOklahomaGuardianIeReportsScriptArgs(process.argv.slice(2));
  const result = await discoverOklahomaGuardianIeOutsideSpendingReports({
    candidateName: options.candidateName,
    electionYear: options.year,
    maxReports: options.maxReports,
  });

  console.log(JSON.stringify(toProbeOklahomaGuardianIeReportsScriptOutput({ startedAt, options, result }), null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Oklahoma Guardian IE report probe failed:", message);
    process.exitCode = 1;
  });
}
