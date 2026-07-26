import { pathToFileURL } from "node:url";

import { aggregateMichiganDirectContributions } from "../pipeline/michiganFinance/michiganDirectContributionAggregator.js";
import { aggregateMichiganOutsideGroupContributions } from "../pipeline/michiganFinance/michiganOutsideGroupContributionAggregator.js";
import { aggregateMichiganOutsideSpending } from "../pipeline/michiganFinance/michiganOutsideSpendingAggregator.js";
import { resolveMichiganCandidateCommittee } from "../pipeline/michiganFinance/michiganCandidateCommitteeResolver.js";
import { normalizeMichiganCandidateNameKeys } from "../pipeline/michiganFinance/michiganCandidateCommitteeResolver.js";
import { buildMichiganMitnLegacyArchiveUrl } from "../pipeline/michiganFinance/michiganMitnLegacyArtifactCache.js";
import {
  readMichiganMitnLegacyContributionRows,
  readMichiganMitnLegacyExpenditureRows,
  type MichiganMitnLegacyContributionRow,
  type MichiganMitnLegacyExpenditureRow,
} from "../pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

export type ProbeMichiganCandidateFinanceOptions = {
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  currentOffice?: string | null;
  rawExtractedDir: string;
  sourceUrl?: string | null;
  maxRows?: number;
  limit: number;
  minIndustryAmount: number;
};

type FinanceBreakdown = {
  name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type OutsideGroup = {
  name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  source_url: string | null;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (!value) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || !next.trim()) {
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

function parseRequiredFlag(args: readonly string[], name: string): string {
  const value = parseFlagValue(args, name);
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
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

function parseElectionYear(value: string): number {
  if (!/^\d{4}$/.test(value)) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  return Number(value);
}

function normalizeId(value: string | null | undefined): string {
  return (value ?? "").trim().toUpperCase();
}

function candidateRowMatchesName(input: {
  row: MichiganMitnLegacyContributionRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const rowKeys = normalizeMichiganCandidateNameKeys(
    [input.row.can_first_name, input.row.can_last_name].filter(Boolean).join(" ")
  );
  for (const rowKey of rowKeys) {
    if (input.candidateNameKeys.has(rowKey)) {
      return true;
    }
  }
  return false;
}

function expenditureRowMatchesName(input: {
  row: MichiganMitnLegacyExpenditureRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const rowKeys = normalizeMichiganCandidateNameKeys(input.row.can_or_ballot);
  for (const rowKey of rowKeys) {
    if (input.candidateNameKeys.has(rowKey)) {
      return true;
    }
  }
  return false;
}

async function loadProbeRows(input: {
  options: ProbeMichiganCandidateFinanceOptions;
}): Promise<{
  contributionRows: MichiganMitnLegacyContributionRow[];
  expenditureRows: MichiganMitnLegacyExpenditureRow[];
  initialResolution: ReturnType<typeof resolveMichiganCandidateCommittee>;
}> {
  const candidateNameKeys = normalizeMichiganCandidateNameKeys(input.options.candidateName);
  const candidateContributionRows = await readMichiganMitnLegacyContributionRows({
    extractedDir: input.options.rawExtractedDir,
    year: input.options.electionYear,
    predicate: (row) => candidateRowMatchesName({ row, candidateNameKeys }),
    maxRows: input.options.maxRows,
  });
  const initialResolution = resolveMichiganCandidateCommittee({
    candidateName: input.options.candidateName,
    officeScope: input.options.officeScope,
    officeName: input.options.officeName,
    electionYear: input.options.electionYear,
    district: input.options.district,
    currentOffice: input.options.currentOffice,
    contributionRows: candidateContributionRows,
    sourceUrl: input.options.sourceUrl,
  });
  const expenditureRows = await readMichiganMitnLegacyExpenditureRows({
    extractedDir: input.options.rawExtractedDir,
    year: input.options.electionYear,
    predicate: (row) => expenditureRowMatchesName({ row, candidateNameKeys }),
    maxRows: input.options.maxRows,
  });

  if (initialResolution.status !== "matched") {
    return {
      contributionRows: candidateContributionRows,
      expenditureRows,
      initialResolution,
    };
  }

  const outsideGroups = aggregateMichiganOutsideSpending({
    candidateName: input.options.candidateName,
    officeScope: input.options.officeScope,
    officeName: input.options.officeName,
    electionYear: input.options.electionYear,
    district: input.options.district,
    expenditureRows,
    sourceUrl: input.options.sourceUrl,
    maxGroups: Math.max(input.options.limit, 5),
  });
  const committeeIds = new Set<string>([normalizeId(initialResolution.committeeId)]);
  for (const group of outsideGroups.summary?.groups ?? []) {
    committeeIds.add(normalizeId(group.committeeId));
  }

  const contributionRows = await readMichiganMitnLegacyContributionRows({
    extractedDir: input.options.rawExtractedDir,
    year: input.options.electionYear,
    predicate: (row) => committeeIds.has(normalizeId(row.cfr_com_id)),
    maxRows: input.options.maxRows,
  });

  return {
    contributionRows,
    expenditureRows,
    initialResolution,
  };
}

export function parseProbeMichiganCandidateFinanceArgs(args: readonly string[]): ProbeMichiganCandidateFinanceOptions {
  const electionYear = parseElectionYear(parseRequiredFlag(args, "--year"));
  const limit = parsePositiveIntegerFlag(args, "--limit") ?? 5;

  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear,
    officeScope: parseFlagValue(args, "--office-scope") ?? "statewide",
    officeName: parseRequiredFlag(args, "--office"),
    district: parseFlagValue(args, "--district"),
    currentOffice: parseFlagValue(args, "--current-office"),
    rawExtractedDir: parseRequiredFlag(args, "--raw-extracted-dir"),
    sourceUrl: parseFlagValue(args, "--source-url") ?? buildMichiganMitnLegacyArchiveUrl({ year: electionYear }),
    maxRows: parsePositiveIntegerFlag(args, "--max-rows"),
    limit,
    minIndustryAmount: parsePositiveIntegerFlag(args, "--min-industry-amount") ?? 25_000,
  };
}

function sortBreakdowns<T extends { amount: number; name: string }>(items: T[]): T[] {
  return [...items].sort((left, right) => right.amount - left.amount || left.name.localeCompare(right.name));
}

function topDirectBreakdowns(
  categoryType: "occupation" | "contribution_size",
  directBreakdowns: ReturnType<typeof aggregateMichiganDirectContributions>["directBreakdowns"],
  limit: number
): FinanceBreakdown[] {
  return sortBreakdowns(
    directBreakdowns
      .filter((breakdown) => breakdown.categoryType === categoryType)
      .map((breakdown) => ({
        name: breakdown.categoryName,
        amount: breakdown.amount,
        contributor_count: breakdown.contributorCount,
        source_url: breakdown.sourceUrl,
      }))
  ).slice(0, limit);
}

function topOutsideGroups(
  supportOppose: "support" | "oppose",
  groups: NonNullable<ReturnType<typeof aggregateMichiganOutsideSpending>["summary"]>["groups"] | undefined,
  limit: number
): OutsideGroup[] {
  return sortBreakdowns(
    (groups ?? [])
      .filter((group) => group.supportOppose === supportOppose)
      .map((group) => ({
        name: group.committeeName,
        support_oppose: group.supportOppose,
        amount: group.amount,
        source_url: group.sourceUrl,
      }))
  ).slice(0, limit);
}

function topOutsideIndustries(
  supportOppose: "support" | "oppose",
  breakdowns: ReturnType<typeof aggregateMichiganOutsideGroupContributions>["outsideGroupBreakdowns"],
  limit: number
): FinanceBreakdown[] {
  return sortBreakdowns(
    breakdowns
      .filter((breakdown) => breakdown.supportOppose === supportOppose && breakdown.categoryType === "industry")
      .map((breakdown) => ({
        name: breakdown.categoryName,
        amount: breakdown.amount,
        contributor_count: breakdown.contributorCount,
        source_url: breakdown.sourceUrl,
      }))
  ).slice(0, limit);
}

export async function runProbeMichiganCandidateFinance(input: {
  options: ProbeMichiganCandidateFinanceOptions;
  contributionRows?: readonly MichiganMitnLegacyContributionRow[];
  expenditureRows?: readonly MichiganMitnLegacyExpenditureRow[];
}) {
  const options = input.options;
  const loadedRows =
    input.contributionRows && input.expenditureRows
      ? {
          contributionRows: input.contributionRows,
          expenditureRows: input.expenditureRows,
          initialResolution: undefined,
        }
      : await loadProbeRows({ options });
  const contributionRows = input.contributionRows ?? loadedRows.contributionRows;
  const expenditureRows = input.expenditureRows ?? loadedRows.expenditureRows;
  const resolution =
    loadedRows.initialResolution ??
    resolveMichiganCandidateCommittee({
      candidateName: options.candidateName,
      officeScope: options.officeScope,
      officeName: options.officeName,
      electionYear: options.electionYear,
      district: options.district,
      currentOffice: options.currentOffice,
      contributionRows,
      sourceUrl: options.sourceUrl,
    });

  if (resolution.status !== "matched") {
    return {
      type: "michigan_candidate_finance_probe",
      ts: new Date().toISOString(),
      ok: false,
      resolution,
      rows_loaded: {
        contributions: contributionRows.length,
        expenditures: expenditureRows.length,
      },
    };
  }

  const direct = aggregateMichiganDirectContributions({
    committeeId: resolution.committeeId,
    electionYear: options.electionYear,
    contributionRows,
    sourceUrl: options.sourceUrl,
    maxBreakdownsPerCategory: Math.max(options.limit, 5),
  });
  const outside = aggregateMichiganOutsideSpending({
    candidateName: options.candidateName,
    officeScope: options.officeScope,
    officeName: options.officeName,
    electionYear: options.electionYear,
    district: options.district,
    expenditureRows,
    sourceUrl: options.sourceUrl,
    maxGroups: Math.max(options.limit, 5),
  });
  const outsideGroupContributions = aggregateMichiganOutsideGroupContributions({
    electionYear: options.electionYear,
    outsideGroups: outside.summary?.groups ?? [],
    contributionRows,
    sourceUrl: options.sourceUrl,
    maxBreakdownsPerCategory: Math.max(options.limit, 5),
    minIndustryAmount: options.minIndustryAmount,
  });

  return {
    type: "michigan_candidate_finance_probe",
    ts: new Date().toISOString(),
    ok: true,
    resolution,
    rows_loaded: {
      contributions: contributionRows.length,
      expenditures: expenditureRows.length,
    },
    summary: {
      total_receipts: direct.summary.totalReceipts,
      direct_contribution_total: direct.summary.directContributionTotal,
      outside_support_total: outside.summary?.supportTotal ?? 0,
      outside_oppose_total: outside.summary?.opposeTotal ?? 0,
    },
    direct_campaign: {
      top_occupations: topDirectBreakdowns("occupation", direct.directBreakdowns, options.limit),
      contribution_size_buckets: topDirectBreakdowns("contribution_size", direct.directBreakdowns, options.limit),
    },
    outside_spending: {
      top_supporting_groups: topOutsideGroups("support", outside.summary?.groups, options.limit),
      top_opposing_groups: topOutsideGroups("oppose", outside.summary?.groups, options.limit),
      top_supporting_industries: topOutsideIndustries(
        "support",
        outsideGroupContributions.outsideGroupBreakdowns,
        options.limit
      ),
      top_opposing_industries: topOutsideIndustries(
        "oppose",
        outsideGroupContributions.outsideGroupBreakdowns,
        options.limit
      ),
    },
    counters: {
      matched_contribution_rows: direct.matchedContributionRowCount,
      included_contribution_rows: direct.includedContributionRowCount,
      skipped_contribution_rows: direct.skippedContributionRowCount,
      matched_outside_expenditure_rows: outside.matchedExpenditureRowCount,
      included_outside_expenditure_rows: outside.includedExpenditureRowCount,
      skipped_outside_expenditure_rows: outside.skippedExpenditureRowCount,
      matched_outside_contribution_rows: outsideGroupContributions.matchedContributionRowCount,
      included_outside_contribution_rows: outsideGroupContributions.includedContributionRowCount,
      skipped_outside_contribution_rows: outsideGroupContributions.skippedContributionRowCount,
    },
  };
}

async function main(): Promise<void> {
  const options = parseProbeMichiganCandidateFinanceArgs(process.argv.slice(2));
  const result = await runProbeMichiganCandidateFinance({ options });
  console.log(JSON.stringify(result, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Michigan candidate finance probe failed:", message);
    process.exitCode = 1;
  });
}
