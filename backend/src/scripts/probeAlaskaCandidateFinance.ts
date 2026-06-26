import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_IE_CONTRIBUTIONS_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
  type AlaskaApocCampaignIncomeRow,
  type AlaskaApocIndependentContributionRow,
  type AlaskaApocIndependentExpenditureRow,
} from "../pipeline/alaskaFinance/alaskaApocClient.js";
import {
  loadAlaskaApocFinanceData,
  type AlaskaApocDataSourceMode,
} from "../pipeline/alaskaFinance/alaskaApocDataSource.js";
import {
  resolveAlaskaCandidateCommittee,
  type AlaskaCandidateCommitteeResolution,
} from "../pipeline/alaskaFinance/alaskaCandidateCommitteeResolver.js";
import {
  aggregateAlaskaDirectContributions,
  type AlaskaFinanceDirectBreakdown,
} from "../pipeline/alaskaFinance/alaskaDirectContributionAggregator.js";
import {
  aggregateAlaskaOutsideGroupContributions,
  classifyAlaskaOutsideGroupContributionRow,
  type AlaskaFinanceOutsideGroupBreakdown,
} from "../pipeline/alaskaFinance/alaskaOutsideGroupContributionAggregator.js";
import {
  aggregateAlaskaOutsideSpending,
  type AlaskaOutsideSpendingGroup,
  type AlaskaSupportOppose,
} from "../pipeline/alaskaFinance/alaskaOutsideSpendingAggregator.js";
import {
  FINANCE_INDUSTRY_SLUGS,
  type FinanceClassificationSource,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";

type AlaskaFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  dataSourceMode: AlaskaApocDataSourceMode;
  incomeCsvPath: string | null;
  independentExpendituresCsvPath: string | null;
  independentContributionsCsvPath: string | null;
  incomeUrl: string | null;
  independentExpendituresUrl: string | null;
  independentContributionsUrl: string | null;
  timeoutMs: number | undefined;
  retryCount: number | undefined;
  retryDelayMs: number | undefined;
  requestSpacingMs: number | undefined;
  candidateFilerId: string | null;
  candidateFilerName: string | null;
  limit: number;
  minIndustryAmount: number;
};

type AlaskaFinanceProbeDatasets = {
  incomeRows: AlaskaApocCampaignIncomeRow[];
  independentExpenditureRows: AlaskaApocIndependentExpenditureRow[];
  independentContributionRows: AlaskaApocIndependentContributionRow[];
};

type AlaskaFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type AlaskaFinanceProbeOutsideGroup = {
  committee_id: string;
  committee_name: string;
  support_oppose: AlaskaSupportOppose;
  amount: number;
  source_url: string | null;
};

type AlaskaFinanceProbeIndustryEvidence = {
  contributor_name: string;
  amount: number;
  committee_id: string;
  committee_name: string;
  classified_label: string;
  classification_source: FinanceClassificationSource;
  source_url: string | null;
};

type AlaskaFinanceProbeIndustry = AlaskaFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: AlaskaSupportOppose;
  evidence: AlaskaFinanceProbeIndustryEvidence[];
};

type AlaskaFinanceProbeOutput = {
  type: "alaska_candidate_finance_probe";
  ts: string;
  args: AlaskaFinanceProbeArgs;
  ok: boolean;
  candidate_match: {
    status: "provided" | AlaskaCandidateCommitteeResolution["status"];
    candidate_name: string;
    candidate_filer_id: string | null;
    candidate_filer_name: string | null;
    matched_row_count: number | null;
    source: "cli" | "apoc_csv" | null;
  };
  direct_campaign: {
    total_direct_contributions: number;
    total_receipts: number;
    top_occupations: AlaskaFinanceProbeBreakdown[];
    contribution_size_buckets: AlaskaFinanceProbeBreakdown[];
    matched_row_count: number;
    included_row_count: number;
    skipped_row_count: number;
  };
  outside_spending: {
    top_supporting_groups: AlaskaFinanceProbeOutsideGroup[];
    top_opposing_groups: AlaskaFinanceProbeOutsideGroup[];
    top_supporting_industries: AlaskaFinanceProbeIndustry[];
    top_opposing_industries: AlaskaFinanceProbeIndustry[];
    matched_expenditure_row_count: number;
    included_expenditure_row_count: number;
    skipped_expenditure_row_count: number;
    matched_contribution_row_count: number;
    included_contribution_row_count: number;
    skipped_contribution_row_count: number;
  };
};

const DEFAULT_LIMIT = 5;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const INDUSTRY_SLUGS = new Set<string>(FINANCE_INDUSTRY_SLUGS);

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
    throw new Error(`Missing required ${name}`);
  }
  return value;
}

function parseRequiredPositiveIntegerFlag(args: readonly string[], name: string): number {
  const raw = parseRequiredFlag(args, name);
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parsePositiveIntegerFlag(args: readonly string[], name: string, fallback: number): number {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseOptionalPositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseOptionalNonNegativeIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^(?:0|[1-9]\d*)$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseNonNegativeNumberFlag(args: readonly string[], name: string, fallback: number): number {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return fallback;
  }
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseDataSourceMode(args: readonly string[]): AlaskaApocDataSourceMode {
  const liveFlag = args.includes("--live");
  const csvFlag = args.includes("--csv");
  if (liveFlag && csvFlag) {
    throw new Error("Provide either --live or --csv, not both");
  }
  const rawMode = parseFlagValue(args, "--data-source");
  if (rawMode !== null) {
    if (liveFlag || csvFlag) {
      throw new Error("Provide --data-source or --live/--csv, not both");
    }
    if (rawMode !== "csv" && rawMode !== "live") {
      throw new Error(`Invalid --data-source value: ${rawMode}`);
    }
    return rawMode;
  }
  return liveFlag ? "live" : "csv";
}

export function parseProbeAlaskaCandidateFinanceArgs(args: readonly string[]): AlaskaFinanceProbeArgs {
  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear: parseRequiredPositiveIntegerFlag(args, "--year"),
    dataSourceMode: parseDataSourceMode(args),
    incomeCsvPath: parseFlagValue(args, "--income-csv"),
    independentExpendituresCsvPath: parseFlagValue(args, "--ie-expenditures-csv"),
    independentContributionsCsvPath: parseFlagValue(args, "--ie-contributions-csv"),
    incomeUrl: parseFlagValue(args, "--income-url"),
    independentExpendituresUrl: parseFlagValue(args, "--ie-expenditures-url"),
    independentContributionsUrl: parseFlagValue(args, "--ie-contributions-url"),
    timeoutMs: parseOptionalPositiveIntegerFlag(args, "--timeout-ms"),
    retryCount: parseOptionalNonNegativeIntegerFlag(args, "--retry-count"),
    retryDelayMs: parseOptionalNonNegativeIntegerFlag(args, "--retry-delay-ms"),
    requestSpacingMs: parseOptionalNonNegativeIntegerFlag(args, "--request-spacing-ms"),
    candidateFilerId: parseFlagValue(args, "--candidate-filer-id"),
    candidateFilerName: parseFlagValue(args, "--candidate-filer-name"),
    limit: parsePositiveIntegerFlag(args, "--limit", DEFAULT_LIMIT),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount", DEFAULT_MIN_INDUSTRY_AMOUNT),
  };
}

async function loadDatasets(args: AlaskaFinanceProbeArgs): Promise<AlaskaFinanceProbeDatasets> {
  const loaded = await loadAlaskaApocFinanceData(
    {
      mode: args.dataSourceMode,
      incomeCsvPath: args.incomeCsvPath ?? undefined,
      independentExpendituresCsvPath: args.independentExpendituresCsvPath ?? undefined,
      independentContributionsCsvPath: args.independentContributionsCsvPath ?? undefined,
      incomeUrl: args.incomeUrl ?? undefined,
      independentExpendituresUrl: args.independentExpendituresUrl ?? undefined,
      independentContributionsUrl: args.independentContributionsUrl ?? undefined,
      timeoutMs: args.timeoutMs,
      retryCount: args.retryCount,
      retryDelayMs: args.retryDelayMs,
      requestSpacingMs: args.requestSpacingMs,
    },
    { logger: console }
  );

  return loaded.apocData;
}

function mapBreakdown(row: AlaskaFinanceDirectBreakdown | AlaskaFinanceOutsideGroupBreakdown): AlaskaFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.contributorCount,
    source_url: row.sourceUrl,
  };
}

function mapOutsideGroup(group: AlaskaOutsideSpendingGroup): AlaskaFinanceProbeOutsideGroup {
  return {
    committee_id: group.committeeId,
    committee_name: group.committeeName,
    support_oppose: group.supportOppose,
    amount: group.amount,
    source_url: group.sourceUrl,
  };
}

function toIndustrySlug(value: string): FinanceIndustrySlug | null {
  return INDUSTRY_SLUGS.has(value) ? (value as FinanceIndustrySlug) : null;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function groupRowId(row: AlaskaApocIndependentContributionRow): string {
  return row.filerId.trim() || normalizeTextKey(row.filerName);
}

function buildIndustryEvidence(input: {
  industrySlug: FinanceIndustrySlug;
  supportOppose: AlaskaSupportOppose;
  groups: readonly AlaskaOutsideSpendingGroup[];
  contributionRows: readonly AlaskaApocIndependentContributionRow[];
  limit: number;
}): AlaskaFinanceProbeIndustryEvidence[] {
  const matchingGroups = input.groups.filter((group) => group.supportOppose === input.supportOppose);
  const groupById = new Map(matchingGroups.map((group) => [normalizeTextKey(group.committeeId), group]));
  const evidence = input.contributionRows
    .map((row) => {
      const group = groupById.get(normalizeTextKey(groupRowId(row)));
      if (!group || row.amount <= 0) {
        return null;
      }
      const classification = classifyAlaskaOutsideGroupContributionRow(row);
      if (classification.industrySlug !== input.industrySlug) {
        return null;
      }
      return {
        contributor_name: row.contributor,
        amount: row.amount,
        committee_id: group.committeeId,
        committee_name: group.committeeName,
        classified_label: classification.rawLabel,
        classification_source: classification.classificationSource,
        source_url: row.sourceUrl ?? group.sourceUrl,
      };
    })
    .filter((row): row is AlaskaFinanceProbeIndustryEvidence => row !== null);

  return evidence
    .sort((left, right) => right.amount - left.amount || left.contributor_name.localeCompare(right.contributor_name))
    .slice(0, input.limit);
}

function buildOutsideIndustries(input: {
  groups: readonly AlaskaOutsideSpendingGroup[];
  breakdowns: readonly AlaskaFinanceOutsideGroupBreakdown[];
  contributionRows: readonly AlaskaApocIndependentContributionRow[];
  limit: number;
}): AlaskaFinanceProbeIndustry[] {
  const industries = new Map<string, AlaskaFinanceProbeIndustry>();
  for (const breakdown of input.breakdowns) {
    if (breakdown.categoryType !== "industry") {
      continue;
    }
    const industrySlug = toIndustrySlug(breakdown.categoryName);
    if (!industrySlug) {
      continue;
    }
    const key = `${breakdown.supportOppose}\u0000${industrySlug}`;
    const evidence = buildIndustryEvidence({
      industrySlug,
      supportOppose: breakdown.supportOppose,
      groups: input.groups,
      contributionRows: input.contributionRows,
      limit: 5,
    });
    const existing = industries.get(key);
    if (existing) {
      existing.amount = Math.round((existing.amount + breakdown.amount) * 100) / 100;
      existing.contributor_count += breakdown.contributorCount;
      existing.source_url ??= breakdown.sourceUrl;
      existing.evidence.push(...evidence);
      continue;
    }
    industries.set(key, {
      category_name: industrySlug,
      industry_slug: industrySlug,
      support_oppose: breakdown.supportOppose,
      amount: breakdown.amount,
      contributor_count: breakdown.contributorCount,
      source_url: breakdown.sourceUrl,
      evidence,
    });
  }

  return [...industries.values()]
    .map((industry) => ({
      ...industry,
      evidence: industry.evidence
        .sort((left, right) => right.amount - left.amount || left.contributor_name.localeCompare(right.contributor_name))
        .slice(0, 5),
    }))
    .sort((left, right) => right.amount - left.amount || left.category_name.localeCompare(right.category_name))
    .slice(0, input.limit);
}

function resolveProbeCandidateMatch(input: {
  args: AlaskaFinanceProbeArgs;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
}): {
  status: "provided" | AlaskaCandidateCommitteeResolution["status"];
  candidateFilerId: string | null;
  candidateFilerName: string | null;
  matchedRowCount: number | null;
  source: "cli" | "apoc_csv" | null;
} {
  if (input.args.candidateFilerId || input.args.candidateFilerName) {
    return {
      status: "provided",
      candidateFilerId: input.args.candidateFilerId,
      candidateFilerName: input.args.candidateFilerName,
      matchedRowCount: null,
      source: "cli",
    };
  }

  const resolution = resolveAlaskaCandidateCommittee({
    candidateName: input.args.candidateName,
    electionYear: input.args.electionYear,
    incomeRows: input.incomeRows,
    sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
  });
  if (resolution.status !== "matched") {
    return {
      status: resolution.status,
      candidateFilerId: null,
      candidateFilerName: null,
      matchedRowCount: null,
      source: null,
    };
  }
  return {
    status: "matched",
    candidateFilerId: resolution.candidateFilerId,
    candidateFilerName: resolution.candidateFilerName,
    matchedRowCount: resolution.matchedRowCount,
    source: resolution.source,
  };
}

export async function runProbeAlaskaCandidateFinance(input: {
  args: AlaskaFinanceProbeArgs;
  datasets?: AlaskaFinanceProbeDatasets;
  now?: Date;
}): Promise<AlaskaFinanceProbeOutput> {
  const datasets = input.datasets ?? (await loadDatasets(input.args));
  const candidateMatch = resolveProbeCandidateMatch({
    args: input.args,
    incomeRows: datasets.incomeRows,
  });
  const candidateFilerId = input.args.candidateFilerId ?? candidateMatch.candidateFilerId;
  const candidateFilerName = input.args.candidateFilerName ?? candidateMatch.candidateFilerName;
  const direct = aggregateAlaskaDirectContributions({
    candidateName: input.args.candidateName,
    electionYear: input.args.electionYear,
    candidateFilerId,
    candidateFilerName,
    incomeRows: datasets.incomeRows,
    sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
    maxBreakdownsPerCategory: input.args.limit,
  });
  const outside = aggregateAlaskaOutsideSpending({
    candidateName: input.args.candidateName,
    electionYear: input.args.electionYear,
    expenditureRows: datasets.independentExpenditureRows,
    sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL,
    maxGroups: input.args.limit,
  });
  const groups = outside.summary?.groups ?? [];
  const outsideGroupBreakdowns = aggregateAlaskaOutsideGroupContributions({
    electionYear: input.args.electionYear,
    outsideGroups: groups,
    contributionRows: datasets.independentContributionRows,
    sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL,
    maxBreakdownsPerCategory: input.args.limit,
    minIndustryAmount: input.args.minIndustryAmount,
  });
  const industries = buildOutsideIndustries({
    groups,
    breakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    contributionRows: datasets.independentContributionRows,
    limit: input.args.limit,
  });

  return {
    type: "alaska_candidate_finance_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: direct.matchedContributionRowCount > 0 || outside.matchedExpenditureRowCount > 0,
    candidate_match: {
      status: candidateMatch.status,
      candidate_name: input.args.candidateName,
      candidate_filer_id: candidateFilerId,
      candidate_filer_name: candidateFilerName,
      matched_row_count: candidateMatch.matchedRowCount,
      source: candidateMatch.source,
    },
    direct_campaign: {
      total_direct_contributions: direct.summary.directContributionTotal,
      total_receipts: direct.summary.totalReceipts,
      top_occupations: direct.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "occupation")
        .map(mapBreakdown),
      contribution_size_buckets: direct.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contribution_size")
        .map(mapBreakdown),
      matched_row_count: direct.matchedContributionRowCount,
      included_row_count: direct.includedContributionRowCount,
      skipped_row_count: direct.skippedContributionRowCount,
    },
    outside_spending: {
      top_supporting_groups: groups.filter((group) => group.supportOppose === "support").map(mapOutsideGroup),
      top_opposing_groups: groups.filter((group) => group.supportOppose === "oppose").map(mapOutsideGroup),
      top_supporting_industries: industries.filter((industry) => industry.support_oppose === "support"),
      top_opposing_industries: industries.filter((industry) => industry.support_oppose === "oppose"),
      matched_expenditure_row_count: outside.matchedExpenditureRowCount,
      included_expenditure_row_count: outside.includedExpenditureRowCount,
      skipped_expenditure_row_count: outside.skippedExpenditureRowCount,
      matched_contribution_row_count: outsideGroupBreakdowns.matchedContributionRowCount,
      included_contribution_row_count: outsideGroupBreakdowns.includedContributionRowCount,
      skipped_contribution_row_count: outsideGroupBreakdowns.skippedContributionRowCount,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runProbeAlaskaCandidateFinance({
    args: parseProbeAlaskaCandidateFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Alaska candidate finance CSV probe failed:", message);
    process.exitCode = 1;
  });
}
