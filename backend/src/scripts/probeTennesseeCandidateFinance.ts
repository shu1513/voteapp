import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  FINANCE_INDUSTRY_SLUGS,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  searchAndResolveTennesseeCandidateCommittee,
  type TennesseeCandidateCommitteeResolution,
} from "../pipeline/tennesseeFinance/tennesseeCandidateCommitteeResolver.js";
import {
  loadTennesseeContributionDataForCandidate,
  type TennesseeCandidateFinanceContributionData,
} from "../pipeline/tennesseeFinance/tennesseeCandidateFinanceBatchSync.js";
import { aggregateTennesseeDirectContributions } from "../pipeline/tennesseeFinance/tennesseeDirectContributionAggregator.js";
import {
  aggregateTennesseeOutsideGroupContributions,
  type TennesseeFinanceOutsideGroupBreakdown,
} from "../pipeline/tennesseeFinance/tennesseeOutsideGroupContributionAggregator.js";
import {
  aggregateTennesseeOutsideSpending,
  type TennesseeOutsideSpendingGroup,
  type TennesseeSupportOppose,
} from "../pipeline/tennesseeFinance/tennesseeOutsideSpendingAggregator.js";
import type { TennesseeCampClientOptions } from "../pipeline/tennesseeFinance/tennesseeCampClient.js";

type TennesseeFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  district: string | null;
  limit: number;
  minIndustryAmount: number;
  timeoutMs: number;
};

type TennesseeFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number | null;
  source_url: string | null;
};

type TennesseeFinanceProbeOutsideGroup = {
  committee_key: string;
  committee_name: string;
  support_oppose: TennesseeSupportOppose;
  amount: number;
  expenditure_count: number;
  source_url: string | null;
};

type TennesseeFinanceProbeIndustryEvidence = {
  organization_name: string;
  organization_type: "donor" | "employer";
  amount: number;
  contributor_count: number | null;
  committee_key: string;
  committee_name: string;
  source_url: string | null;
};

type TennesseeFinanceProbeIndustry = TennesseeFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: TennesseeSupportOppose;
  evidence: TennesseeFinanceProbeIndustryEvidence[];
};

type TennesseeFinanceProbeClassifiableOutsideBreakdown = TennesseeFinanceOutsideGroupBreakdown & {
  categoryType: "donor" | "employer";
};

type TennesseeFinanceProbeOutput = {
  type: "tennessee_candidate_finance_live_probe";
  ts: string;
  args: TennesseeFinanceProbeArgs;
  ok: boolean;
  resolution: TennesseeCandidateCommitteeResolution;
  direct_campaign: {
    total_raised: number | null;
    top_occupations: TennesseeFinanceProbeBreakdown[];
    contribution_size_buckets: TennesseeFinanceProbeBreakdown[];
  };
  outside_spending: {
    support_total: number | null;
    oppose_total: number | null;
    top_supporting_groups: TennesseeFinanceProbeOutsideGroup[];
    top_opposing_groups: TennesseeFinanceProbeOutsideGroup[];
    top_supporting_industries: TennesseeFinanceProbeIndustry[];
    top_opposing_industries: TennesseeFinanceProbeIndustry[];
    matched_outside_contribution_row_count: number;
    included_outside_contribution_row_count: number;
    skipped_outside_contribution_row_count: number;
  };
  source_row_counts: {
    contribution_rows: number;
    expenditure_rows: number;
    outside_group_contribution_rows: number;
  };
};

type TennesseeFinanceProbeClient = {
  resolveCandidateCommittee: typeof searchAndResolveTennesseeCandidateCommittee;
  loadContributionDataForCandidate: typeof loadTennesseeContributionDataForCandidate;
};

const DEFAULT_LIMIT = 5;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const INDUSTRY_SLUGS = new Set<string>(FINANCE_INDUSTRY_SLUGS);

const DEFAULT_CLIENT: TennesseeFinanceProbeClient = {
  resolveCandidateCommittee: searchAndResolveTennesseeCandidateCommittee,
  loadContributionDataForCandidate: loadTennesseeContributionDataForCandidate,
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

function parseRequiredFlag(args: readonly string[], name: string): string {
  const value = parseFlagValue(args, name);
  if (!value) {
    throw new Error(`Missing required ${name}`);
  }
  return value;
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

function parseRequiredPositiveIntegerFlag(args: readonly string[], name: string): number {
  const raw = parseRequiredFlag(args, name);
  if (!/^[1-9]\d*$/.test(raw)) {
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

function parseOfficeScope(value: string | null): TennesseeFinanceProbeArgs["officeScope"] {
  const normalized = value?.trim() || "statewide";
  if (normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower") {
    return normalized;
  }
  throw new Error(`Invalid --scope value: ${value}`);
}

export function parseProbeTennesseeCandidateFinanceArgs(args: readonly string[]): TennesseeFinanceProbeArgs {
  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear: parseRequiredPositiveIntegerFlag(args, "--year"),
    officeScope: parseOfficeScope(parseFlagValue(args, "--scope")),
    officeName: parseRequiredFlag(args, "--office"),
    district: parseFlagValue(args, "--district"),
    limit: parsePositiveIntegerFlag(args, "--limit", DEFAULT_LIMIT),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount", DEFAULT_MIN_INDUSTRY_AMOUNT),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS),
  };
}

function emptyOutput(input: {
  args: TennesseeFinanceProbeArgs;
  resolution: TennesseeCandidateCommitteeResolution;
  now?: Date;
}): TennesseeFinanceProbeOutput {
  return {
    type: "tennessee_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: false,
    resolution: input.resolution,
    direct_campaign: {
      total_raised: null,
      top_occupations: [],
      contribution_size_buckets: [],
    },
    outside_spending: {
      support_total: null,
      oppose_total: null,
      top_supporting_groups: [],
      top_opposing_groups: [],
      top_supporting_industries: [],
      top_opposing_industries: [],
      matched_outside_contribution_row_count: 0,
      included_outside_contribution_row_count: 0,
      skipped_outside_contribution_row_count: 0,
    },
    source_row_counts: {
      contribution_rows: 0,
      expenditure_rows: 0,
      outside_group_contribution_rows: 0,
    },
  };
}

function mapBreakdown(row: {
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
}): TennesseeFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.contributorCount ?? null,
    source_url: row.sourceUrl ?? null,
  };
}

function mapOutsideGroup(group: TennesseeOutsideSpendingGroup): TennesseeFinanceProbeOutsideGroup {
  return {
    committee_key: group.committeeKey,
    committee_name: group.committeeName,
    support_oppose: group.supportOppose,
    amount: group.amount,
    expenditure_count: group.expenditureCount,
    source_url: group.sourceUrl ?? null,
  };
}

function toIndustrySlug(value: string): FinanceIndustrySlug | null {
  return INDUSTRY_SLUGS.has(value) ? (value as FinanceIndustrySlug) : null;
}

function buildOutsideIndustries(input: {
  groups: readonly TennesseeOutsideSpendingGroup[];
  breakdowns: readonly TennesseeFinanceOutsideGroupBreakdown[];
  limit: number;
}): TennesseeFinanceProbeIndustry[] {
  const groupNames = new Map(
    input.groups.map((group) => [`${group.committeeKey}\u0000${group.supportOppose}`, group.committeeName])
  );
  const labelBreakdowns = input.breakdowns.filter(
    (breakdown): breakdown is TennesseeFinanceProbeClassifiableOutsideBreakdown =>
      breakdown.categoryType === "donor" || breakdown.categoryType === "employer"
  );
  const industries = new Map<string, TennesseeFinanceProbeIndustry>();

  for (const breakdown of input.breakdowns) {
    if (breakdown.categoryType !== "industry") {
      continue;
    }
    const industrySlug = toIndustrySlug(breakdown.categoryName);
    if (!industrySlug) {
      continue;
    }
    const key = `${breakdown.supportOppose}\u0000${industrySlug}`;
    const evidence = labelBreakdowns
      .filter((label) => label.committeeKey === breakdown.committeeKey && label.supportOppose === breakdown.supportOppose)
      .filter(
        (label) => classifyFinanceLabel({ rawLabel: label.categoryName, labelType: label.categoryType }).industrySlug === industrySlug
      )
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, 5)
      .map((label) => ({
        organization_name: label.categoryName,
        organization_type: label.categoryType,
        amount: label.amount,
        contributor_count: label.contributorCount,
        committee_key: label.committeeKey,
        committee_name: groupNames.get(`${label.committeeKey}\u0000${label.supportOppose}`) ?? label.committeeKey,
        source_url: label.sourceUrl ?? breakdown.sourceUrl ?? null,
      }));

    const existing = industries.get(key);
    if (!existing) {
      industries.set(key, {
        category_name: industrySlug,
        industry_slug: industrySlug,
        support_oppose: breakdown.supportOppose,
        amount: breakdown.amount,
        contributor_count: breakdown.contributorCount,
        source_url: breakdown.sourceUrl,
        evidence,
      });
      continue;
    }
    existing.amount = Math.round((existing.amount + breakdown.amount) * 100) / 100;
    existing.contributor_count = (existing.contributor_count ?? 0) + breakdown.contributorCount;
    existing.source_url ??= breakdown.sourceUrl;
    existing.evidence.push(...evidence);
  }

  return [...industries.values()]
    .map((industry) => ({
      ...industry,
      evidence: industry.evidence
        .sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name))
        .slice(0, 5),
    }))
    .sort((left, right) => right.amount - left.amount || left.category_name.localeCompare(right.category_name))
    .slice(0, input.limit);
}

function clientOptions(args: TennesseeFinanceProbeArgs): TennesseeCampClientOptions {
  return { timeoutMs: args.timeoutMs };
}

export async function runProbeTennesseeCandidateFinance(input: {
  args: TennesseeFinanceProbeArgs;
  client?: Partial<TennesseeFinanceProbeClient>;
  now?: Date;
}): Promise<TennesseeFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const options = clientOptions(input.args);
  const resolution = await client.resolveCandidateCommittee(
    {
      candidateName: input.args.candidateName,
      electionYear: input.args.electionYear,
      officeScope: input.args.officeScope,
      officeName: input.args.officeName,
      district: input.args.district,
    },
    options
  );

  if (resolution.status !== "matched") {
    return emptyOutput({ args: input.args, resolution, now: input.now });
  }

  const data: TennesseeCandidateFinanceContributionData = await client.loadContributionDataForCandidate(
    {
      candidateName: input.args.candidateName,
      ownerName: resolution.ownerName,
      electionYear: input.args.electionYear,
      clientOptions: options,
    }
  );

  const directFinance = aggregateTennesseeDirectContributions({
    candidate: {
      ownerName: resolution.ownerName,
      candidateName: input.args.candidateName,
    },
    electionYear: input.args.electionYear,
    contributions: data.contributions,
    sourceUrl: data.sourceUrl ?? resolution.sourceUrl ?? null,
    maxBreakdownsPerCategory: input.args.limit,
  });
  const outsideFinance = aggregateTennesseeOutsideSpending({
    candidateName: input.args.candidateName,
    ownerName: resolution.ownerName,
    electionYear: input.args.electionYear,
    expenditureRecords: data.expenditures,
    sourceUrl: data.expenditureSourceUrl ?? data.sourceUrl ?? null,
    maxGroups: input.args.limit,
  });
  const outsideGroups = outsideFinance.summary?.groups ?? [];
  const outsideGroupBreakdowns = aggregateTennesseeOutsideGroupContributions({
    electionYear: input.args.electionYear,
    outsideGroups,
    contributionRecords: data.outsideGroupContributionRecords,
    sourceUrl: data.outsideContributionSourceUrl ?? data.sourceUrl ?? null,
    maxBreakdownsPerCategory: input.args.limit,
    minIndustryAmount: input.args.minIndustryAmount,
  });
  const outsideIndustries = buildOutsideIndustries({
    groups: outsideGroups,
    breakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    limit: input.args.limit,
  });
  const directBreakdowns = directFinance.directBreakdowns;

  return {
    type: "tennessee_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: true,
    resolution,
    direct_campaign: {
      total_raised: directFinance.summary.totalReceipts,
      top_occupations: directBreakdowns.filter((row) => row.categoryType === "occupation").map(mapBreakdown),
      contribution_size_buckets: directBreakdowns
        .filter((row) => row.categoryType === "contribution_size")
        .map(mapBreakdown),
    },
    outside_spending: {
      support_total: outsideFinance.summary?.supportTotal ?? null,
      oppose_total: outsideFinance.summary?.opposeTotal ?? null,
      top_supporting_groups: outsideGroups.filter((group) => group.supportOppose === "support").map(mapOutsideGroup),
      top_opposing_groups: outsideGroups.filter((group) => group.supportOppose === "oppose").map(mapOutsideGroup),
      top_supporting_industries: outsideIndustries.filter((industry) => industry.support_oppose === "support"),
      top_opposing_industries: outsideIndustries.filter((industry) => industry.support_oppose === "oppose"),
      matched_outside_contribution_row_count: outsideGroupBreakdowns.matchedContributionRowCount,
      included_outside_contribution_row_count: outsideGroupBreakdowns.includedContributionRowCount,
      skipped_outside_contribution_row_count: outsideGroupBreakdowns.skippedContributionRowCount,
    },
    source_row_counts: {
      contribution_rows: data.contributions.length,
      expenditure_rows: data.expenditures.length,
      outside_group_contribution_rows: data.outsideGroupContributionRecords.length,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseProbeTennesseeCandidateFinanceArgs(process.argv.slice(2));
  const output = await runProbeTennesseeCandidateFinance({ args });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Tennessee candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
