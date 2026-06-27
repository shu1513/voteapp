import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  aggregateIllinoisDirectContributions,
  aggregateIllinoisOutsideGroupContributions,
  aggregateIllinoisOutsideSpending,
  type IllinoisFinanceDirectBreakdown,
  type IllinoisFinanceOutsideGroupBreakdown,
  type IllinoisOutsideSpendingGroup,
} from "../pipeline/illinoisFinance/illinoisFinanceAggregators.js";
import {
  fetchIllinoisSbeCandidateContributionRecords,
  fetchIllinoisSbeCommitteeContributionRecords,
  fetchIllinoisSbeIndependentExpenditureRecords,
  ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
  ILLINOIS_SBE_CONTRIBUTION_COMMITTEE_SEARCH_URL,
  ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL,
  type IllinoisSbeClientOptions,
  type IllinoisSbeContributionRecord,
  type IllinoisSbeExpenditureRecord,
  type IllinoisSbeSupportOppose,
} from "../pipeline/illinoisFinance/illinoisSbeClient.js";

type IllinoisFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeName: string;
  fromDate: string;
  toDate: string;
  limit: number;
  funderLimit: number;
  minIndustryAmount: number;
  timeoutMs: number;
};

type IllinoisFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type IllinoisFinanceProbeOutsideGroup = {
  committee_key: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  expenditure_count: number;
  source_url: string | null;
};

type IllinoisFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number;
  committee_key: string;
  committee_name: string;
  source_url: string | null;
};

type IllinoisFinanceProbeIndustry = IllinoisFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: "support" | "oppose";
  evidence: IllinoisFinanceProbeIndustryEvidence[];
};

type IllinoisFinanceProbeOutput = {
  type: "illinois_candidate_finance_live_probe";
  ts: string;
  args: IllinoisFinanceProbeArgs;
  ok: boolean;
  search: {
    candidate_first_name: string | null;
    candidate_last_name: string;
    direct_contributions_source_url: string;
    outside_expenditures_source_url: string;
    outside_group_contributions_source_url: string;
  };
  direct_campaign: {
    top_occupations: IllinoisFinanceProbeBreakdown[];
    contribution_size_buckets: IllinoisFinanceProbeBreakdown[];
  };
  outside_spending: {
    top_supporting_groups: IllinoisFinanceProbeOutsideGroup[];
    top_opposing_groups: IllinoisFinanceProbeOutsideGroup[];
    top_supporting_industries: IllinoisFinanceProbeIndustry[];
    top_opposing_industries: IllinoisFinanceProbeIndustry[];
    skipped_outside_funder_lookup_count: number;
  };
};

type IllinoisFinanceProbeClient = {
  getCandidateContributions(
    input: Parameters<typeof fetchIllinoisSbeCandidateContributionRecords>[0],
    options: IllinoisSbeClientOptions
  ): Promise<IllinoisSbeContributionRecord[]>;
  getCommitteeContributions(
    input: Parameters<typeof fetchIllinoisSbeCommitteeContributionRecords>[0],
    options: IllinoisSbeClientOptions
  ): Promise<IllinoisSbeContributionRecord[]>;
  getIndependentExpenditures(
    input: Parameters<typeof fetchIllinoisSbeIndependentExpenditureRecords>[0],
    options: IllinoisSbeClientOptions
  ): Promise<IllinoisSbeExpenditureRecord[]>;
};

const DEFAULT_LIMIT = 5;
const DEFAULT_FUNDER_LIMIT = 20;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;

const DEFAULT_CLIENT: IllinoisFinanceProbeClient = {
  getCandidateContributions: fetchIllinoisSbeCandidateContributionRecords,
  getCommitteeContributions: fetchIllinoisSbeCommitteeContributionRecords,
  getIndependentExpenditures: fetchIllinoisSbeIndependentExpenditureRecords,
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

function defaultFromDate(electionYear: number): string {
  return `1/1/${electionYear - 1}`;
}

function defaultToDate(electionYear: number): string {
  return `12/31/${electionYear}`;
}

function parseDateFlag(args: readonly string[], name: string, fallback: string): string {
  const raw = parseFlagValue(args, name);
  const value = raw ?? fallback;
  if (!/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}; use m/d/yyyy`);
  }
  return value;
}

export function parseProbeIllinoisCandidateFinanceArgs(args: readonly string[]): IllinoisFinanceProbeArgs {
  const electionYear = parseRequiredPositiveIntegerFlag(args, "--year");
  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear,
    officeName: parseRequiredFlag(args, "--office"),
    fromDate: parseDateFlag(args, "--from-date", defaultFromDate(electionYear)),
    toDate: parseDateFlag(args, "--to-date", defaultToDate(electionYear)),
    limit: parsePositiveIntegerFlag(args, "--limit", DEFAULT_LIMIT),
    funderLimit: parsePositiveIntegerFlag(args, "--funder-limit", DEFAULT_FUNDER_LIMIT),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount", DEFAULT_MIN_INDUSTRY_AMOUNT),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS),
  };
}

function splitCandidateName(candidateName: string): { firstName: string | null; lastName: string } {
  const normalized = candidateName.replace(/\s+/g, " ").trim();
  if (!normalized) {
    throw new Error("candidate name is required");
  }
  const commaIndex = normalized.indexOf(",");
  if (commaIndex > -1) {
    const lastName = normalized.slice(0, commaIndex).trim();
    const firstName = normalized.slice(commaIndex + 1).trim();
    if (!lastName) {
      throw new Error("candidate last name is required");
    }
    return { firstName: firstName || null, lastName };
  }
  const parts = normalized.split(" ");
  if (parts.length === 1) {
    return { firstName: null, lastName: parts[0]! };
  }
  return {
    firstName: parts.slice(0, -1).join(" "),
    lastName: parts[parts.length - 1]!,
  };
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function probeBreakdownFromDirect(row: IllinoisFinanceDirectBreakdown): IllinoisFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: roundCurrency(row.amount),
    contributor_count: row.contributorCount,
    source_url: row.sourceUrl,
  };
}

function probeOutsideGroupFromAggregate(group: IllinoisOutsideSpendingGroup): IllinoisFinanceProbeOutsideGroup {
  return {
    committee_key: group.committeeKey,
    committee_name: group.committeeName,
    support_oppose: group.supportOppose,
    amount: roundCurrency(group.amount),
    expenditure_count: group.expenditureCount,
    source_url: group.sourceUrl,
  };
}

function sourceUrlForCommittee(
  groups: readonly IllinoisOutsideSpendingGroup[],
  committeeKey: string,
  supportOppose: IllinoisSbeSupportOppose
): string | null {
  return groups.find((group) => group.committeeKey === committeeKey && group.supportOppose === supportOppose)?.sourceUrl ?? null;
}

function committeeNameForKey(groups: readonly IllinoisOutsideSpendingGroup[], committeeKey: string): string {
  return groups.find((group) => group.committeeKey === committeeKey)?.committeeName ?? committeeKey;
}

function buildProbeIndustries(input: {
  groups: readonly IllinoisOutsideSpendingGroup[];
  breakdowns: readonly IllinoisFinanceOutsideGroupBreakdown[];
  limit: number;
  evidenceLimit: number;
}): IllinoisFinanceProbeIndustry[] {
  const donors = input.breakdowns.filter((breakdown) => breakdown.categoryType === "donor");
  return input.breakdowns
    .filter((breakdown) => breakdown.categoryType === "industry")
    .map((industry) => {
      const evidence = donors
        .filter((donor) => donor.committeeKey === industry.committeeKey && donor.supportOppose === industry.supportOppose)
        .filter((donor) => classifyFinanceLabel({ rawLabel: donor.categoryName, labelType: "donor" }).industrySlug === industry.categoryName)
        .map((donor) => ({
          organization_name: donor.categoryName,
          amount: roundCurrency(donor.amount),
          contributor_count: donor.contributorCount,
          committee_key: donor.committeeKey,
          committee_name: committeeNameForKey(input.groups, donor.committeeKey),
          source_url: donor.sourceUrl ?? sourceUrlForCommittee(input.groups, donor.committeeKey, donor.supportOppose),
        }))
        .sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name))
        .slice(0, input.evidenceLimit);

      return {
        category_name: industry.categoryName,
        industry_slug: industry.categoryName as FinanceIndustrySlug,
        support_oppose: industry.supportOppose,
        amount: roundCurrency(industry.amount),
        contributor_count: industry.contributorCount,
        source_url: industry.sourceUrl ?? sourceUrlForCommittee(input.groups, industry.committeeKey, industry.supportOppose),
        evidence,
      };
    })
    .sort((left, right) => right.amount - left.amount || left.category_name.localeCompare(right.category_name))
    .slice(0, input.limit);
}

async function buildOutsideIndustryBreakdowns(input: {
  client: IllinoisFinanceProbeClient;
  groups: readonly IllinoisOutsideSpendingGroup[];
  electionYear: number;
  funderLimit: number;
  minIndustryAmount: number;
  clientOptions: IllinoisSbeClientOptions;
}): Promise<{ breakdowns: IllinoisFinanceOutsideGroupBreakdown[]; skippedOutsideFunderLookupCount: number }> {
  const contributionRecords: IllinoisSbeContributionRecord[] = [];
  let skippedOutsideFunderLookupCount = 0;
  const fetchedCommitteeKeys = new Set<string>();

  for (const group of input.groups) {
    if (fetchedCommitteeKeys.has(group.committeeKey)) {
      continue;
    }
    fetchedCommitteeKeys.add(group.committeeKey);
    try {
      const records = await input.client.getCommitteeContributions(
        {
          committeeName: group.committeeName,
          contributionType: "All Types",
        },
        input.clientOptions
      );
      contributionRecords.push(...records);
    } catch {
      skippedOutsideFunderLookupCount += 1;
    }
  }

  const aggregation = aggregateIllinoisOutsideGroupContributions({
    electionYear: input.electionYear,
    outsideGroups: input.groups,
    contributionRecords,
    maxBreakdownsPerCategory: input.funderLimit,
    minIndustryAmount: input.minIndustryAmount,
    sourceUrl: ILLINOIS_SBE_CONTRIBUTION_COMMITTEE_SEARCH_URL,
  });

  return {
    breakdowns: aggregation.outsideGroupBreakdowns,
    skippedOutsideFunderLookupCount,
  };
}

export async function runProbeIllinoisCandidateFinance(input: {
  args: IllinoisFinanceProbeArgs;
  client?: Partial<IllinoisFinanceProbeClient>;
  now?: Date;
}): Promise<IllinoisFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const clientOptions: IllinoisSbeClientOptions = { timeoutMs: input.args.timeoutMs };
  const candidate = splitCandidateName(input.args.candidateName);

  const [directRecords, supportRecords, opposeRecords] = await Promise.all([
    client.getCandidateContributions(
      {
        candidateLastName: candidate.lastName,
        candidateFirstName: candidate.firstName,
        electionYear: input.args.electionYear,
        contributionType: "Individual Contributions",
      },
      clientOptions
    ),
    client.getIndependentExpenditures(
      {
        candidateName: input.args.candidateName,
        office: input.args.officeName,
        supportOppose: "support",
        fromDate: input.args.fromDate,
        toDate: input.args.toDate,
      },
      clientOptions
    ),
    client.getIndependentExpenditures(
      {
        candidateName: input.args.candidateName,
        office: input.args.officeName,
        supportOppose: "oppose",
        fromDate: input.args.fromDate,
        toDate: input.args.toDate,
      },
      clientOptions
    ),
  ]);

  const directAggregation = aggregateIllinoisDirectContributions({
    electionYear: input.args.electionYear,
    contributionRecords: directRecords,
    maxBreakdownsPerCategory: input.args.limit,
    sourceUrl: ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
  });
  const outsideAggregation = aggregateIllinoisOutsideSpending({
    electionYear: input.args.electionYear,
    expenditureRecords: [...supportRecords, ...opposeRecords],
    maxGroups: input.args.limit * 2,
    sourceUrl: ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL,
  });
  const outsideGroups = outsideAggregation.summary?.groups ?? [];
  const industryBreakdowns = await buildOutsideIndustryBreakdowns({
    client,
    groups: outsideGroups,
    electionYear: input.args.electionYear,
    funderLimit: input.args.funderLimit,
    minIndustryAmount: input.args.minIndustryAmount,
    clientOptions,
  });
  const probeIndustries = buildProbeIndustries({
    groups: outsideGroups,
    breakdowns: industryBreakdowns.breakdowns,
    limit: input.args.limit,
    evidenceLimit: input.args.funderLimit,
  });

  return {
    type: "illinois_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: true,
    search: {
      candidate_first_name: candidate.firstName,
      candidate_last_name: candidate.lastName,
      direct_contributions_source_url: ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
      outside_expenditures_source_url: ILLINOIS_SBE_EXPENDITURE_ALL_SEARCH_URL,
      outside_group_contributions_source_url: ILLINOIS_SBE_CONTRIBUTION_COMMITTEE_SEARCH_URL,
    },
    direct_campaign: {
      top_occupations: directAggregation.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "occupation")
        .map(probeBreakdownFromDirect),
      contribution_size_buckets: directAggregation.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contribution_size")
        .map(probeBreakdownFromDirect),
    },
    outside_spending: {
      top_supporting_groups: outsideGroups
        .filter((group) => group.supportOppose === "support")
        .slice(0, input.args.limit)
        .map(probeOutsideGroupFromAggregate),
      top_opposing_groups: outsideGroups
        .filter((group) => group.supportOppose === "oppose")
        .slice(0, input.args.limit)
        .map(probeOutsideGroupFromAggregate),
      top_supporting_industries: probeIndustries
        .filter((industry) => industry.support_oppose === "support")
        .slice(0, input.args.limit),
      top_opposing_industries: probeIndustries
        .filter((industry) => industry.support_oppose === "oppose")
        .slice(0, input.args.limit),
      skipped_outside_funder_lookup_count: industryBreakdowns.skippedOutsideFunderLookupCount,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseProbeIllinoisCandidateFinanceArgs(process.argv.slice(2));
  const output = await runProbeIllinoisCandidateFinance({ args });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Illinois candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
