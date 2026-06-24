import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  FINANCE_INDUSTRY_SLUGS,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  normalizeMassachusettsCandidateNameKeys,
  searchAndResolveMassachusettsCandidateCommittee,
  type MassachusettsCandidateCommitteeResolution,
} from "../pipeline/massachusettsFinance/massachusettsCandidateCommitteeResolver.js";
import { aggregateMassachusettsDirectContributions } from "../pipeline/massachusettsFinance/massachusettsDirectContributionAggregator.js";
import {
  aggregateMassachusettsOutsideGroupContributions,
  type MassachusettsFinanceOutsideGroupBreakdown,
} from "../pipeline/massachusettsFinance/massachusettsOutsideGroupContributionAggregator.js";
import {
  aggregateMassachusettsOutsideSpending,
  type MassachusettsOutsideSpendingGroup,
  type MassachusettsSupportOppose,
} from "../pipeline/massachusettsFinance/massachusettsOutsideSpendingAggregator.js";
import {
  buildMassachusettsOcpfContributionItemsUrl,
  buildMassachusettsOcpfIepacReportSummariesUrl,
  getMassachusettsOcpfContributionItems,
  getMassachusettsOcpfIepacReportSummaries,
  getMassachusettsOcpfReportDetail,
  type MassachusettsOcpfClientOptions,
  type MassachusettsOcpfContributionItem,
  type MassachusettsOcpfIepacReportSummary,
  type MassachusettsOcpfReportDetail,
} from "../pipeline/massachusettsFinance/massachusettsOcpfClient.js";

type MassachusettsFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  district: string | null;
  limit: number;
  contributionItemLimit: number | undefined;
  iepacReportLimit: number;
  minIndustryAmount: number;
  timeoutMs: number;
};

type MassachusettsFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type MassachusettsFinanceProbeOutsideGroup = {
  iepac_cpf_id: string;
  iepac_name: string;
  support_oppose: MassachusettsSupportOppose;
  amount: number;
  source_url: string | null;
};

type MassachusettsFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number;
  iepac_cpf_id: string;
  iepac_name: string;
  source_url: string | null;
};

type MassachusettsFinanceProbeIndustry = MassachusettsFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: MassachusettsSupportOppose;
  evidence: MassachusettsFinanceProbeIndustryEvidence[];
};

type MassachusettsFinanceProbeOutput = {
  type: "massachusetts_candidate_finance_live_probe";
  ts: string;
  args: MassachusettsFinanceProbeArgs;
  ok: boolean;
  resolution: MassachusettsCandidateCommitteeResolution;
  direct_campaign: {
    top_occupations: MassachusettsFinanceProbeBreakdown[];
    contribution_size_buckets: MassachusettsFinanceProbeBreakdown[];
  };
  outside_spending: {
    top_supporting_groups: MassachusettsFinanceProbeOutsideGroup[];
    top_opposing_groups: MassachusettsFinanceProbeOutsideGroup[];
    top_supporting_industries: MassachusettsFinanceProbeIndustry[];
    top_opposing_industries: MassachusettsFinanceProbeIndustry[];
    iepac_report_count: number;
    iepac_report_detail_count: number;
  };
};

type MassachusettsFinanceProbeClient = {
  resolveCandidateCommittee: (
    input: Omit<Parameters<typeof searchAndResolveMassachusettsCandidateCommittee>[0], "filers">,
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsCandidateCommitteeResolution>;
  getContributionItems: (
    input: {
      candidateCpfId: string;
      electionYear: number;
      limit?: number;
    },
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfContributionItem[]>;
  getIepacReportSummaries: (
    electionYear: number,
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfIepacReportSummary[]>;
  getReportDetail: (
    input: { reportId: number },
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfReportDetail>;
};

const DEFAULT_LIMIT = 5;
const DEFAULT_IEPAC_REPORT_LIMIT = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const INDUSTRY_SLUGS = new Set<string>(FINANCE_INDUSTRY_SLUGS);

const DEFAULT_CLIENT: MassachusettsFinanceProbeClient = {
  resolveCandidateCommittee: searchAndResolveMassachusettsCandidateCommittee,
  getContributionItems: getMassachusettsOcpfContributionItems,
  getIepacReportSummaries: getMassachusettsOcpfIepacReportSummaries,
  getReportDetail: getMassachusettsOcpfReportDetail,
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

function parseOfficeScope(value: string | null): MassachusettsFinanceProbeArgs["officeScope"] {
  const normalized = value?.trim() || "statewide";
  if (normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower") {
    return normalized;
  }
  throw new Error(`Invalid --scope value: ${value}`);
}

export function parseProbeMassachusettsCandidateFinanceArgs(
  args: readonly string[]
): MassachusettsFinanceProbeArgs {
  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear: parseRequiredPositiveIntegerFlag(args, "--year"),
    officeScope: parseOfficeScope(parseFlagValue(args, "--scope")),
    officeName: parseRequiredFlag(args, "--office"),
    district: parseFlagValue(args, "--district"),
    limit: parsePositiveIntegerFlag(args, "--limit", DEFAULT_LIMIT),
    contributionItemLimit: parseOptionalPositiveIntegerFlag(args, "--contribution-limit"),
    iepacReportLimit: parsePositiveIntegerFlag(args, "--iepac-report-limit", DEFAULT_IEPAC_REPORT_LIMIT),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount", DEFAULT_MIN_INDUSTRY_AMOUNT),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS),
  };
}

function emptyOutput(input: {
  args: MassachusettsFinanceProbeArgs;
  resolution: MassachusettsCandidateCommitteeResolution;
  now?: Date;
}): MassachusettsFinanceProbeOutput {
  return {
    type: "massachusetts_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: false,
    resolution: input.resolution,
    direct_campaign: {
      top_occupations: [],
      contribution_size_buckets: [],
    },
    outside_spending: {
      top_supporting_groups: [],
      top_opposing_groups: [],
      top_supporting_industries: [],
      top_opposing_industries: [],
      iepac_report_count: 0,
      iepac_report_detail_count: 0,
    },
  };
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

function reportMentionsCandidate(input: {
  report: MassachusettsOcpfIepacReportSummary;
  args: MassachusettsFinanceProbeArgs;
  resolution: Extract<MassachusettsCandidateCommitteeResolution, { status: "matched" }>;
}): boolean {
  const haystack = normalizeTextKey(
    [
      input.report.cpfId,
      input.report.committeeName,
      input.report.candidateListing,
      input.report.candidateSpendingBreakdown,
    ].join(" ")
  );
  if (haystack.includes(input.resolution.candidateCpfId)) {
    return true;
  }

  const candidateKeys = new Set([
    ...normalizeMassachusettsCandidateNameKeys(input.args.candidateName),
    ...normalizeMassachusettsCandidateNameKeys(input.resolution.filerName),
  ]);
  return [...candidateKeys].some((key) => key.length > 0 && haystack.includes(key));
}

function mapBreakdown(row: {
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
}): MassachusettsFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.contributorCount,
    source_url: row.sourceUrl ?? null,
  };
}

function mapOutsideGroup(group: MassachusettsOutsideSpendingGroup): MassachusettsFinanceProbeOutsideGroup {
  return {
    iepac_cpf_id: group.iepacCpfId,
    iepac_name: group.iepacName,
    support_oppose: group.supportOppose,
    amount: group.amount,
    source_url: group.sourceUrl ?? null,
  };
}

function toIndustrySlug(value: string): FinanceIndustrySlug | null {
  return INDUSTRY_SLUGS.has(value) ? (value as FinanceIndustrySlug) : null;
}

function buildOutsideIndustries(input: {
  groups: readonly MassachusettsOutsideSpendingGroup[];
  breakdowns: readonly MassachusettsFinanceOutsideGroupBreakdown[];
  limit: number;
}): MassachusettsFinanceProbeIndustry[] {
  const groupNames = new Map(input.groups.map((group) => [`${group.iepacCpfId}\u0000${group.supportOppose}`, group.iepacName]));
  const donorBreakdowns = input.breakdowns.filter((breakdown) => breakdown.categoryType === "donor");
  const industries = new Map<string, MassachusettsFinanceProbeIndustry>();

  for (const breakdown of input.breakdowns) {
    if (breakdown.categoryType !== "industry") {
      continue;
    }
    const industrySlug = toIndustrySlug(breakdown.categoryName);
    if (!industrySlug) {
      continue;
    }
    const key = `${breakdown.supportOppose}\u0000${industrySlug}`;
    const evidence = donorBreakdowns
      .filter((donor) => donor.iepacCpfId === breakdown.iepacCpfId && donor.supportOppose === breakdown.supportOppose)
      .filter((donor) => classifyFinanceLabel({ rawLabel: donor.categoryName, labelType: "donor" }).industrySlug === industrySlug)
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, 5)
      .map((donor) => ({
        organization_name: donor.categoryName,
        amount: donor.amount,
        contributor_count: donor.contributorCount,
        iepac_cpf_id: donor.iepacCpfId,
        iepac_name: groupNames.get(`${donor.iepacCpfId}\u0000${donor.supportOppose}`) ?? donor.iepacCpfId,
        source_url: donor.sourceUrl ?? breakdown.sourceUrl ?? null,
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
    existing.contributor_count += breakdown.contributorCount;
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

export async function runProbeMassachusettsCandidateFinance(input: {
  args: MassachusettsFinanceProbeArgs;
  client?: Partial<MassachusettsFinanceProbeClient>;
  now?: Date;
}): Promise<MassachusettsFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const clientOptions: MassachusettsOcpfClientOptions = { timeoutMs: input.args.timeoutMs };
  const resolution = await client.resolveCandidateCommittee(
    {
      candidateName: input.args.candidateName,
      officeScope: input.args.officeScope,
      officeName: input.args.officeName,
      electionYear: input.args.electionYear,
      district: input.args.district,
    },
    clientOptions
  );

  if (resolution.status !== "matched") {
    return emptyOutput({ args: input.args, resolution, now: input.now });
  }

  const [contributionItems, iepacReportSummaries] = await Promise.all([
    client.getContributionItems(
      {
        candidateCpfId: resolution.candidateCpfId,
        electionYear: input.args.electionYear,
        limit: input.args.contributionItemLimit,
      },
      clientOptions
    ),
    client.getIepacReportSummaries(input.args.electionYear, clientOptions),
  ]);
  const candidateIepacReportMatches = iepacReportSummaries.filter((report) =>
    reportMentionsCandidate({ report, args: input.args, resolution })
  );
  const candidateIepacReports = (
    candidateIepacReportMatches.length > 0 ? candidateIepacReportMatches : iepacReportSummaries
  ).slice(0, input.args.iepacReportLimit);
  const reportDetails = await Promise.all(
    candidateIepacReports.map((report) => client.getReportDetail({ reportId: report.reportId }, clientOptions))
  );

  const direct = aggregateMassachusettsDirectContributions({
    candidateCpfId: resolution.candidateCpfId,
    electionYear: input.args.electionYear,
    contributionItems,
    sourceUrl: buildMassachusettsOcpfContributionItemsUrl({
      candidateCpfId: resolution.candidateCpfId,
      electionYear: input.args.electionYear,
      limit: input.args.contributionItemLimit,
    }),
    maxBreakdownsPerCategory: input.args.limit,
  });
  const outside = aggregateMassachusettsOutsideSpending({
    candidateCpfId: resolution.candidateCpfId,
    electionYear: input.args.electionYear,
    reportDetails,
    sourceUrl: buildMassachusettsOcpfIepacReportSummariesUrl(input.args.electionYear),
    maxGroups: input.args.limit,
  });
  const outsideGroupBreakdowns = aggregateMassachusettsOutsideGroupContributions({
    electionYear: input.args.electionYear,
    outsideGroups: outside.summary?.groups ?? [],
    reportDetails,
    sourceUrl: buildMassachusettsOcpfIepacReportSummariesUrl(input.args.electionYear),
    maxBreakdownsPerCategory: input.args.limit,
    minIndustryAmount: input.args.minIndustryAmount,
  });

  const topOccupations = direct.directBreakdowns
    .filter((breakdown) => breakdown.categoryType === "occupation")
    .map(mapBreakdown)
    .slice(0, input.args.limit);
  const contributionSizeBuckets = direct.directBreakdowns
    .filter((breakdown) => breakdown.categoryType === "contribution_size")
    .map(mapBreakdown)
    .slice(0, input.args.limit);
  const groups = outside.summary?.groups ?? [];
  const industries = buildOutsideIndustries({
    groups,
    breakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    limit: input.args.limit,
  });

  return {
    type: "massachusetts_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: true,
    resolution,
    direct_campaign: {
      top_occupations: topOccupations,
      contribution_size_buckets: contributionSizeBuckets,
    },
    outside_spending: {
      top_supporting_groups: groups.filter((group) => group.supportOppose === "support").map(mapOutsideGroup),
      top_opposing_groups: groups.filter((group) => group.supportOppose === "oppose").map(mapOutsideGroup),
      top_supporting_industries: industries.filter((industry) => industry.support_oppose === "support"),
      top_opposing_industries: industries.filter((industry) => industry.support_oppose === "oppose"),
      iepac_report_count: iepacReportSummaries.length,
      iepac_report_detail_count: reportDetails.length,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runProbeMassachusettsCandidateFinance({
    args: parseProbeMassachusettsCandidateFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Massachusetts candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
