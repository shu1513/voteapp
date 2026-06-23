import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  searchAndResolveWashingtonCandidateCommittee,
  type WashingtonCandidateCommitteeResolution,
} from "../pipeline/washingtonFinance/washingtonCandidateCommitteeResolver.js";
import { toWashingtonPdcOfficeSearchInput } from "../pipeline/washingtonFinance/washingtonFinanceEligibleOffices.js";
import {
  getWashingtonPdcContributionSizeAggregates,
  getWashingtonPdcDirectOccupationAggregates,
  getWashingtonPdcIndependentExpenditureGroups,
  getWashingtonPdcSponsorOrganizationFunders,
  getWashingtonPdcSponsorSummaryByName,
  type WashingtonPdcAggregate,
  type WashingtonPdcCandidateSummary,
  type WashingtonPdcClientOptions,
  type WashingtonPdcIndependentSpendingGroup,
} from "../pipeline/washingtonFinance/washingtonPdcClient.js";

type WashingtonFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  legislativeDistrict: string | null;
  limit: number;
  funderLimit: number;
  minIndustryAmount: number;
  timeoutMs: number;
  appToken: string | null;
};

type WashingtonFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type WashingtonFinanceProbeOutsideGroup = {
  sponsor_id: string;
  sponsor_name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  expenditure_count: number;
  source_url: string | null;
};

type WashingtonFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number;
  sponsor_id: string;
  sponsor_name: string;
  source_url: string | null;
};

type WashingtonFinanceProbeIndustry = WashingtonFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: "support" | "oppose";
  evidence: WashingtonFinanceProbeIndustryEvidence[];
};

type WashingtonFinanceProbeOutput = {
  type: "washington_candidate_finance_live_probe";
  ts: string;
  args: Omit<WashingtonFinanceProbeArgs, "appToken"> & { appTokenProvided: boolean };
  ok: boolean;
  resolution: WashingtonCandidateCommitteeResolution;
  direct_campaign: {
    top_occupations: WashingtonFinanceProbeBreakdown[];
    contribution_size_buckets: WashingtonFinanceProbeBreakdown[];
  };
  outside_spending: {
    top_supporting_groups: WashingtonFinanceProbeOutsideGroup[];
    top_opposing_groups: WashingtonFinanceProbeOutsideGroup[];
    top_supporting_industries: WashingtonFinanceProbeIndustry[];
    top_opposing_industries: WashingtonFinanceProbeIndustry[];
    skipped_sponsor_funder_lookup_count: number;
  };
};

type WashingtonFinanceProbeClient = {
  resolveCandidateCommittee: typeof searchAndResolveWashingtonCandidateCommittee;
  getDirectOccupationAggregates: typeof getWashingtonPdcDirectOccupationAggregates;
  getContributionSizeAggregates: typeof getWashingtonPdcContributionSizeAggregates;
  getIndependentExpenditureGroups: typeof getWashingtonPdcIndependentExpenditureGroups;
  getSponsorSummaryByName: typeof getWashingtonPdcSponsorSummaryByName;
  getSponsorOrganizationFunders: typeof getWashingtonPdcSponsorOrganizationFunders;
};

const DEFAULT_LIMIT = 5;
const DEFAULT_FUNDER_LIMIT = 20;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;

const DEFAULT_CLIENT: WashingtonFinanceProbeClient = {
  resolveCandidateCommittee: searchAndResolveWashingtonCandidateCommittee,
  getDirectOccupationAggregates: getWashingtonPdcDirectOccupationAggregates,
  getContributionSizeAggregates: getWashingtonPdcContributionSizeAggregates,
  getIndependentExpenditureGroups: getWashingtonPdcIndependentExpenditureGroups,
  getSponsorSummaryByName: getWashingtonPdcSponsorSummaryByName,
  getSponsorOrganizationFunders: getWashingtonPdcSponsorOrganizationFunders,
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

function parsePositiveIntegerFlag(
  args: readonly string[],
  name: string,
  fallback: number
): number {
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

function parseNonNegativeNumberFlag(
  args: readonly string[],
  name: string,
  fallback: number
): number {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return fallback;
  }
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseOfficeScope(value: string | null): WashingtonFinanceProbeArgs["officeScope"] {
  const normalized = value?.trim() || "statewide";
  if (normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower") {
    return normalized;
  }
  throw new Error(`Invalid --scope value: ${value}`);
}

export function parseProbeWashingtonCandidateFinanceArgs(
  args: readonly string[]
): WashingtonFinanceProbeArgs {
  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear: parseRequiredPositiveIntegerFlag(args, "--year"),
    officeScope: parseOfficeScope(parseFlagValue(args, "--scope")),
    officeName: parseRequiredFlag(args, "--office"),
    legislativeDistrict: parseFlagValue(args, "--district"),
    limit: parsePositiveIntegerFlag(args, "--limit", DEFAULT_LIMIT),
    funderLimit: parsePositiveIntegerFlag(args, "--funder-limit", DEFAULT_FUNDER_LIMIT),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount", DEFAULT_MIN_INDUSTRY_AMOUNT),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS),
    appToken: parseFlagValue(args, "--app-token") ?? process.env.WASHINGTON_PDC_APP_TOKEN?.trim() ?? null,
  };
}

function publicProbeArgs(args: WashingtonFinanceProbeArgs): WashingtonFinanceProbeOutput["args"] {
  return {
    candidateName: args.candidateName,
    electionYear: args.electionYear,
    officeScope: args.officeScope,
    officeName: args.officeName,
    legislativeDistrict: args.legislativeDistrict,
    limit: args.limit,
    funderLimit: args.funderLimit,
    minIndustryAmount: args.minIndustryAmount,
    timeoutMs: args.timeoutMs,
    appTokenProvided: Boolean(args.appToken),
  };
}

function mapAggregate(row: WashingtonPdcAggregate): WashingtonFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.count,
    source_url: row.sourceUrl ?? null,
  };
}

function mapOutsideGroup(group: WashingtonPdcIndependentSpendingGroup): WashingtonFinanceProbeOutsideGroup {
  return {
    sponsor_id: group.sponsorId,
    sponsor_name: group.sponsorName,
    support_oppose: group.supportOppose,
    amount: group.amount,
    expenditure_count: group.expenditureCount,
    source_url: group.sourceUrl ?? null,
  };
}

function strictSponsorCommittee(
  summaries: readonly WashingtonPdcCandidateSummary[]
): { filerId: string; committeeId: string; committeeName: string; sourceUrl: string | null } | null {
  const usable = new Map<string, WashingtonPdcCandidateSummary>();
  for (const summary of summaries) {
    const filerId = summary.filerId.trim();
    const committeeId = summary.committeeId?.trim();
    if (!filerId || !committeeId) {
      continue;
    }
    usable.set(`${filerId}\u0000${committeeId}`, summary);
  }
  if (usable.size !== 1) {
    return null;
  }
  const summary = [...usable.values()][0]!;
  return {
    filerId: summary.filerId,
    committeeId: summary.committeeId ?? "",
    committeeName: summary.filerName,
    sourceUrl: summary.sourceUrl ?? null,
  };
}

function addIndustryEvidence(
  industries: Map<string, WashingtonFinanceProbeIndustry>,
  input: {
    industrySlug: FinanceIndustrySlug;
    group: WashingtonPdcIndependentSpendingGroup;
    funder: WashingtonPdcAggregate;
  }
): void {
  const key = `${input.group.supportOppose}\u0000${input.industrySlug}`;
  const existing = industries.get(key);
  const evidence: WashingtonFinanceProbeIndustryEvidence = {
    organization_name: input.funder.categoryName,
    amount: input.funder.amount,
    contributor_count: input.funder.count,
    sponsor_id: input.group.sponsorId,
    sponsor_name: input.group.sponsorName,
    source_url: input.funder.sourceUrl ?? input.group.sourceUrl ?? null,
  };
  if (!existing) {
    industries.set(key, {
      category_name: input.industrySlug,
      industry_slug: input.industrySlug,
      support_oppose: input.group.supportOppose,
      amount: input.funder.amount,
      contributor_count: input.funder.count,
      source_url: evidence.source_url,
      evidence: [evidence],
    });
    return;
  }
  existing.amount = Math.round((existing.amount + input.funder.amount) * 100) / 100;
  existing.contributor_count += input.funder.count;
  existing.source_url ??= evidence.source_url;
  existing.evidence.push(evidence);
}

async function buildOutsideIndustries(input: {
  client: WashingtonFinanceProbeClient;
  groups: readonly WashingtonPdcIndependentSpendingGroup[];
  electionYear: number;
  funderLimit: number;
  minIndustryAmount: number;
  clientOptions: WashingtonPdcClientOptions;
}): Promise<{
  industries: WashingtonFinanceProbeIndustry[];
  skippedSponsorFunderLookupCount: number;
}> {
  const industries = new Map<string, WashingtonFinanceProbeIndustry>();
  let skippedSponsorFunderLookupCount = 0;

  for (const group of input.groups) {
    const sponsorSummaries = await input.client.getSponsorSummaryByName(
      { sponsorName: group.sponsorName, electionYear: input.electionYear, limit: 20 },
      input.clientOptions
    );
    const sponsor = strictSponsorCommittee(sponsorSummaries);
    if (!sponsor) {
      skippedSponsorFunderLookupCount += 1;
      continue;
    }

    const funders = await input.client.getSponsorOrganizationFunders(
      {
        filerId: sponsor.filerId,
        committeeId: sponsor.committeeId,
        electionYear: input.electionYear,
        limit: input.funderLimit,
      },
      input.clientOptions
    );
    for (const funder of funders) {
      if (funder.amount < input.minIndustryAmount) {
        continue;
      }
      const classification = classifyFinanceLabel({ rawLabel: funder.categoryName, labelType: "donor" });
      if (!classification.industrySlug) {
        continue;
      }
      addIndustryEvidence(industries, { industrySlug: classification.industrySlug, group, funder });
    }
  }

  return {
    industries: [...industries.values()]
      .map((industry) => ({
        ...industry,
        evidence: industry.evidence
          .sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name))
          .slice(0, 5),
      }))
      .sort((left, right) => right.amount - left.amount || left.category_name.localeCompare(right.category_name)),
    skippedSponsorFunderLookupCount,
  };
}

export async function runProbeWashingtonCandidateFinance(input: {
  args: WashingtonFinanceProbeArgs;
  client?: Partial<WashingtonFinanceProbeClient>;
  now?: Date;
}): Promise<WashingtonFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const clientOptions: WashingtonPdcClientOptions = {
    timeoutMs: input.args.timeoutMs,
    ...(input.args.appToken ? { appToken: input.args.appToken } : {}),
  };
  const officeSearch = toWashingtonPdcOfficeSearchInput({
    officeScope: input.args.officeScope,
    officeCanonicalName: input.args.officeName,
    legislativeDistrict: input.args.legislativeDistrict,
  });
  const resolution = await client.resolveCandidateCommittee(
    {
      candidateName: input.args.candidateName,
      officeScope: input.args.officeScope,
      officeName: input.args.officeName,
      electionYear: input.args.electionYear,
      legislativeDistrict: input.args.legislativeDistrict,
    },
    clientOptions
  );

  if (resolution.status !== "matched") {
    return {
      type: "washington_candidate_finance_live_probe",
      ts: (input.now ?? new Date()).toISOString(),
      args: publicProbeArgs(input.args),
      ok: false,
      resolution,
      direct_campaign: {
        top_occupations: [],
        contribution_size_buckets: [],
      },
      outside_spending: {
        top_supporting_groups: [],
        top_opposing_groups: [],
        top_supporting_industries: [],
        top_opposing_industries: [],
        skipped_sponsor_funder_lookup_count: 0,
      },
    };
  }

  const [occupations, contributionSizes, outsideGroups] = await Promise.all([
    client.getDirectOccupationAggregates(
      {
        filerId: resolution.filerId,
        committeeId: resolution.committeeId,
        electionYear: input.args.electionYear,
        limit: input.args.limit,
      },
      clientOptions
    ),
    client.getContributionSizeAggregates(
      {
        filerId: resolution.filerId,
        committeeId: resolution.committeeId,
        electionYear: input.args.electionYear,
        limit: input.args.limit,
      },
      clientOptions
    ),
    client.getIndependentExpenditureGroups(
      {
        candidateName: input.args.candidateName,
        electionYear: input.args.electionYear,
        office: officeSearch?.pdcOffice ?? null,
        legislativeDistrict: officeSearch?.legislativeDistrict ?? null,
        limit: input.args.limit,
      },
      clientOptions
    ),
  ]);

  const outsideIndustryResult = await buildOutsideIndustries({
    client,
    groups: outsideGroups,
    electionYear: input.args.electionYear,
    funderLimit: input.args.funderLimit,
    minIndustryAmount: input.args.minIndustryAmount,
    clientOptions,
  });
  const supportingIndustries = outsideIndustryResult.industries
    .filter((industry) => industry.support_oppose === "support")
    .slice(0, input.args.limit);
  const opposingIndustries = outsideIndustryResult.industries
    .filter((industry) => industry.support_oppose === "oppose")
    .slice(0, input.args.limit);

  return {
    type: "washington_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: publicProbeArgs(input.args),
    ok: true,
    resolution,
    direct_campaign: {
      top_occupations: occupations.map(mapAggregate).slice(0, input.args.limit),
      contribution_size_buckets: contributionSizes.map(mapAggregate).slice(0, input.args.limit),
    },
    outside_spending: {
      top_supporting_groups: outsideGroups
        .filter((group) => group.supportOppose === "support")
        .map(mapOutsideGroup)
        .slice(0, input.args.limit),
      top_opposing_groups: outsideGroups
        .filter((group) => group.supportOppose === "oppose")
        .map(mapOutsideGroup)
        .slice(0, input.args.limit),
      top_supporting_industries: supportingIndustries,
      top_opposing_industries: opposingIndustries,
      skipped_sponsor_funder_lookup_count: outsideIndustryResult.skippedSponsorFunderLookupCount,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseProbeWashingtonCandidateFinanceArgs(process.argv.slice(2));
  const output = await runProbeWashingtonCandidateFinance({ args });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Washington candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
