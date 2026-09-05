import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  searchAndResolveHawaiiCandidateCommittee,
  type HawaiiCandidateCommitteeResolution,
} from "../pipeline/hawaiiFinance/hawaiiCandidateCommitteeResolver.js";
import {
  getHawaiiCscContributionSizeAggregates,
  getHawaiiCscDirectOccupationAggregates,
  getHawaiiCscIndependentExpenditureGroups,
  getHawaiiCscNoncandidateCommitteeFunders,
  HAWAII_CSC_CANDIDATE_CONTRIBUTIONS_DATASET,
  HAWAII_CSC_DATA_BASE_URL,
  HAWAII_CSC_NONCANDIDATE_CONTRIBUTIONS_DATASET,
  HAWAII_CSC_NONCANDIDATE_EXPENDITURES_DATASET,
  type HawaiiCscAggregate,
  type HawaiiCscClientOptions,
  type HawaiiCscIndependentSpendingGroup,
} from "../pipeline/hawaiiFinance/hawaiiCscClient.js";
import {
  readStrictFlagValue,
  readStrictNonNegativeNumberFlag,
  readStrictPositiveIntegerFlag,
  readStrictRequiredFlagValue,
  readStrictRequiredPositiveIntegerFlag,
} from "../utils/cliFlags.js";

type HawaiiFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  district: string | null;
  limit: number;
  funderLimit: number;
  minIndustryAmount: number;
  timeoutMs: number;
  appToken: string | null;
};

type HawaiiFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type HawaiiFinanceProbeOutsideGroup = {
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  expenditure_count: number;
  source_url: string | null;
};

type HawaiiFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number;
  committee_id: string;
  committee_name: string;
  source_url: string | null;
};

type HawaiiFinanceProbeIndustry = HawaiiFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: "support" | "oppose";
  evidence: HawaiiFinanceProbeIndustryEvidence[];
};

type HawaiiFinanceProbeOutput = {
  type: "hawaii_candidate_finance_live_probe";
  ts: string;
  args: Omit<HawaiiFinanceProbeArgs, "appToken"> & { appTokenProvided: boolean };
  ok: boolean;
  resolution: HawaiiCandidateCommitteeResolution;
  direct_campaign: {
    top_occupations: HawaiiFinanceProbeBreakdown[];
    contribution_size_buckets: HawaiiFinanceProbeBreakdown[];
  };
  outside_spending: {
    top_supporting_groups: HawaiiFinanceProbeOutsideGroup[];
    top_opposing_groups: HawaiiFinanceProbeOutsideGroup[];
    top_supporting_industries: HawaiiFinanceProbeIndustry[];
    top_opposing_industries: HawaiiFinanceProbeIndustry[];
    skipped_outside_funder_lookup_count: number;
  };
};

type HawaiiFinanceProbeClient = {
  resolveCandidateCommittee: typeof searchAndResolveHawaiiCandidateCommittee;
  getDirectOccupationAggregates: typeof getHawaiiCscDirectOccupationAggregates;
  getContributionSizeAggregates: typeof getHawaiiCscContributionSizeAggregates;
  getIndependentExpenditureGroups: typeof getHawaiiCscIndependentExpenditureGroups;
  getNoncandidateCommitteeFunders: typeof getHawaiiCscNoncandidateCommitteeFunders;
};

const DEFAULT_LIMIT = 5;
const DEFAULT_FUNDER_LIMIT = 20;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const CANDIDATE_CONTRIBUTION_SOURCE_URL = `${HAWAII_CSC_DATA_BASE_URL}/${HAWAII_CSC_CANDIDATE_CONTRIBUTIONS_DATASET}.json`;
const NONCANDIDATE_CONTRIBUTION_SOURCE_URL = `${HAWAII_CSC_DATA_BASE_URL}/${HAWAII_CSC_NONCANDIDATE_CONTRIBUTIONS_DATASET}.json`;
const NONCANDIDATE_EXPENDITURE_SOURCE_URL = `${HAWAII_CSC_DATA_BASE_URL}/${HAWAII_CSC_NONCANDIDATE_EXPENDITURES_DATASET}.json`;

const DEFAULT_CLIENT: HawaiiFinanceProbeClient = {
  resolveCandidateCommittee: searchAndResolveHawaiiCandidateCommittee,
  getDirectOccupationAggregates: getHawaiiCscDirectOccupationAggregates,
  getContributionSizeAggregates: getHawaiiCscContributionSizeAggregates,
  getIndependentExpenditureGroups: getHawaiiCscIndependentExpenditureGroups,
  getNoncandidateCommitteeFunders: getHawaiiCscNoncandidateCommitteeFunders,
};

function parseOfficeScope(value: string | null): HawaiiFinanceProbeArgs["officeScope"] {
  const normalized = value?.trim() || "statewide";
  if (normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower") {
    return normalized;
  }
  throw new Error(`Invalid --scope value: ${value}`);
}

export function parseProbeHawaiiCandidateFinanceArgs(args: readonly string[]): HawaiiFinanceProbeArgs {
  return {
    candidateName: readStrictRequiredFlagValue(args, "--candidate-name"),
    electionYear: readStrictRequiredPositiveIntegerFlag(args, "--year"),
    officeScope: parseOfficeScope(readStrictFlagValue(args, "--scope")),
    officeName: readStrictRequiredFlagValue(args, "--office"),
    district: readStrictFlagValue(args, "--district"),
    limit: readStrictPositiveIntegerFlag(args, "--limit") ?? DEFAULT_LIMIT,
    funderLimit: readStrictPositiveIntegerFlag(args, "--funder-limit") ?? DEFAULT_FUNDER_LIMIT,
    minIndustryAmount: readStrictNonNegativeNumberFlag(args, "--min-industry-amount") ?? DEFAULT_MIN_INDUSTRY_AMOUNT,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms") ?? DEFAULT_TIMEOUT_MS,
    appToken: readStrictFlagValue(args, "--app-token") ?? process.env.HAWAII_CSC_APP_TOKEN?.trim() ?? null,
  };
}

function publicProbeArgs(args: HawaiiFinanceProbeArgs): HawaiiFinanceProbeOutput["args"] {
  return {
    candidateName: args.candidateName,
    electionYear: args.electionYear,
    officeScope: args.officeScope,
    officeName: args.officeName,
    district: args.district,
    limit: args.limit,
    funderLimit: args.funderLimit,
    minIndustryAmount: args.minIndustryAmount,
    timeoutMs: args.timeoutMs,
    appTokenProvided: Boolean(args.appToken),
  };
}

function mapAggregate(row: HawaiiCscAggregate, sourceUrl: string): HawaiiFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.count,
    source_url: sourceUrl,
  };
}

function mapOutsideGroup(group: HawaiiCscIndependentSpendingGroup): HawaiiFinanceProbeOutsideGroup {
  return {
    committee_id: group.committeeId,
    committee_name: group.committeeName,
    support_oppose: group.supportOppose,
    amount: group.amount,
    expenditure_count: group.expenditureCount,
    source_url: NONCANDIDATE_EXPENDITURE_SOURCE_URL,
  };
}

function addIndustryEvidence(
  industries: Map<string, HawaiiFinanceProbeIndustry>,
  input: {
    industrySlug: FinanceIndustrySlug;
    group: HawaiiCscIndependentSpendingGroup;
    funder: HawaiiCscAggregate;
  }
): void {
  const key = `${input.group.supportOppose}\u0000${input.industrySlug}`;
  const evidence: HawaiiFinanceProbeIndustryEvidence = {
    organization_name: input.funder.categoryName,
    amount: input.funder.amount,
    contributor_count: input.funder.count,
    committee_id: input.group.committeeId,
    committee_name: input.group.committeeName,
    source_url: NONCANDIDATE_CONTRIBUTION_SOURCE_URL,
  };
  const existing = industries.get(key);
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
  existing.evidence.push(evidence);
}

async function buildOutsideIndustries(input: {
  client: HawaiiFinanceProbeClient;
  groups: readonly HawaiiCscIndependentSpendingGroup[];
  funderLimit: number;
  minIndustryAmount: number;
  clientOptions: HawaiiCscClientOptions;
}): Promise<{
  industries: HawaiiFinanceProbeIndustry[];
  skippedOutsideFunderLookupCount: number;
}> {
  const industries = new Map<string, HawaiiFinanceProbeIndustry>();
  let skippedOutsideFunderLookupCount = 0;

  for (const group of input.groups) {
    let funders: HawaiiCscAggregate[];
    try {
      funders = await input.client.getNoncandidateCommitteeFunders(
        {
          committeeId: group.committeeId,
          electionPeriod: group.electionPeriod,
          limit: input.funderLimit,
        },
        input.clientOptions
      );
    } catch {
      skippedOutsideFunderLookupCount += 1;
      continue;
    }
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
    skippedOutsideFunderLookupCount,
  };
}

export async function runProbeHawaiiCandidateFinance(input: {
  args: HawaiiFinanceProbeArgs;
  client?: Partial<HawaiiFinanceProbeClient>;
  now?: Date;
}): Promise<HawaiiFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const clientOptions: HawaiiCscClientOptions = {
    timeoutMs: input.args.timeoutMs,
    ...(input.args.appToken ? { appToken: input.args.appToken } : {}),
  };
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
    return {
      type: "hawaii_candidate_finance_live_probe",
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
        skipped_outside_funder_lookup_count: 0,
      },
    };
  }

  const [occupations, contributionSizes, outsideGroups] = await Promise.all([
    client.getDirectOccupationAggregates(
      {
        committeeId: resolution.committeeId,
        electionPeriod: resolution.electionPeriod,
        limit: input.args.limit,
      },
      clientOptions
    ),
    client.getContributionSizeAggregates(
      {
        committeeId: resolution.committeeId,
        electionPeriod: resolution.electionPeriod,
        limit: input.args.limit,
      },
      clientOptions
    ),
    client.getIndependentExpenditureGroups(
      {
        candidateName: input.args.candidateName,
        electionYear: input.args.electionYear,
        limit: input.args.limit,
      },
      clientOptions
    ),
  ]);

  const outsideIndustryResult = await buildOutsideIndustries({
    client,
    groups: outsideGroups,
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
    type: "hawaii_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: publicProbeArgs(input.args),
    ok: true,
    resolution,
    direct_campaign: {
      top_occupations: occupations.map((row) => mapAggregate(row, CANDIDATE_CONTRIBUTION_SOURCE_URL)).slice(0, input.args.limit),
      contribution_size_buckets: contributionSizes
        .map((row) => mapAggregate(row, CANDIDATE_CONTRIBUTION_SOURCE_URL))
        .slice(0, input.args.limit),
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
      skipped_outside_funder_lookup_count: outsideIndustryResult.skippedOutsideFunderLookupCount,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseProbeHawaiiCandidateFinanceArgs(process.argv.slice(2));
  const output = await runProbeHawaiiCandidateFinance({ args });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Hawaii candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
