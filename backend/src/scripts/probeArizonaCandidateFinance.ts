import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  buildArizonaCandidateFinanceSnapshot,
  type ArizonaCandidateFinanceSnapshot,
  type ArizonaCandidateFinanceSnapshotClient,
} from "../pipeline/arizonaFinance/arizonaCandidateFinanceSnapshot.js";
import {
  searchArizonaSpotlightIncomeTransactions,
  searchArizonaSpotlightIndependentExpenditures,
  type ArizonaSpotlightClientOptions,
  type ArizonaSpotlightIncomeTransaction,
} from "../pipeline/arizonaFinance/arizonaSpotlightClient.js";
import {
  readStrictFlagValue,
  readStrictNonNegativeNumberFlag,
  readStrictPositiveIntegerFlag,
  readStrictRequiredFlagValue,
  readStrictRequiredPositiveIntegerFlag,
} from "../utils/cliFlags.js";

type ArizonaFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeName: string;
  committeeId: string | null;
  candidateFilerId: string | null;
  limit: number;
  resolutionLimit: number;
  incomeLimit: number;
  independentExpenditureLimit: number;
  outsideIncomeLimit: number;
  outsideMaxGroups: number;
  minIndustryAmount: number;
  timeoutMs: number;
};

type ArizonaCandidateCommitteeResolution =
  | {
      status: "provided";
      committeeId: string;
      committeeName: string | null;
      candidateFilerId: string;
      confidence: "provided";
      source: "cli";
      sourceUrl: null;
    }
  | {
      status: "matched";
      committeeId: string;
      committeeName: string;
      candidateFilerId: string;
      confidence: "single_committee";
      source: "spotlight_income_search";
      sourceUrl: string | null;
      matchedIncomeRowCount: number;
    }
  | {
      status: "unmatched";
      reason: "no_income_rows";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_committee_matches";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: ArizonaCandidateCommitteeMatch[];
    };

type ArizonaCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  amount: number;
  rowCount: number;
  sourceUrl: string | null;
};

type ArizonaFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number | null;
  source_url: string | null;
};

type ArizonaFinanceProbeOutsideGroup = {
  committee_id: string;
  committee_name: string;
  support_oppose: "support" | "oppose";
  amount: number;
  expenditure_count: number;
  source_url: string | null;
};

type ArizonaFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number | null;
  committee_id: string;
  committee_name: string;
  source_url: string | null;
};

type ArizonaFinanceProbeIndustry = ArizonaFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: "support" | "oppose";
  evidence: ArizonaFinanceProbeIndustryEvidence[];
};

type ArizonaFinanceProbeOutput = {
  type: "arizona_candidate_finance_live_probe";
  ts: string;
  args: ArizonaFinanceProbeArgs;
  ok: boolean;
  resolution: ArizonaCandidateCommitteeResolution;
  direct_campaign: {
    top_occupations: ArizonaFinanceProbeBreakdown[];
    contribution_size_buckets: ArizonaFinanceProbeBreakdown[];
  };
  outside_spending: {
    top_supporting_groups: ArizonaFinanceProbeOutsideGroup[];
    top_opposing_groups: ArizonaFinanceProbeOutsideGroup[];
    top_supporting_industries: ArizonaFinanceProbeIndustry[];
    top_opposing_industries: ArizonaFinanceProbeIndustry[];
  };
  fetched: ArizonaCandidateFinanceSnapshot["fetched"] | null;
};

type ArizonaFinanceProbeClient = ArizonaCandidateFinanceSnapshotClient;

const DEFAULT_LIMIT = 5;
const DEFAULT_RESOLUTION_LIMIT = 100;
const DEFAULT_TRANSACTION_LIMIT = 500;
const DEFAULT_OUTSIDE_MAX_GROUPS = 10;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;

const DEFAULT_CLIENT: ArizonaFinanceProbeClient = {
  searchIncomeTransactions: searchArizonaSpotlightIncomeTransactions,
  searchIndependentExpenditures: searchArizonaSpotlightIndependentExpenditures,
};

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function parseProbeArizonaCandidateFinanceArgs(args: readonly string[]): ArizonaFinanceProbeArgs {
  return {
    candidateName: readStrictRequiredFlagValue(args, "--candidate-name"),
    electionYear: readStrictRequiredPositiveIntegerFlag(args, "--year"),
    officeName: readStrictRequiredFlagValue(args, "--office"),
    committeeId: readStrictFlagValue(args, "--committee-id"),
    candidateFilerId: readStrictFlagValue(args, "--candidate-filer-id"),
    limit: readStrictPositiveIntegerFlag(args, "--limit") ?? DEFAULT_LIMIT,
    resolutionLimit: readStrictPositiveIntegerFlag(args, "--resolution-limit") ?? DEFAULT_RESOLUTION_LIMIT,
    incomeLimit: readStrictPositiveIntegerFlag(args, "--income-limit") ?? DEFAULT_TRANSACTION_LIMIT,
    independentExpenditureLimit: readStrictPositiveIntegerFlag(args, "--ie-limit") ?? DEFAULT_TRANSACTION_LIMIT,
    outsideIncomeLimit: readStrictPositiveIntegerFlag(args, "--outside-income-limit") ?? DEFAULT_TRANSACTION_LIMIT,
    outsideMaxGroups: readStrictPositiveIntegerFlag(args, "--outside-max-groups") ?? DEFAULT_OUTSIDE_MAX_GROUPS,
    minIndustryAmount: readStrictNonNegativeNumberFlag(args, "--min-industry-amount") ?? DEFAULT_MIN_INDUSTRY_AMOUNT,
    timeoutMs: readStrictPositiveIntegerFlag(args, "--timeout-ms") ?? DEFAULT_TIMEOUT_MS,
  };
}

function emptyOutput(input: {
  args: ArizonaFinanceProbeArgs;
  resolution: ArizonaCandidateCommitteeResolution;
  now?: Date;
}): ArizonaFinanceProbeOutput {
  return {
    type: "arizona_candidate_finance_live_probe",
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
    },
    fetched: null,
  };
}

function addCommitteeMatch(matches: Map<string, ArizonaCandidateCommitteeMatch>, row: ArizonaSpotlightIncomeTransaction): void {
  const committeeId = row.committeeId.trim();
  const committeeName = row.committeeName.trim();
  if (!committeeId || !committeeName) {
    return;
  }
  const existing = matches.get(committeeId);
  if (!existing) {
    matches.set(committeeId, {
      committeeId,
      committeeName,
      amount: row.amount,
      rowCount: 1,
      sourceUrl: row.sourceUrl ?? null,
    });
    return;
  }
  existing.amount = Math.round((existing.amount + row.amount) * 100) / 100;
  existing.rowCount += 1;
  existing.sourceUrl ??= row.sourceUrl ?? null;
}

async function resolveArizonaCandidateCommittee(input: {
  args: ArizonaFinanceProbeArgs;
  client: ArizonaFinanceProbeClient;
  clientOptions: ArizonaSpotlightClientOptions;
}): Promise<ArizonaCandidateCommitteeResolution> {
  if (input.args.committeeId) {
    return {
      status: "provided",
      committeeId: input.args.committeeId,
      committeeName: null,
      candidateFilerId: input.args.candidateFilerId ?? input.args.committeeId,
      confidence: "provided",
      source: "cli",
      sourceUrl: null,
    };
  }

  const rows = await input.client.searchIncomeTransactions(
    {
      electionYear: input.args.electionYear,
      filerName: input.args.candidateName,
      limit: input.args.resolutionLimit,
    },
    input.clientOptions
  );
  const matches = new Map<string, ArizonaCandidateCommitteeMatch>();
  for (const row of rows) {
    addCommitteeMatch(matches, row);
  }
  const sortedMatches = [...matches.values()].sort(
    (left, right) => right.amount - left.amount || left.committeeName.localeCompare(right.committeeName)
  );

  if (sortedMatches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_income_rows",
      candidateNameNormalized: normalizeTextKey(input.args.candidateName),
      officeNameNormalized: normalizeTextKey(input.args.officeName),
    };
  }

  if (sortedMatches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_committee_matches",
      candidateNameNormalized: normalizeTextKey(input.args.candidateName),
      officeNameNormalized: normalizeTextKey(input.args.officeName),
      matches: sortedMatches.slice(0, 10),
    };
  }

  const match = sortedMatches[0]!;
  return {
    status: "matched",
    committeeId: match.committeeId,
    committeeName: match.committeeName,
    candidateFilerId: input.args.candidateFilerId ?? match.committeeId,
    confidence: "single_committee",
    source: "spotlight_income_search",
    sourceUrl: match.sourceUrl,
    matchedIncomeRowCount: match.rowCount,
  };
}

function mapBreakdown(row: {
  categoryName: string;
  amount: number;
  contributorCount: number | null;
  sourceUrl: string | null;
}): ArizonaFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.contributorCount,
    source_url: row.sourceUrl,
  };
}

function mapOutsideGroup(row: {
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  expenditureCount: number;
  sourceUrl: string | null;
}): ArizonaFinanceProbeOutsideGroup {
  return {
    committee_id: row.committeeId,
    committee_name: row.committeeName,
    support_oppose: row.supportOppose,
    amount: row.amount,
    expenditure_count: row.expenditureCount,
    source_url: row.sourceUrl,
  };
}

function buildOutsideIndustryProbeRows(input: {
  snapshot: ArizonaCandidateFinanceSnapshot;
  supportOppose: "support" | "oppose";
  limit: number;
}): ArizonaFinanceProbeIndustry[] {
  const groups = new Map(
    (input.snapshot.outsideSpending.summary?.groups ?? []).map((group) => [
      `${group.committeeId}\u0000${group.supportOppose}`,
      group,
    ])
  );
  const donorBreakdowns = input.snapshot.outsideGroupContributions.outsideGroupBreakdowns.filter(
    (row) => row.categoryType === "donor" && row.supportOppose === input.supportOppose
  );

  return input.snapshot.outsideGroupContributions.outsideGroupBreakdowns
    .filter((row) => row.categoryType === "industry" && row.supportOppose === input.supportOppose)
    .map((industry): ArizonaFinanceProbeIndustry => {
      const evidence = donorBreakdowns
        .filter((donor) => donor.committeeId === industry.committeeId)
        .filter(
          (donor) =>
            classifyFinanceLabel({ rawLabel: donor.categoryName, labelType: "donor" }).industrySlug ===
            industry.categoryName
        )
        .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
        .slice(0, 5)
        .map((donor): ArizonaFinanceProbeIndustryEvidence => {
          const group = groups.get(`${donor.committeeId}\u0000${donor.supportOppose}`);
          return {
            organization_name: donor.categoryName,
            amount: donor.amount,
            contributor_count: donor.contributorCount,
            committee_id: donor.committeeId,
            committee_name: group?.committeeName ?? donor.committeeId,
            source_url: donor.sourceUrl ?? group?.sourceUrl ?? null,
          };
        });
      return {
        category_name: industry.categoryName,
        industry_slug: industry.categoryName as FinanceIndustrySlug,
        support_oppose: industry.supportOppose,
        amount: industry.amount,
        contributor_count: industry.contributorCount,
        source_url: industry.sourceUrl,
        evidence,
      };
    })
    .sort((left, right) => right.amount - left.amount || left.category_name.localeCompare(right.category_name))
    .slice(0, input.limit);
}

export async function runProbeArizonaCandidateFinance(input: {
  args: ArizonaFinanceProbeArgs;
  client?: Partial<ArizonaFinanceProbeClient>;
  now?: Date;
}): Promise<ArizonaFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const clientOptions: ArizonaSpotlightClientOptions = { timeoutMs: input.args.timeoutMs };
  const resolution = await resolveArizonaCandidateCommittee({
    args: input.args,
    client,
    clientOptions,
  });

  if (resolution.status !== "matched" && resolution.status !== "provided") {
    return emptyOutput({ args: input.args, resolution, now: input.now });
  }

  const snapshot = await buildArizonaCandidateFinanceSnapshot({
    candidateName: input.args.candidateName,
    candidateCommitteeId: resolution.committeeId,
    candidateFilerId: resolution.candidateFilerId,
    electionYear: input.args.electionYear,
    directIncomeLimit: input.args.incomeLimit,
    independentExpenditureLimitPerPosition: input.args.independentExpenditureLimit,
    outsideGroupIncomeLimitPerGroup: input.args.outsideIncomeLimit,
    outsideMaxGroups: input.args.outsideMaxGroups,
    directMaxBreakdownsPerCategory: input.args.limit,
    outsideMaxBreakdownsPerCategory: input.args.limit,
    minIndustryAmount: input.args.minIndustryAmount,
    spotlightClientOptions: clientOptions,
    spotlightClient: client,
  });

  const directBreakdowns = snapshot.directFinance.directBreakdowns;
  const outsideGroups = snapshot.outsideSpending.summary?.groups ?? [];

  return {
    type: "arizona_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: true,
    resolution,
    direct_campaign: {
      top_occupations: directBreakdowns
        .filter((row) => row.categoryType === "occupation")
        .map(mapBreakdown)
        .slice(0, input.args.limit),
      contribution_size_buckets: directBreakdowns
        .filter((row) => row.categoryType === "contribution_size")
        .map(mapBreakdown)
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
      top_supporting_industries: buildOutsideIndustryProbeRows({
        snapshot,
        supportOppose: "support",
        limit: input.args.limit,
      }),
      top_opposing_industries: buildOutsideIndustryProbeRows({
        snapshot,
        supportOppose: "oppose",
        limit: input.args.limit,
      }),
    },
    fetched: snapshot.fetched,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const args = parseProbeArizonaCandidateFinanceArgs(process.argv.slice(2));
  const output = await runProbeArizonaCandidateFinance({ args });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Arizona candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
