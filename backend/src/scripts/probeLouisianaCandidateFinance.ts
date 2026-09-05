import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { classifyFinanceLabel, FINANCE_INDUSTRY_SLUGS, type FinanceIndustrySlug } from "../pipeline/finance/financeLabelClassifier.js";
import {
  DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR,
  getLouisianaCampaignFinanceArtifactCachePaths,
  readLouisianaCampaignFinanceArtifactCacheMetadata,
  refreshLouisianaCampaignFinanceArtifactCache,
  type LouisianaCampaignFinanceArtifactRefreshResult,
} from "../pipeline/louisianaFinance/louisianaCampaignFinanceArtifactCache.js";
import {
  readLouisianaCampaignFinanceContributionRows,
  readLouisianaCampaignFinanceExpenditureRows,
  type LouisianaCampaignFinanceCsvRow,
} from "../pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";
import {
  resolveLouisianaCandidateCommittee,
  type LouisianaCandidateCommitteeResolution,
} from "../pipeline/louisianaFinance/louisianaCandidateCommitteeResolver.js";
import { aggregateLouisianaDirectContributions } from "../pipeline/louisianaFinance/louisianaDirectContributionAggregator.js";
import {
  aggregateLouisianaOutsideGroupContributions,
  type LouisianaFinanceOutsideGroupBreakdown,
} from "../pipeline/louisianaFinance/louisianaOutsideGroupContributionAggregator.js";
import {
  aggregateLouisianaOutsideSupport,
  type LouisianaOutsideSupportGroup,
  type LouisianaSupportOppose,
} from "../pipeline/louisianaFinance/louisianaOutsideSupportAggregator.js";
import {
  readStrictFlagValue,
  readStrictPositiveIntegerFlag,
  readStrictRequiredFlagValue,
  readStrictRequiredPositiveIntegerFlag,
} from "../utils/cliFlags.js";

type LouisianaFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  district: string | null;
  limit: number;
  cacheDir: string;
  contributionCsvPath: string | null;
  expenditureCsvPath: string | null;
  refreshCache: boolean;
  forceRefresh: boolean;
  startYear?: number;
  endYear?: number;
  expectedDirectTotal: number | null;
  expectedOutsideSupportTotal: number | null;
  pacFilerNumber: string | null;
  expectedPacReceiptsTotal: number | null;
  ambiguousCandidateName: string | null;
  expectedAmbiguousStatus: "ambiguous" | "unmatched" | "not_matched";
  expectedTolerance: number;
};

type LouisianaFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type LouisianaFinanceProbeOutsideGroup = {
  filer_number: string;
  filer_name: string;
  support_oppose: LouisianaSupportOppose;
  support_mechanism: "la_pac_contribution_to_candidate";
  amount: number;
  expenditure_count: number;
  source_url: string | null;
};

type LouisianaFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number;
  filer_number: string;
  filer_name: string;
  source_url: string | null;
};

type LouisianaFinanceProbeIndustry = LouisianaFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: LouisianaSupportOppose;
  evidence: LouisianaFinanceProbeIndustryEvidence[];
};

type LouisianaFinanceProbeExpectedComparison = {
  label: "direct_contributions" | "outside_support" | "pac_receipts";
  expected_total: number;
  observed_total: number;
  delta: number;
  tolerance: number;
  ok: boolean;
};

type LouisianaFinanceProbeAmbiguousCheck = {
  candidate_name: string;
  expected_status: LouisianaFinanceProbeArgs["expectedAmbiguousStatus"];
  actual_status: LouisianaCandidateCommitteeResolution["status"];
  ok: boolean;
};

type LouisianaFinanceProbeOutput = {
  type: "louisiana_candidate_finance_live_probe";
  ts: string;
  args: LouisianaFinanceProbeArgs;
  ok: boolean;
  cache_refresh: LouisianaCampaignFinanceArtifactRefreshResult | null;
  source_urls: {
    contributions: string | null;
    expenditures: string | null;
  };
  resolution: LouisianaCandidateCommitteeResolution;
  validation: {
    expected_total_comparisons: LouisianaFinanceProbeExpectedComparison[];
    expected_total_comparison_ok: boolean | null;
    no_occupation_data: boolean;
    ambiguous_candidate_check: LouisianaFinanceProbeAmbiguousCheck | null;
  };
  rows_loaded: {
    contributions: number;
    expenditures: number;
  };
  direct_campaign: {
    total_receipts: number;
    direct_contribution_total: number;
    top_occupations: [];
    contributor_types: LouisianaFinanceProbeBreakdown[];
    contribution_size_buckets: LouisianaFinanceProbeBreakdown[];
  };
  outside_spending: {
    support_total: number;
    oppose_total: number | null;
    top_supporting_groups: LouisianaFinanceProbeOutsideGroup[];
    top_opposing_groups: LouisianaFinanceProbeOutsideGroup[];
    top_supporting_industries: LouisianaFinanceProbeIndustry[];
    top_opposing_industries: LouisianaFinanceProbeIndustry[];
  };
  known_pac: {
    filer_number: string;
    filer_name: string | null;
    receipts_total: number;
    top_donors: LouisianaFinanceProbeBreakdown[];
    top_industries: LouisianaFinanceProbeIndustry[];
  } | null;
  counters: {
    direct_matched_rows: number;
    direct_included_rows: number;
    direct_skipped_rows: number;
    outside_matched_rows: number;
    outside_included_rows: number;
    outside_skipped_rows: number;
    outside_group_matched_rows: number;
    outside_group_included_rows: number;
    outside_group_skipped_rows: number;
  };
};

type LouisianaProbeRows = {
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  expenditureRows: readonly LouisianaCampaignFinanceCsvRow[];
  contributionSourceUrl: string | null;
  expenditureSourceUrl: string | null;
  cacheRefresh: LouisianaCampaignFinanceArtifactRefreshResult | null;
};

const DEFAULT_LIMIT = 5;
const DEFAULT_EXPECTED_TOLERANCE = 0.01;
const INDUSTRY_SLUGS = new Set<string>(FINANCE_INDUSTRY_SLUGS);

function parseOptionalAmountFlag(args: readonly string[], name: string): number | null {
  const raw = readStrictFlagValue(args, name);
  if (raw === null) {
    return null;
  }
  const normalized = raw.replace(/[$,]/g, "");
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(normalized)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(normalized);
}

function parseNonNegativeNumberFlag(args: readonly string[], name: string, fallback: number): number {
  const raw = readStrictFlagValue(args, name);
  if (raw === null) {
    return fallback;
  }
  const normalized = raw.replace(/[$,]/g, "");
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(normalized)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(normalized);
}

function parseOfficeScope(value: string | null): LouisianaFinanceProbeArgs["officeScope"] {
  const normalized = value?.trim() || "statewide";
  if (normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower") {
    return normalized;
  }
  throw new Error(`Invalid --scope value: ${value}`);
}

function parseExpectedAmbiguousStatus(value: string | null): LouisianaFinanceProbeArgs["expectedAmbiguousStatus"] {
  const normalized = value?.trim() || "not_matched";
  if (normalized === "ambiguous" || normalized === "unmatched" || normalized === "not_matched") {
    return normalized;
  }
  throw new Error(`Invalid --expected-ambiguous-status value: ${value}`);
}

export function parseProbeLouisianaCandidateFinanceArgs(args: readonly string[]): LouisianaFinanceProbeArgs {
  return {
    candidateName: readStrictRequiredFlagValue(args, "--candidate-name"),
    electionYear: readStrictRequiredPositiveIntegerFlag(args, "--year"),
    officeScope: parseOfficeScope(readStrictFlagValue(args, "--scope")),
    officeName: readStrictRequiredFlagValue(args, "--office"),
    district: readStrictFlagValue(args, "--district"),
    limit: readStrictPositiveIntegerFlag(args, "--limit") ?? DEFAULT_LIMIT,
    cacheDir: readStrictFlagValue(args, "--cache-dir") ?? DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR,
    contributionCsvPath: readStrictFlagValue(args, "--contributions-csv"),
    expenditureCsvPath: readStrictFlagValue(args, "--expenditures-csv"),
    refreshCache: args.includes("--refresh-cache"),
    forceRefresh: args.includes("--force-refresh"),
    startYear: readStrictPositiveIntegerFlag(args, "--start-year"),
    endYear: readStrictPositiveIntegerFlag(args, "--end-year"),
    expectedDirectTotal: parseOptionalAmountFlag(args, "--expected-direct-total"),
    expectedOutsideSupportTotal: parseOptionalAmountFlag(args, "--expected-outside-support-total"),
    pacFilerNumber: readStrictFlagValue(args, "--pac-filer-number"),
    expectedPacReceiptsTotal: parseOptionalAmountFlag(args, "--expected-pac-receipts-total"),
    ambiguousCandidateName: readStrictFlagValue(args, "--ambiguous-candidate-name"),
    expectedAmbiguousStatus: parseExpectedAmbiguousStatus(readStrictFlagValue(args, "--expected-ambiguous-status")),
    expectedTolerance: parseNonNegativeNumberFlag(args, "--expected-tolerance", DEFAULT_EXPECTED_TOLERANCE),
  };
}

function firstNonEmpty(row: LouisianaCampaignFinanceCsvRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
}

function normalizeFilerNumber(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, "");
}

function parseAmountCents(raw: string): number | null {
  const trimmed = raw.trim();
  const isParentheticalNegative = /^\(.+\)$/.test(trimmed);
  const normalized = trimmed.replace(/[,$()]/g, "");
  if (!normalized || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }
  const amount = Number(normalized) * (isParentheticalNegative ? -1 : 1);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseYearFromDate(raw: string): number | null {
  const trimmed = raw.trim();
  const isoMatch = /^(\d{4})-\d{1,2}-\d{1,2}\b/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number.parseInt(isoMatch[1], 10);
  }
  const slashMatch = /^\d{1,2}\/\d{1,2}\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[1]) {
    return Number.parseInt(slashMatch[1], 10);
  }
  return null;
}

function isElectionCycleDate(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseYearFromDate(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function displayFilerName(row: LouisianaCampaignFinanceCsvRow): string {
  const explicit = firstNonEmpty(row, ["FilerName", "Filer Name"]);
  if (explicit) {
    return explicit;
  }
  const firstName = firstNonEmpty(row, ["FilerFirstName", "Filer First Name"]);
  const lastName = firstNonEmpty(row, ["FilerLastName", "Filer Last Name"]);
  if (lastName && firstName) {
    return `${lastName}, ${firstName}`;
  }
  return lastName || firstName;
}

function mapBreakdown(row: {
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
}): LouisianaFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.contributorCount,
    source_url: row.sourceUrl ?? null,
  };
}

function mapOutsideGroup(group: LouisianaOutsideSupportGroup): LouisianaFinanceProbeOutsideGroup {
  return {
    filer_number: group.filerNumber,
    filer_name: group.filerName,
    support_oppose: group.supportOppose,
    support_mechanism: group.supportMechanism,
    amount: group.amount,
    expenditure_count: group.expenditureCount,
    source_url: group.sourceUrl ?? null,
  };
}

function toIndustrySlug(value: string): FinanceIndustrySlug | null {
  return INDUSTRY_SLUGS.has(value) ? (value as FinanceIndustrySlug) : null;
}

function buildOutsideIndustries(input: {
  groups: readonly LouisianaOutsideSupportGroup[];
  breakdowns: readonly LouisianaFinanceOutsideGroupBreakdown[];
  limit: number;
}): LouisianaFinanceProbeIndustry[] {
  const groupNames = new Map(input.groups.map((group) => [`${group.filerNumber}\u0000${group.supportOppose}`, group.filerName]));
  const donorBreakdowns = input.breakdowns.filter((breakdown) => breakdown.categoryType === "donor");
  const industries = new Map<string, LouisianaFinanceProbeIndustry>();

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
      .filter((donor) => donor.filerNumber === breakdown.filerNumber && donor.supportOppose === breakdown.supportOppose)
      .filter((donor) => classifyFinanceLabel({ rawLabel: donor.categoryName, labelType: "donor" }).industrySlug === industrySlug)
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, 5)
      .map((donor) => ({
        organization_name: donor.categoryName,
        amount: donor.amount,
        contributor_count: donor.contributorCount,
        filer_number: donor.filerNumber,
        filer_name: groupNames.get(`${donor.filerNumber}\u0000${donor.supportOppose}`) ?? donor.filerNumber,
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

  const sortedIndustries = [...industries.values()]
    .map((industry) => ({
      ...industry,
      evidence: industry.evidence
        .sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name))
        .slice(0, 5),
    }))
    .sort((left, right) => right.amount - left.amount || left.category_name.localeCompare(right.category_name));

  return (["support", "oppose"] as const).flatMap((supportOppose) =>
    sortedIndustries.filter((industry) => industry.support_oppose === supportOppose).slice(0, input.limit)
  );
}

function sumPositiveCycleContributionsForFiler(input: {
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  filerNumber: string;
  electionYear: number;
}): number {
  const filerNumber = normalizeFilerNumber(input.filerNumber);
  let totalCents = 0;
  for (const row of input.contributionRows) {
    if (normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"])) !== filerNumber) {
      continue;
    }
    if (!isElectionCycleDate({ rawDate: firstNonEmpty(row, ["ContributionDate", "Contribution Date"]), electionYear: input.electionYear })) {
      continue;
    }
    const amountCents = parseAmountCents(firstNonEmpty(row, ["ContributionAmt", "Contribution Amount", "Amount"]));
    if (amountCents !== null && amountCents > 0) {
      totalCents += amountCents;
    }
  }
  return centsToDollars(totalCents);
}

function findFilerName(input: {
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  filerNumber: string;
}): string | null {
  const filerNumber = normalizeFilerNumber(input.filerNumber);
  for (const row of input.contributionRows) {
    if (normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"])) === filerNumber) {
      const filerName = displayFilerName(row);
      return filerName || null;
    }
  }
  return null;
}

function buildExpectedComparisons(input: {
  args: LouisianaFinanceProbeArgs;
  directContributionTotal: number;
  outsideSupportTotal: number;
  pacReceiptsTotal: number | null;
}): LouisianaFinanceProbeExpectedComparison[] {
  const comparisons: LouisianaFinanceProbeExpectedComparison[] = [];

  function add(label: LouisianaFinanceProbeExpectedComparison["label"], expectedTotal: number | null, observedTotal: number | null): void {
    if (expectedTotal === null) {
      return;
    }
    if (observedTotal === null) {
      throw new Error(`Cannot compare ${label}; observed total is unavailable`);
    }
    const delta = Math.round((observedTotal - expectedTotal) * 100) / 100;
    comparisons.push({
      label,
      expected_total: expectedTotal,
      observed_total: observedTotal,
      delta,
      tolerance: input.args.expectedTolerance,
      ok: Math.abs(delta) <= input.args.expectedTolerance,
    });
  }

  add("direct_contributions", input.args.expectedDirectTotal, input.directContributionTotal);
  add("outside_support", input.args.expectedOutsideSupportTotal, input.outsideSupportTotal);
  add("pac_receipts", input.args.expectedPacReceiptsTotal, input.pacReceiptsTotal);
  return comparisons;
}

function buildAmbiguousCandidateCheck(input: {
  args: LouisianaFinanceProbeArgs;
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
}): LouisianaFinanceProbeAmbiguousCheck | null {
  if (!input.args.ambiguousCandidateName) {
    return null;
  }
  const resolution = resolveLouisianaCandidateCommittee({
    candidateName: input.args.ambiguousCandidateName,
    officeScope: input.args.officeScope,
    officeName: input.args.officeName,
    electionYear: input.args.electionYear,
    district: input.args.district,
    candidateRows: input.contributionRows,
  });
  const ok =
    input.args.expectedAmbiguousStatus === "not_matched"
      ? resolution.status !== "matched"
      : resolution.status === input.args.expectedAmbiguousStatus;
  return {
    candidate_name: input.args.ambiguousCandidateName,
    expected_status: input.args.expectedAmbiguousStatus,
    actual_status: resolution.status,
    ok,
  };
}

async function loadProbeRows(args: LouisianaFinanceProbeArgs): Promise<LouisianaProbeRows> {
  const range = { startYear: args.startYear, endYear: args.endYear };
  const paths = getLouisianaCampaignFinanceArtifactCachePaths(args.cacheDir, range);
  const cacheRefresh = args.refreshCache
    ? await refreshLouisianaCampaignFinanceArtifactCache({
        cacheDir: args.cacheDir,
        range,
        force: args.forceRefresh,
      })
    : null;
  const metadata = await readLouisianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  const contributionPath = args.contributionCsvPath ?? paths.downloads.contributions;
  const expenditurePath = args.expenditureCsvPath ?? paths.downloads.expenditures;
  const contributionSourceUrl =
    args.contributionCsvPath === null ? metadata?.downloads.contributions.remote.url ?? contributionPath : contributionPath;
  const expenditureSourceUrl =
    args.expenditureCsvPath === null ? metadata?.downloads.expenditures.remote.url ?? expenditurePath : expenditurePath;

  return {
    contributionRows: await readLouisianaCampaignFinanceContributionRows({ filePath: contributionPath }),
    expenditureRows: await readLouisianaCampaignFinanceExpenditureRows({ filePath: expenditurePath }),
    contributionSourceUrl,
    expenditureSourceUrl,
    cacheRefresh,
  };
}

function emptyOutput(input: {
  args: LouisianaFinanceProbeArgs;
  resolution: LouisianaCandidateCommitteeResolution;
  rows: LouisianaProbeRows;
  ambiguousCandidateCheck: LouisianaFinanceProbeAmbiguousCheck | null;
  now?: Date;
}): LouisianaFinanceProbeOutput {
  const noOccupationData = true;
  return {
    type: "louisiana_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: false,
    cache_refresh: input.rows.cacheRefresh,
    source_urls: {
      contributions: input.rows.contributionSourceUrl,
      expenditures: input.rows.expenditureSourceUrl,
    },
    resolution: input.resolution,
    validation: {
      expected_total_comparisons: [],
      expected_total_comparison_ok: null,
      no_occupation_data: noOccupationData,
      ambiguous_candidate_check: input.ambiguousCandidateCheck,
    },
    rows_loaded: {
      contributions: input.rows.contributionRows.length,
      expenditures: input.rows.expenditureRows.length,
    },
    direct_campaign: {
      total_receipts: 0,
      direct_contribution_total: 0,
      top_occupations: [],
      contributor_types: [],
      contribution_size_buckets: [],
    },
    outside_spending: {
      support_total: 0,
      oppose_total: null,
      top_supporting_groups: [],
      top_opposing_groups: [],
      top_supporting_industries: [],
      top_opposing_industries: [],
    },
    known_pac: null,
    counters: {
      direct_matched_rows: 0,
      direct_included_rows: 0,
      direct_skipped_rows: 0,
      outside_matched_rows: 0,
      outside_included_rows: 0,
      outside_skipped_rows: 0,
      outside_group_matched_rows: 0,
      outside_group_included_rows: 0,
      outside_group_skipped_rows: 0,
    },
  };
}

export async function runProbeLouisianaCandidateFinance(input: {
  args: LouisianaFinanceProbeArgs;
  now?: Date;
  rows?: LouisianaProbeRows;
}): Promise<LouisianaFinanceProbeOutput> {
  if (input.args.expectedPacReceiptsTotal !== null && !input.args.pacFilerNumber) {
    throw new Error("--expected-pac-receipts-total requires --pac-filer-number");
  }

  const rows = input.rows ?? (await loadProbeRows(input.args));
  const ambiguousCandidateCheck = buildAmbiguousCandidateCheck({
    args: input.args,
    contributionRows: rows.contributionRows,
  });
  const resolution = resolveLouisianaCandidateCommittee({
    candidateName: input.args.candidateName,
    officeScope: input.args.officeScope,
    officeName: input.args.officeName,
    electionYear: input.args.electionYear,
    district: input.args.district,
    candidateRows: rows.contributionRows,
    sourceUrl: rows.contributionSourceUrl,
  });

  if (resolution.status !== "matched") {
    return emptyOutput({ args: input.args, resolution, rows, ambiguousCandidateCheck, now: input.now });
  }

  const direct = aggregateLouisianaDirectContributions({
    filerNumber: resolution.filerNumber,
    electionYear: input.args.electionYear,
    contributionRows: rows.contributionRows,
    sourceUrl: rows.contributionSourceUrl,
    maxBreakdownsPerCategory: input.args.limit,
  });
  const outside = aggregateLouisianaOutsideSupport({
    candidateName: input.args.candidateName,
    candidateFilerName: resolution.filerName,
    electionYear: input.args.electionYear,
    expenditureRows: rows.expenditureRows,
    sourceUrl: rows.expenditureSourceUrl,
    maxGroups: input.args.limit,
  });
  const outsideGroupBreakdowns = aggregateLouisianaOutsideGroupContributions({
    electionYear: input.args.electionYear,
    outsideGroups: outside.summary.groups,
    contributionRows: rows.contributionRows,
    sourceUrl: rows.contributionSourceUrl,
    maxBreakdownsPerCategory: input.args.limit,
  });
  const industries = buildOutsideIndustries({
    groups: outside.summary.groups,
    breakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    limit: input.args.limit,
  });

  const pacReceiptsTotal = input.args.pacFilerNumber
    ? sumPositiveCycleContributionsForFiler({
        contributionRows: rows.contributionRows,
        filerNumber: input.args.pacFilerNumber,
        electionYear: input.args.electionYear,
      })
    : null;
  const expectedComparisons = buildExpectedComparisons({
    args: input.args,
    directContributionTotal: direct.summary.directContributionTotal,
    outsideSupportTotal: outside.summary.outsideSupportTotal,
    pacReceiptsTotal,
  });
  const knownPacBreakdowns = input.args.pacFilerNumber
    ? outsideGroupBreakdowns.outsideGroupBreakdowns.filter(
        (breakdown) => normalizeFilerNumber(breakdown.filerNumber) === normalizeFilerNumber(input.args.pacFilerNumber)
      )
    : [];
  const knownPacIndustries = input.args.pacFilerNumber
    ? buildOutsideIndustries({
        groups: outside.summary.groups.filter(
          (group) => normalizeFilerNumber(group.filerNumber) === normalizeFilerNumber(input.args.pacFilerNumber)
        ),
        breakdowns: knownPacBreakdowns,
        limit: input.args.limit,
      })
    : [];
  const noOccupationData = true;
  const expectedTotalComparisonOk =
    expectedComparisons.length > 0 ? expectedComparisons.every((comparison) => comparison.ok) : null;

  return {
    type: "louisiana_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok:
      expectedTotalComparisonOk !== false &&
      noOccupationData &&
      (ambiguousCandidateCheck?.ok ?? true),
    cache_refresh: rows.cacheRefresh,
    source_urls: {
      contributions: rows.contributionSourceUrl,
      expenditures: rows.expenditureSourceUrl,
    },
    resolution,
    validation: {
      expected_total_comparisons: expectedComparisons,
      expected_total_comparison_ok: expectedTotalComparisonOk,
      no_occupation_data: noOccupationData,
      ambiguous_candidate_check: ambiguousCandidateCheck,
    },
    rows_loaded: {
      contributions: rows.contributionRows.length,
      expenditures: rows.expenditureRows.length,
    },
    direct_campaign: {
      total_receipts: direct.summary.totalReceipts,
      direct_contribution_total: direct.summary.directContributionTotal,
      top_occupations: [],
      contributor_types: direct.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contributor_type")
        .map(mapBreakdown)
        .slice(0, input.args.limit),
      contribution_size_buckets: direct.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contribution_size")
        .map(mapBreakdown),
    },
    outside_spending: {
      support_total: outside.summary.outsideSupportTotal,
      oppose_total: outside.summary.outsideOpposeTotal,
      top_supporting_groups: outside.summary.groups.filter((group) => group.supportOppose === "support").map(mapOutsideGroup),
      top_opposing_groups: outside.summary.groups.filter((group) => group.supportOppose === "oppose").map(mapOutsideGroup),
      top_supporting_industries: industries.filter((industry) => industry.support_oppose === "support"),
      top_opposing_industries: industries.filter((industry) => industry.support_oppose === "oppose"),
    },
    known_pac: input.args.pacFilerNumber
      ? {
          filer_number: input.args.pacFilerNumber,
          filer_name: findFilerName({ contributionRows: rows.contributionRows, filerNumber: input.args.pacFilerNumber }),
          receipts_total: pacReceiptsTotal ?? 0,
          top_donors: knownPacBreakdowns
            .filter((breakdown) => breakdown.categoryType === "donor")
            .map((breakdown) => ({
              category_name: breakdown.categoryName,
              amount: breakdown.amount,
              contributor_count: breakdown.contributorCount,
              source_url: breakdown.sourceUrl,
            }))
            .slice(0, input.args.limit),
          top_industries: knownPacIndustries.filter((industry) => industry.support_oppose === "support"),
        }
      : null,
    counters: {
      direct_matched_rows: direct.matchedContributionRowCount,
      direct_included_rows: direct.includedContributionRowCount,
      direct_skipped_rows: direct.skippedContributionRowCount,
      outside_matched_rows: outside.matchedExpenditureRowCount,
      outside_included_rows: outside.includedExpenditureRowCount,
      outside_skipped_rows: outside.skippedExpenditureRowCount,
      outside_group_matched_rows: outsideGroupBreakdowns.matchedContributionRowCount,
      outside_group_included_rows: outsideGroupBreakdowns.includedContributionRowCount,
      outside_group_skipped_rows: outsideGroupBreakdowns.skippedContributionRowCount,
    },
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runProbeLouisianaCandidateFinance({
    args: parseProbeLouisianaCandidateFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Louisiana candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
