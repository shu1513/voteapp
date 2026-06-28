import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyFinanceLabel,
  FINANCE_INDUSTRY_SLUGS,
  type FinanceIndustrySlug,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  getVermontContributionDetails,
  getVermontExpenditureDetails,
  type VermontCampaignFinanceClientOptions,
  type VermontContributionRow,
  type VermontExpenditureRow,
  type VermontPagedResult,
  type VermontTransactionSearchInput,
} from "../pipeline/vermontFinance/vermontCampaignFinanceClient.js";
import {
  searchAndResolveVermontCandidateCommittee,
  type VermontCandidateCommitteeResolution,
} from "../pipeline/vermontFinance/vermontCandidateCommitteeResolver.js";
import { aggregateVermontDirectContributions } from "../pipeline/vermontFinance/vermontDirectContributionAggregator.js";
import {
  fetchAndAggregateVermontOutsideGroupContributions,
  type VermontFinanceOutsideGroupBreakdown,
  type VermontOutsideGroupContributionFetchAndAggregationResult,
} from "../pipeline/vermontFinance/vermontOutsideGroupContributionAggregator.js";
import {
  aggregateVermontOutsideSpending,
  type VermontOutsideSpendingGroup,
  type VermontSupportOppose,
} from "../pipeline/vermontFinance/vermontOutsideSpendingAggregator.js";

type VermontFinanceProbeArgs = {
  candidateName: string;
  electionYear: number;
  officeScope: "statewide" | "state_upper" | "state_lower";
  officeName: string;
  district: string | null;
  limit: number;
  pageSize: number;
  maxPages: number;
  outsideGroupMaxPages: number;
  minIndustryAmount: number;
  timeoutMs: number;
  directCsvPath: string | null;
  outsideSupportCsvPath: string | null;
  csvAmountColumn: string;
  csvTolerance: number;
};

type VermontFinanceProbeBreakdown = {
  category_name: string;
  amount: number;
  contributor_count: number;
  source_url: string | null;
};

type VermontFinanceProbeOutsideGroup = {
  filer_registration_guid: string;
  filer_name: string;
  support_oppose: VermontSupportOppose;
  support_mechanism: "vt_pac_contribution_to_registrant";
  amount: number;
  expenditure_count: number;
  entity_id: number | null;
  source_url: string | null;
};

type VermontFinanceProbeIndustryEvidence = {
  organization_name: string;
  amount: number;
  contributor_count: number;
  filer_registration_guid: string;
  filer_name: string;
  source_url: string | null;
};

type VermontFinanceProbeIndustry = VermontFinanceProbeBreakdown & {
  industry_slug: FinanceIndustrySlug;
  support_oppose: VermontSupportOppose;
  evidence: VermontFinanceProbeIndustryEvidence[];
};

type VermontFinanceProbeCsvComparison = {
  label: "direct_contributions" | "outside_support";
  file_path: string;
  amount_column: string;
  csv_total: number;
  api_total: number;
  delta: number;
  tolerance: number;
  ok: boolean;
};

type VermontFinanceProbeOutput = {
  type: "vermont_candidate_finance_live_probe";
  ts: string;
  args: VermontFinanceProbeArgs;
  ok: boolean;
  resolution: VermontCandidateCommitteeResolution;
  validation: {
    csv_comparisons: VermontFinanceProbeCsvComparison[];
    csv_comparison_ok: boolean | null;
  };
  rows_loaded: {
    candidate_contributions: number;
    expenditure_rows: number;
    outside_group_contributions: number;
  };
  direct_campaign: {
    total_receipts: number;
    direct_contribution_total: number;
    top_occupations: [];
    contributor_source_types: VermontFinanceProbeBreakdown[];
    contribution_size_buckets: VermontFinanceProbeBreakdown[];
  };
  outside_spending: {
    support_total: number;
    oppose_total: number;
    top_supporting_groups: VermontFinanceProbeOutsideGroup[];
    top_opposing_groups: VermontFinanceProbeOutsideGroup[];
    top_supporting_industries: VermontFinanceProbeIndustry[];
    top_opposing_industries: VermontFinanceProbeIndustry[];
  };
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

type VermontFinanceProbeClient = {
  resolveCandidateCommittee: (
    input: Omit<Parameters<typeof searchAndResolveVermontCandidateCommittee>[0], "transactionRows">,
    options?: VermontCampaignFinanceClientOptions
  ) => Promise<VermontCandidateCommitteeResolution>;
  getContributionDetails: (
    input: VermontTransactionSearchInput,
    options?: VermontCampaignFinanceClientOptions
  ) => Promise<VermontPagedResult<VermontContributionRow>>;
  getExpenditureDetails: (
    input: VermontTransactionSearchInput,
    options?: VermontCampaignFinanceClientOptions
  ) => Promise<VermontPagedResult<VermontExpenditureRow>>;
  fetchOutsideGroupContributions: (
    input: Parameters<typeof fetchAndAggregateVermontOutsideGroupContributions>[0],
    options?: VermontCampaignFinanceClientOptions
  ) => Promise<VermontOutsideGroupContributionFetchAndAggregationResult>;
};

const VERMONT_CAMPAIGN_FINANCE_SOURCE_URL = "https://campaignfinance.vermont.gov/";
const DEFAULT_LIMIT = 5;
const DEFAULT_PAGE_SIZE = 100;
const DEFAULT_MAX_PAGES = 20;
const DEFAULT_OUTSIDE_GROUP_MAX_PAGES = 10;
const DEFAULT_MIN_INDUSTRY_AMOUNT = 25_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_CSV_AMOUNT_COLUMN = "transactionAmount";
const DEFAULT_CSV_TOLERANCE = 0.01;
const INDUSTRY_SLUGS = new Set<string>(FINANCE_INDUSTRY_SLUGS);

const DEFAULT_CLIENT: VermontFinanceProbeClient = {
  resolveCandidateCommittee: searchAndResolveVermontCandidateCommittee,
  getContributionDetails: getVermontContributionDetails,
  getExpenditureDetails: getVermontExpenditureDetails,
  fetchOutsideGroupContributions: fetchAndAggregateVermontOutsideGroupContributions,
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

function parseOfficeScope(value: string | null): VermontFinanceProbeArgs["officeScope"] {
  const normalized = value?.trim() || "statewide";
  if (normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower") {
    return normalized;
  }
  throw new Error(`Invalid --scope value: ${value}`);
}

export function parseProbeVermontCandidateFinanceArgs(args: readonly string[]): VermontFinanceProbeArgs {
  return {
    candidateName: parseRequiredFlag(args, "--candidate-name"),
    electionYear: parseRequiredPositiveIntegerFlag(args, "--year"),
    officeScope: parseOfficeScope(parseFlagValue(args, "--scope")),
    officeName: parseRequiredFlag(args, "--office"),
    district: parseFlagValue(args, "--district"),
    limit: parsePositiveIntegerFlag(args, "--limit", DEFAULT_LIMIT),
    pageSize: parsePositiveIntegerFlag(args, "--page-size", DEFAULT_PAGE_SIZE),
    maxPages: parsePositiveIntegerFlag(args, "--max-pages", DEFAULT_MAX_PAGES),
    outsideGroupMaxPages: parsePositiveIntegerFlag(
      args,
      "--outside-group-max-pages",
      DEFAULT_OUTSIDE_GROUP_MAX_PAGES
    ),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount", DEFAULT_MIN_INDUSTRY_AMOUNT),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS),
    directCsvPath: parseFlagValue(args, "--direct-csv"),
    outsideSupportCsvPath: parseFlagValue(args, "--outside-support-csv"),
    csvAmountColumn: parseFlagValue(args, "--csv-amount-column") ?? DEFAULT_CSV_AMOUNT_COLUMN,
    csvTolerance: parseNonNegativeNumberFlag(args, "--csv-tolerance", DEFAULT_CSV_TOLERANCE),
  };
}

function emptyOutput(input: {
  args: VermontFinanceProbeArgs;
  resolution: VermontCandidateCommitteeResolution;
  now?: Date;
}): VermontFinanceProbeOutput {
  return {
    type: "vermont_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: false,
    resolution: input.resolution,
    validation: {
      csv_comparisons: [],
      csv_comparison_ok: null,
    },
    rows_loaded: {
      candidate_contributions: 0,
      expenditure_rows: 0,
      outside_group_contributions: 0,
    },
    direct_campaign: {
      total_receipts: 0,
      direct_contribution_total: 0,
      top_occupations: [],
      contributor_source_types: [],
      contribution_size_buckets: [],
    },
    outside_spending: {
      support_total: 0,
      oppose_total: 0,
      top_supporting_groups: [],
      top_opposing_groups: [],
      top_supporting_industries: [],
      top_opposing_industries: [],
    },
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

async function fetchTransactionRows<T>(input: {
  pageSize: number;
  maxPages: number;
  fetchPage: (pageNumber: number) => Promise<VermontPagedResult<T>>;
}): Promise<T[]> {
  const rows: T[] = [];
  for (let pageNumber = 1; pageNumber <= input.maxPages; pageNumber += 1) {
    const page = await input.fetchPage(pageNumber);
    rows.push(...page.items);
    if (page.items.length < input.pageSize || pageNumber * input.pageSize >= page.totalItems) {
      break;
    }
  }
  return rows;
}

function mapBreakdown(row: {
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
}): VermontFinanceProbeBreakdown {
  return {
    category_name: row.categoryName,
    amount: row.amount,
    contributor_count: row.contributorCount,
    source_url: row.sourceUrl ?? null,
  };
}

function mapOutsideGroup(group: VermontOutsideSpendingGroup): VermontFinanceProbeOutsideGroup {
  return {
    filer_registration_guid: group.filerRegistrationGuid,
    filer_name: group.filerName,
    support_oppose: group.supportOppose,
    support_mechanism: group.supportMechanism,
    amount: group.amount,
    expenditure_count: group.expenditureCount,
    entity_id: group.entityId,
    source_url: group.sourceUrl ?? null,
  };
}

function toIndustrySlug(value: string): FinanceIndustrySlug | null {
  return INDUSTRY_SLUGS.has(value) ? (value as FinanceIndustrySlug) : null;
}

function buildOutsideIndustries(input: {
  groups: readonly VermontOutsideSpendingGroup[];
  breakdowns: readonly VermontFinanceOutsideGroupBreakdown[];
  limit: number;
}): VermontFinanceProbeIndustry[] {
  const groupNames = new Map(
    input.groups.map((group) => [`${group.filerRegistrationGuid}\u0000${group.supportOppose}`, group.filerName])
  );
  const donorBreakdowns = input.breakdowns.filter((breakdown) => breakdown.categoryType === "donor");
  const industries = new Map<string, VermontFinanceProbeIndustry>();

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
      .filter(
        (donor) =>
          donor.filerRegistrationGuid === breakdown.filerRegistrationGuid &&
          donor.supportOppose === breakdown.supportOppose
      )
      .filter((donor) => classifyFinanceLabel({ rawLabel: donor.categoryName, labelType: "donor" }).industrySlug === industrySlug)
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, 5)
      .map((donor) => ({
        organization_name: donor.categoryName,
        amount: donor.amount,
        contributor_count: donor.contributorCount,
        filer_registration_guid: donor.filerRegistrationGuid,
        filer_name:
          groupNames.get(`${donor.filerRegistrationGuid}\u0000${donor.supportOppose}`) ?? donor.filerRegistrationGuid,
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

function parseCsvRows(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
        continue;
      }
      if (char === '"') {
        inQuotes = false;
        continue;
      }
      field += char;
      continue;
    }
    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (char === "\n" || char === "\r") {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(field);
      if (row.some((value) => value.trim().length > 0)) {
        rows.push(row);
      }
      row = [];
      field = "";
      continue;
    }
    field += char;
  }

  row.push(field);
  if (row.some((value) => value.trim().length > 0)) {
    rows.push(row);
  }
  return rows;
}

function normalizeColumnName(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9]+/g, "");
}

function parseCsvAmount(value: string): number {
  const trimmed = value.trim();
  const isNegative = /^\(.*\)$/.test(trimmed);
  const cleaned = trimmed.replace(/^\((.*)\)$/, "$1").replace(/[$,\s]/g, "");
  if (!/^-?(?:\d+|\d*\.\d+)$/.test(cleaned)) {
    throw new Error(`Invalid CSV amount value: ${value}`);
  }
  const amount = Number(cleaned);
  return isNegative ? -amount : amount;
}

export function sumCsvAmountColumnFromText(input: { text: string; amountColumn: string }): number {
  const rows = parseCsvRows(input.text);
  const headers = rows[0] ?? [];
  const amountColumnKey = normalizeColumnName(input.amountColumn);
  const amountColumnIndex = headers.findIndex((header) => normalizeColumnName(header) === amountColumnKey);
  if (amountColumnIndex < 0) {
    throw new Error(`CSV amount column not found: ${input.amountColumn}`);
  }

  let totalCents = 0;
  for (const row of rows.slice(1)) {
    const raw = row[amountColumnIndex];
    if (raw === undefined || raw.trim().length === 0) {
      continue;
    }
    totalCents += Math.round(parseCsvAmount(raw) * 100);
  }
  return totalCents / 100;
}

async function buildCsvComparisons(input: {
  args: VermontFinanceProbeArgs;
  readCsvFile: (path: string) => Promise<string>;
  directContributionTotal: number;
  outsideSupportTotal: number;
}): Promise<VermontFinanceProbeCsvComparison[]> {
  const comparisons: VermontFinanceProbeCsvComparison[] = [];

  async function addComparison(label: VermontFinanceProbeCsvComparison["label"], filePath: string, apiTotal: number): Promise<void> {
    const csvTotal = sumCsvAmountColumnFromText({
      text: await input.readCsvFile(filePath),
      amountColumn: input.args.csvAmountColumn,
    });
    const delta = Math.round((apiTotal - csvTotal) * 100) / 100;
    comparisons.push({
      label,
      file_path: filePath,
      amount_column: input.args.csvAmountColumn,
      csv_total: csvTotal,
      api_total: apiTotal,
      delta,
      tolerance: input.args.csvTolerance,
      ok: Math.abs(delta) <= input.args.csvTolerance,
    });
  }

  if (input.args.directCsvPath) {
    await addComparison("direct_contributions", input.args.directCsvPath, input.directContributionTotal);
  }
  if (input.args.outsideSupportCsvPath) {
    await addComparison("outside_support", input.args.outsideSupportCsvPath, input.outsideSupportTotal);
  }

  return comparisons;
}

export async function runProbeVermontCandidateFinance(input: {
  args: VermontFinanceProbeArgs;
  client?: Partial<VermontFinanceProbeClient>;
  now?: Date;
  readCsvFile?: (path: string) => Promise<string>;
}): Promise<VermontFinanceProbeOutput> {
  const client = { ...DEFAULT_CLIENT, ...(input.client ?? {}) };
  const clientOptions: VermontCampaignFinanceClientOptions = { timeoutMs: input.args.timeoutMs };
  const readCsvFile = input.readCsvFile ?? ((path: string) => readFile(path, "utf8"));
  const resolution = await client.resolveCandidateCommittee(
    {
      candidateName: input.args.candidateName,
      officeScope: input.args.officeScope,
      officeName: input.args.officeName,
      district: input.args.district,
      electionYear: input.args.electionYear,
    },
    clientOptions
  );

  if (resolution.status !== "matched") {
    return emptyOutput({ args: input.args, resolution, now: input.now });
  }

  const [contributionRows, expenditureRows] = await Promise.all([
    fetchTransactionRows({
      pageSize: input.args.pageSize,
      maxPages: input.args.maxPages,
      fetchPage: (pageNumber) =>
        client.getContributionDetails(
          {
            pageNumber,
            pageSize: input.args.pageSize,
            filerRegistrationGuid: resolution.filerRegistrationGuid,
            electionYear: input.args.electionYear,
            transactionTypeCode: "TCON",
          },
          clientOptions
        ),
    }),
    fetchTransactionRows({
      pageSize: input.args.pageSize,
      maxPages: input.args.maxPages,
      fetchPage: (pageNumber) =>
        client.getExpenditureDetails(
          {
            pageNumber,
            pageSize: input.args.pageSize,
            electionYear: input.args.electionYear,
            transactionTypeCode: "TEXP",
          },
          clientOptions
        ),
    }),
  ]);

  const direct = aggregateVermontDirectContributions({
    filerRegistrationGuid: resolution.filerRegistrationGuid,
    electionYear: input.args.electionYear,
    contributionRows,
    sourceUrl: VERMONT_CAMPAIGN_FINANCE_SOURCE_URL,
    maxBreakdownsPerCategory: input.args.limit,
  });
  const outside = aggregateVermontOutsideSpending({
    candidateName: input.args.candidateName,
    candidateEntityId: resolution.entityId,
    electionYear: input.args.electionYear,
    expenditureRows,
    sourceUrl: VERMONT_CAMPAIGN_FINANCE_SOURCE_URL,
    maxGroups: input.args.limit,
  });
  const outsideGroupBreakdowns = await client.fetchOutsideGroupContributions(
    {
      electionYear: input.args.electionYear,
      outsideGroups: outside.summary.groups,
      sourceUrl: VERMONT_CAMPAIGN_FINANCE_SOURCE_URL,
      maxBreakdownsPerCategory: input.args.limit,
      minIndustryAmount: input.args.minIndustryAmount,
      pageSize: input.args.pageSize,
      maxPagesPerGroup: input.args.outsideGroupMaxPages,
    },
    clientOptions
  );
  const groups = outside.summary.groups;
  const industries = buildOutsideIndustries({
    groups,
    breakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    limit: input.args.limit,
  });
  const csvComparisons = await buildCsvComparisons({
    args: input.args,
    readCsvFile,
    directContributionTotal: direct.summary.directContributionTotal,
    outsideSupportTotal: outside.summary.outsideSupportTotal,
  });

  return {
    type: "vermont_candidate_finance_live_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: input.args,
    ok: true,
    resolution,
    validation: {
      csv_comparisons: csvComparisons,
      csv_comparison_ok: csvComparisons.length > 0 ? csvComparisons.every((comparison) => comparison.ok) : null,
    },
    rows_loaded: {
      candidate_contributions: contributionRows.length,
      expenditure_rows: expenditureRows.length,
      outside_group_contributions: outsideGroupBreakdowns.fetchedContributionRowCount,
    },
    direct_campaign: {
      total_receipts: direct.summary.totalReceipts,
      direct_contribution_total: direct.summary.directContributionTotal,
      top_occupations: [],
      contributor_source_types: direct.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contributor_source_type")
        .map(mapBreakdown)
        .slice(0, input.args.limit),
      contribution_size_buckets: direct.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contribution_size")
        .map(mapBreakdown),
    },
    outside_spending: {
      support_total: outside.summary.outsideSupportTotal,
      oppose_total: outside.summary.outsideOpposeTotal,
      top_supporting_groups: groups.filter((group) => group.supportOppose === "support").map(mapOutsideGroup),
      top_opposing_groups: groups.filter((group) => group.supportOppose === "oppose").map(mapOutsideGroup),
      top_supporting_industries: industries.filter((industry) => industry.support_oppose === "support"),
      top_opposing_industries: industries.filter((industry) => industry.support_oppose === "oppose"),
    },
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
  const output = await runProbeVermontCandidateFinance({
    args: parseProbeVermontCandidateFinanceArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Vermont candidate finance live probe failed:", message);
    process.exitCode = 1;
  });
}
