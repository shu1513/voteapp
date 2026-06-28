import type { LouisianaCampaignFinanceCsvRow } from "./louisianaCampaignFinanceArtifactReader.js";
import { normalizeLouisianaCandidateNameKeys } from "./louisianaCandidateCommitteeResolver.js";

export type LouisianaSupportOppose = "support" | "oppose";
export type LouisianaSupportMechanism = "la_pac_contribution_to_candidate";

export type LouisianaOutsideSupportGroup = {
  filerNumber: string;
  filerName: string;
  supportOppose: LouisianaSupportOppose;
  supportMechanism: LouisianaSupportMechanism;
  amount: number;
  expenditureCount: number;
  sourceUrl: string | null;
};

export type LouisianaOutsideSupportSummary = {
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  sourceUrl: string | null;
  groups: LouisianaOutsideSupportGroup[];
};

export type LouisianaOutsideSupportAggregationInput = {
  candidateName: string;
  candidateFilerName?: string | null;
  candidateCommitteeName?: string | null;
  electionYear: number;
  expenditureRows: readonly LouisianaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type LouisianaOutsideSupportAggregationResult = {
  summary: LouisianaOutsideSupportSummary;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  filerNumber: string;
  filerName: string;
  supportOppose: LouisianaSupportOppose;
  supportMechanism: LouisianaSupportMechanism;
  amountCents: number;
  expenditureCount: number;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Louisiana outside support aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Louisiana outside support aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|TO|ELECT|COMMITTEE|FRIENDS|CAMPAIGN)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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

function normalizeFilerNumber(value: string): string {
  return value.trim().replace(/\s+/g, "");
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
  if (!trimmed) {
    return null;
  }
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

function filerDisplayName(row: LouisianaCampaignFinanceCsvRow): string {
  const explicit = firstNonEmpty(row, ["FilerName", "Filer Name"]);
  if (explicit) {
    return explicit;
  }
  const firstName = firstNonEmpty(row, ["FilerFirstName", "Filer First Name"]);
  const lastName = firstNonEmpty(row, ["FilerLastName", "Filer Last Name"]);
  return [firstName, lastName].filter(Boolean).join(" ").trim() || lastName || firstName;
}

function isPacLikeFiler(row: LouisianaCampaignFinanceCsvRow): boolean {
  const reportCode = normalizeTextKey(`${firstNonEmpty(row, ["ReportCode", "Report Code"])} ${firstNonEmpty(row, ["ReportType", "Report Type"])}`);
  if (/\bF?202\b/.test(reportCode)) {
    return true;
  }
  const filerName = normalizeTextKey(filerDisplayName(row));
  return /\b(PAC|POLITICAL ACTION)\b/.test(filerName);
}

function isContributionSchedule(row: LouisianaCampaignFinanceCsvRow): boolean {
  const schedule = normalizeTextKey(firstNonEmpty(row, ["Schedule"]));
  if (schedule && schedule !== "E 3" && schedule !== "E3") {
    return false;
  }

  const text = normalizeTextKey(
    `${firstNonEmpty(row, ["ExpenditureDescription", "Expenditure Description"])} ${firstNonEmpty(row, [
      "RecipientName",
      "Recipient Name",
    ])}`
  );
  if (!text || /\b(REFUND|REFUNDED|RETURNED|REIMBURSEMENT|FEE|FEES)\b/.test(text)) {
    return false;
  }
  return /\b(CONTRIBUTION|DONATION|DONATE|CAMPAIGN CONTRIBUTION)\b/.test(text);
}

function rowNames(row: LouisianaCampaignFinanceCsvRow): string[] {
  return [
    firstNonEmpty(row, ["CandidateBeneficiary", "Candidate Beneficiary"]),
    firstNonEmpty(row, ["RecipientName", "Recipient Name"]),
  ].filter(Boolean);
}

function rowMatchesTargetCandidate(input: {
  row: LouisianaCampaignFinanceCsvRow;
  candidateNameKeys: ReadonlySet<string>;
  targetAliasKeys: ReadonlySet<string>;
}): boolean {
  for (const name of rowNames(input.row)) {
    for (const key of normalizeLouisianaCandidateNameKeys(name)) {
      if (input.candidateNameKeys.has(key)) {
        return true;
      }
    }
    if (input.targetAliasKeys.has(normalizeTextKey(name))) {
      return true;
    }
  }
  return false;
}

function groupKey(input: { filerNumber: string; supportOppose: LouisianaSupportOppose }): string {
  return `${normalizeFilerNumber(input.filerNumber)}\u0000${input.supportOppose}`;
}

function toGroups(input: { groups: Iterable<GroupAccumulator>; sourceUrl: string | null }): LouisianaOutsideSupportGroup[] {
  return [...input.groups]
    .sort((left, right) => right.amountCents - left.amountCents || left.filerName.localeCompare(right.filerName))
    .map((group) => ({
      filerNumber: group.filerNumber,
      filerName: group.filerName,
      supportOppose: group.supportOppose,
      supportMechanism: group.supportMechanism,
      amount: centsToDollars(group.amountCents),
      expenditureCount: group.expenditureCount,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateLouisianaOutsideSupport(
  input: LouisianaOutsideSupportAggregationInput
): LouisianaOutsideSupportAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeLouisianaCandidateNameKeys(input.candidateName);
  if (candidateNameKeys.size === 0) {
    throw new Error("Louisiana outside support candidate name is required");
  }
  const targetAliasKeys = new Set(
    [input.candidateName, input.candidateFilerName ?? "", input.candidateCommitteeName ?? ""]
      .map(normalizeTextKey)
      .filter(Boolean)
  );
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const sourceUrl = input.sourceUrl ?? null;
  const groups = new Map<string, GroupAccumulator>();
  let matchedExpenditureRowCount = 0;
  let includedExpenditureRowCount = 0;
  let skippedExpenditureRowCount = 0;

  for (const row of input.expenditureRows) {
    if (!rowMatchesTargetCandidate({ row, candidateNameKeys, targetAliasKeys })) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const filerNumber = normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"]));
    const filerName = filerDisplayName(row);
    const amountCents = parseAmountCents(firstNonEmpty(row, ["ExpenditureAmt", "Expenditure Amount", "Amount"]));
    if (
      !filerNumber ||
      !filerName ||
      amountCents === null ||
      amountCents <= 0 ||
      !isElectionCycleDate({
        rawDate: firstNonEmpty(row, ["ExpenditureDate", "Expenditure Date"]),
        electionYear,
      }) ||
      !isPacLikeFiler(row) ||
      !isContributionSchedule(row)
    ) {
      skippedExpenditureRowCount += 1;
      continue;
    }

    includedExpenditureRowCount += 1;
    const supportOppose: LouisianaSupportOppose = "support";
    const key = groupKey({ filerNumber, supportOppose });
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      existing.expenditureCount += 1;
      continue;
    }
    groups.set(key, {
      filerNumber,
      filerName,
      supportOppose,
      supportMechanism: "la_pac_contribution_to_candidate",
      amountCents,
      expenditureCount: 1,
    });
  }

  const allOutsideGroups = toGroups({ groups: groups.values(), sourceUrl });
  const outsideGroups = allOutsideGroups.slice(0, maxGroups);
  const outsideSupportTotal = allOutsideGroups
    .filter((group) => group.supportOppose === "support")
    .reduce((sum, group) => sum + group.amount, 0);
  const outsideOpposeTotal = allOutsideGroups
    .filter((group) => group.supportOppose === "oppose")
    .reduce((sum, group) => sum + group.amount, 0);

  return {
    summary: {
      outsideSupportTotal: Math.round(outsideSupportTotal * 100) / 100,
      outsideOpposeTotal: Math.round(outsideOpposeTotal * 100) / 100,
      sourceUrl,
      groups: outsideGroups,
    },
    matchedExpenditureRowCount,
    includedExpenditureRowCount,
    skippedExpenditureRowCount,
  };
}
