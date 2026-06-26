import { normalizeTennesseeCandidateNameKeys } from "./tennesseeCandidateCommitteeResolver.js";
import type { TennesseeCampExpenditureRecord } from "./tennesseeCampClient.js";

export type TennesseeSupportOppose = "support" | "oppose";

export type TennesseeOutsideSpendingGroup = {
  committeeKey: string;
  committeeName: string;
  supportOppose: TennesseeSupportOppose;
  amount: number;
  expenditureCount: number;
  sourceUrl: string | null;
};

export type TennesseeOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: TennesseeOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type TennesseeOutsideSpendingAggregationInput = {
  candidateName: string;
  ownerName?: string | null;
  electionYear: number;
  expenditureRecords: readonly TennesseeCampExpenditureRecord[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type TennesseeOutsideSpendingAggregationResult = {
  summary: TennesseeOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  committeeKey: string;
  committeeName: string;
  supportOppose: TennesseeSupportOppose;
  amountCents: number;
  expenditureCount: number;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Tennessee outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Tennessee outside spending aggregation ${fieldName}: ${value}`);
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
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeCommitteeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

function amountToCents(amount: number): number | null {
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseDateYear(raw: string | null | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[3]) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isCycleYear(input: { record: TennesseeCampExpenditureRecord; electionYear: number }): boolean {
  const year = parseDateYear(input.record.date) ?? input.record.electionYear;
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function candidateKeysForMatch(input: { candidateName: string; ownerName?: string | null }): Set<string> {
  return new Set([
    ...normalizeTennesseeCandidateNameKeys(input.candidateName),
    ...normalizeTennesseeCandidateNameKeys(input.ownerName ?? ""),
  ]);
}

function targetMatchesCandidate(input: {
  candidateFor: string | null;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of normalizeTennesseeCandidateNameKeys(input.candidateFor ?? "")) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function supportOpposeFromCode(value: string | null): TennesseeSupportOppose | null {
  const normalized = normalizeTextKey(value);
  if (normalized === "S" || normalized === "SUPPORT") {
    return "support";
  }
  if (normalized === "O" || normalized === "OPPOSE") {
    return "oppose";
  }
  return null;
}

function isIndependentExpenditure(record: TennesseeCampExpenditureRecord): boolean {
  return normalizeTextKey(record.type) === "INDEPENDENT";
}

function groupKey(input: { committeeKey: string; supportOppose: TennesseeSupportOppose }): string {
  return `${input.committeeKey}\u0000${input.supportOppose}`;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): TennesseeOutsideSpendingGroup[] {
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.committeeName.localeCompare(right.committeeName)
    )
    .slice(0, input.maxGroups)
    .map((group) => ({
      committeeKey: group.committeeKey,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      expenditureCount: group.expenditureCount,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateTennesseeOutsideSpending(
  input: TennesseeOutsideSpendingAggregationInput
): TennesseeOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const candidateNameKeys = candidateKeysForMatch({ candidateName: input.candidateName, ownerName: input.ownerName });
  if (candidateNameKeys.size === 0) {
    return {
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    };
  }

  const groups = new Map<string, GroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let matchedExpenditureRowCount = 0;
  let includedExpenditureRowCount = 0;
  let skippedExpenditureRowCount = 0;

  for (const record of input.expenditureRecords) {
    if (!targetMatchesCandidate({ candidateFor: record.candidateFor, candidateNameKeys })) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const committeeName = record.candidatePacName?.trim() ?? "";
    const committeeKey = normalizeCommitteeKey(committeeName);
    const supportOppose = supportOpposeFromCode(record.supportOpposeCode);
    const amountCents = amountToCents(record.amount);
    if (
      !committeeName ||
      !committeeKey ||
      !supportOppose ||
      amountCents === null ||
      amountCents <= 0 ||
      !isIndependentExpenditure(record) ||
      normalizeTextKey(record.adjustment) === "Y" ||
      !isCycleYear({ record, electionYear })
    ) {
      skippedExpenditureRowCount += 1;
      continue;
    }

    includedExpenditureRowCount += 1;
    if (supportOppose === "support") {
      supportTotalCents += amountCents;
    } else {
      opposeTotalCents += amountCents;
    }

    const key = groupKey({ committeeKey, supportOppose });
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      existing.expenditureCount += 1;
      continue;
    }
    groups.set(key, {
      committeeKey,
      committeeName,
      supportOppose,
      amountCents,
      expenditureCount: 1,
    });
  }

  const grouped = toGroups({
    groups: groups.values(),
    maxGroups,
    sourceUrl: input.sourceUrl ?? null,
  });
  if (grouped.length === 0) {
    return {
      summary: null,
      matchedExpenditureRowCount,
      includedExpenditureRowCount,
      skippedExpenditureRowCount,
    };
  }

  return {
    summary: {
      supportTotal: centsToDollars(supportTotalCents),
      opposeTotal: centsToDollars(opposeTotalCents),
      groups: grouped,
      sourceUrl: input.sourceUrl ?? null,
    },
    matchedExpenditureRowCount,
    includedExpenditureRowCount,
    skippedExpenditureRowCount,
  };
}
