import type { NewMexicoCfisExpenditureRow } from "./newMexicoCfisArtifactReader.js";
import {
  newMexicoCandidateNameMiddleConflict,
  normalizeNewMexicoCandidateNameKeys,
} from "./newMexicoCandidateCommitteeResolver.js";

export type NewMexicoSupportOppose = "support" | "oppose";

export type NewMexicoOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: NewMexicoSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type NewMexicoOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: NewMexicoOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type NewMexicoOutsideSpendingAggregationInput = {
  candidateName: string;
  electionYear: number;
  expenditureRows: readonly NewMexicoCfisExpenditureRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type NewMexicoOutsideSpendingAggregationResult = {
  summary: NewMexicoOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: NewMexicoSupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2020 || value > 2100) {
    throw new Error(`Invalid New Mexico outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Mexico outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0 || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }

  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseNewMexicoCfisDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseNewMexicoCfisDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isIndependentExpenditureEntity(row: NewMexicoCfisExpenditureRow): boolean {
  const entityType = normalizeTextKey(row["Report Entity Type"]);
  return /\bINDEPENDENT EXPENDITURE\b/.test(entityType);
}

function supportOpposeFromStance(value: string): NewMexicoSupportOppose | null {
  const normalized = normalizeTextKey(value);
  if (normalized === "SUPPORT" || normalized === "SUPPORTED" || normalized === "FOR") {
    return "support";
  }
  if (normalized === "OPPOSE" || normalized === "OPPOSED" || normalized === "AGAINST") {
    return "oppose";
  }
  return null;
}

function targetMatchesCandidate(input: {
  reason: string;
  candidateName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const reasonKeys = normalizeNewMexicoCandidateNameKeys(input.reason);
  if (reasonKeys.size === 0) {
    return false;
  }
  for (const key of reasonKeys) {
    if (input.candidateNameKeys.has(key)) {
      return !newMexicoCandidateNameMiddleConflict(input.candidateName, input.reason);
    }
  }
  return false;
}

function groupKey(input: { committeeId: string; supportOppose: NewMexicoSupportOppose }): string {
  return `${input.committeeId}\u0000${input.supportOppose}`;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): NewMexicoOutsideSpendingGroup[] {
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.committeeName.localeCompare(right.committeeName)
    )
    .slice(0, input.maxGroups)
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateNewMexicoOutsideSpending(
  input: NewMexicoOutsideSpendingAggregationInput
): NewMexicoOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const candidateNameKeys = normalizeNewMexicoCandidateNameKeys(input.candidateName);
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

  for (const row of input.expenditureRows) {
    if (!targetMatchesCandidate({ reason: row.Reason, candidateName: input.candidateName, candidateNameKeys })) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const committeeId = normalizeId(row.OrgID);
    const committeeName = row["Committee Name"].trim();
    const supportOppose = supportOpposeFromStance(row.Stance);
    const amountCents = parseAmountCents(row["Expenditure Amount"]);
    if (
      !committeeId ||
      !committeeName ||
      !supportOppose ||
      amountCents === null ||
      amountCents <= 0 ||
      !isIndependentExpenditureEntity(row) ||
      !isCycleYear({ rawDate: row["Expenditure Date"], electionYear })
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

    const key = groupKey({ committeeId, supportOppose });
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    groups.set(key, {
      committeeId,
      committeeName,
      supportOppose,
      amountCents,
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
