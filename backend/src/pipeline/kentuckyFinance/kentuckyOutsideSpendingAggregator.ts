import type { KentuckyKrefIndependentExpenditureRecord } from "./kentuckyKrefClient.js";

export type KentuckySupportOppose = "support" | "oppose";

export type KentuckyOutsideSpendingGroup = {
  committeeKey: string;
  committeeName: string;
  supportOppose: KentuckySupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type KentuckyOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: KentuckyOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type KentuckyOutsideSpendingAggregationInput = {
  candidateName: string;
  electionDate: string;
  officeOrBallotMeasure: string;
  expenditureRecords: readonly KentuckyKrefIndependentExpenditureRecord[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type KentuckyOutsideSpendingAggregationResult = {
  summary: KentuckyOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  committeeKey: string;
  committeeName: string;
  supportOppose: KentuckySupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS = 50;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Kentucky outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
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

function officeNameKeys(value: string | null | undefined): Set<string> {
  const normalized = normalizeTextKey(value).replace(/\s*\b(EVEN|ODD)\b\s*$/, "").trim();
  const keys = new Set<string>();
  if (normalized) {
    keys.add(normalized);
  }
  if (normalized === "STATE LOWER CHAMBER LEGISLATOR" || normalized === "STATE REPRESENTATIVE") {
    keys.add("STATE LOWER CHAMBER LEGISLATOR");
    keys.add("STATE REPRESENTATIVE");
  }
  if (normalized === "STATE UPPER CHAMBER LEGISLATOR" || normalized === "STATE SENATOR") {
    keys.add("STATE UPPER CHAMBER LEGISLATOR");
    keys.add("STATE SENATOR");
  }
  return keys;
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeKentuckyCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();

  const normalized = normalizePersonName(trimmed);
  if (normalized) {
    keys.add(normalized);
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
  }

  const commaParts = trimmed
    .split(",")
    .map((part) => normalizePersonName(part))
    .filter(Boolean);
  if (commaParts.length >= 2) {
    const lastName = commaParts[0] ?? "";
    const firstNames = commaParts.slice(1).join(" ").trim();
    const flipped = normalizePersonName(`${firstNames} ${lastName}`);
    if (flipped) {
      keys.add(flipped);
      const flippedParts = flipped.split(" ").filter(Boolean);
      if (flippedParts.length >= 2) {
        keys.add(`${flippedParts[0]} ${flippedParts[flippedParts.length - 1]}`);
      }
    }
  }

  return keys;
}

function candidateNamesMatch(input: { expectedKeys: ReadonlySet<string>; actualName: string | undefined }): boolean {
  for (const key of normalizeKentuckyCandidateNameKeys(input.actualName ?? "")) {
    if (input.expectedKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function parseDateKey(value: string | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }

  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[1] && slashMatch[2] && slashMatch[3]) {
    const month = slashMatch[1].padStart(2, "0");
    const day = slashMatch[2].padStart(2, "0");
    return `${slashMatch[3]}-${month}-${day}`;
  }

  const isoMatch = /^(\d{4})-(\d{2})-(\d{2})\b/.exec(trimmed);
  if (isoMatch?.[1] && isoMatch[2] && isoMatch[3]) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
  }

  return null;
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

function groupKey(input: { committeeKey: string; supportOppose: KentuckySupportOppose }): string {
  return `${input.committeeKey}\u0000${input.supportOppose}`;
}

function recordMatchesTarget(input: {
  record: KentuckyKrefIndependentExpenditureRecord;
  candidateNameKeys: ReadonlySet<string>;
  electionDateKey: string;
  officeOrBallotMeasureKeys: ReadonlySet<string>;
}): boolean {
  return (
    candidateNamesMatch({ expectedKeys: input.candidateNameKeys, actualName: input.record.candidateName }) &&
    // Cycle-year match, not exact-date: KREF tags rows to the specific
    // election (primary vs general), so an exact-date rule drops all
    // primary-tagged spending for a general-election candidate mid-cycle.
    parseDateKey(input.record.electionDate)?.slice(0, 4) === input.electionDateKey.slice(0, 4) &&
    [...officeNameKeys(input.record.officeOrBallotMeasure)].some((key) => input.officeOrBallotMeasureKeys.has(key))
  );
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): KentuckyOutsideSpendingGroup[] {
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
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateKentuckyOutsideSpending(
  input: KentuckyOutsideSpendingAggregationInput
): KentuckyOutsideSpendingAggregationResult {
  const candidateNameKeys = normalizeKentuckyCandidateNameKeys(input.candidateName);
  if (candidateNameKeys.size === 0) {
    return {
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    };
  }

  const electionDateKey = parseDateKey(requireNonEmpty(input.electionDate, "Kentucky outside spending election date"));
  if (!electionDateKey) {
    throw new Error("Kentucky outside spending election date must use MM/DD/YYYY or YYYY-MM-DD format");
  }
  const officeOrBallotMeasureKeys = officeNameKeys(
    requireNonEmpty(input.officeOrBallotMeasure, "Kentucky outside spending office or ballot measure")
  );
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const sourceUrl = input.sourceUrl ?? null;

  const groups = new Map<string, GroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let matchedExpenditureRowCount = 0;
  let includedExpenditureRowCount = 0;
  let skippedExpenditureRowCount = 0;

  for (const record of input.expenditureRecords) {
    if (!recordMatchesTarget({ record, candidateNameKeys, electionDateKey, officeOrBallotMeasureKeys })) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const committeeName = record.spenderName?.trim().replace(/\s+/g, " ") ?? "";
    const committeeKey = normalizeCommitteeKey(committeeName);
    const amountCents = amountToCents(record.amount);
    if (!committeeName || !committeeKey || !record.supportOppose || amountCents === null || amountCents <= 0) {
      skippedExpenditureRowCount += 1;
      continue;
    }

    includedExpenditureRowCount += 1;
    if (record.supportOppose === "support") {
      supportTotalCents += amountCents;
    } else {
      opposeTotalCents += amountCents;
    }

    const key = groupKey({ committeeKey, supportOppose: record.supportOppose });
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    groups.set(key, {
      committeeKey,
      committeeName,
      supportOppose: record.supportOppose,
      amountCents,
    });
  }

  const groupedRows = toGroups({ groups: groups.values(), maxGroups, sourceUrl });
  return {
    summary:
      groupedRows.length > 0
        ? {
            supportTotal: centsToDollars(supportTotalCents),
            opposeTotal: centsToDollars(opposeTotalCents),
            groups: groupedRows,
            sourceUrl,
          }
        : null,
    matchedExpenditureRowCount,
    includedExpenditureRowCount,
    skippedExpenditureRowCount,
  };
}
