import { normalizeDistrictOfColumbiaCandidateNameKeys } from "./districtOfColumbiaCandidateCommitteeResolver.js";
import type { DistrictOfColumbiaOcfExpenditureRecord } from "./districtOfColumbiaOcfClient.js";

export type DistrictOfColumbiaSupportOppose = "support" | "oppose";

export type DistrictOfColumbiaOutsideSpendingGroup = {
  committeeKey: string;
  committeeName: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type DistrictOfColumbiaOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: DistrictOfColumbiaOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type DistrictOfColumbiaOutsideSpendingAggregationInput = {
  candidateName: string;
  electionYear: number;
  expenditureRecords: readonly DistrictOfColumbiaOcfExpenditureRecord[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type DistrictOfColumbiaOutsideSpendingAggregationResult = {
  summary: DistrictOfColumbiaOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  committeeKey: string;
  committeeName: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  amountCents: number;
};

type DirectionMatch = {
  supportOppose: DistrictOfColumbiaSupportOppose;
  targetText: string;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid D.C. outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid D.C. outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

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

function parseDistrictOfColumbiaOcfDateYear(raw: string | undefined): number | null {
  const trimmed = raw?.trim();
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

function isCycleYear(input: { rawDate: string | undefined; electionYear: number }): boolean {
  const year = parseDistrictOfColumbiaOcfDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isIndependentExpenditurePurpose(value: string | undefined): boolean {
  const normalized = normalizeTextKey(value ?? "");
  return /\bINDEPENDENT\s+EXPENDITURES?\b/.test(normalized);
}

function directionFromText(value: string): DirectionMatch | null {
  const normalized = normalizeTextKey(value);
  if (!normalized) {
    return null;
  }

  const directionPattern = /\b(SUPPORTING|SUPPORTS|SUPPORTED|SUPPORT|OPPOSING|OPPOSES|OPPOSED|OPPOSE|AGAINST)\b/g;
  const matches = [...normalized.matchAll(directionPattern)];
  if (matches.length !== 1) {
    return null;
  }

  const [match] = matches;
  const token = match?.[1];
  const index = match?.index;
  if (!token || index === undefined) {
    return null;
  }
  const supportOppose: DistrictOfColumbiaSupportOppose = token.startsWith("SUPPORT") ? "support" : "oppose";
  const targetText = normalized
    .slice(index + token.length)
    .replace(/^(OF|TO|FOR)\s+/, "")
    .trim();
  return targetText ? { supportOppose, targetText } : null;
}

function hasMultiCandidateCue(targetText: string): boolean {
  return /\b(AND|ALSO|MULTIPLE|VARIOUS)\b|[;/+]/.test(targetText);
}

function candidateKeysForMatch(candidateName: string): Set<string> {
  const keys = normalizeDistrictOfColumbiaCandidateNameKeys(candidateName);
  const firstKey = [...keys][0];
  const parts = firstKey?.split(" ").filter(Boolean) ?? [];
  if (parts.length >= 2) {
    keys.add(`${parts[parts.length - 1]} ${parts[0]}`);
  }
  return keys;
}

function targetMatchesCandidate(input: { targetText: string; candidateNameKeys: ReadonlySet<string> }): boolean {
  const targetText = ` ${normalizeTextKey(input.targetText)} `;
  for (const key of input.candidateNameKeys) {
    const normalizedKey = normalizeTextKey(key);
    if (normalizedKey && targetText.includes(` ${normalizedKey} `)) {
      return true;
    }
  }
  return false;
}

function parseStrictIndependentExpenditureTarget(input: {
  explanation: string | undefined;
  candidateNameKeys: ReadonlySet<string>;
}): DistrictOfColumbiaSupportOppose | null {
  const direction = directionFromText(input.explanation ?? "");
  if (!direction || hasMultiCandidateCue(direction.targetText)) {
    return null;
  }
  return targetMatchesCandidate({ targetText: direction.targetText, candidateNameKeys: input.candidateNameKeys })
    ? direction.supportOppose
    : null;
}

function groupKey(input: { committeeKey: string; supportOppose: DistrictOfColumbiaSupportOppose }): string {
  return `${input.committeeKey}\u0000${input.supportOppose}`;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): DistrictOfColumbiaOutsideSpendingGroup[] {
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

export function aggregateDistrictOfColumbiaOutsideSpending(
  input: DistrictOfColumbiaOutsideSpendingAggregationInput
): DistrictOfColumbiaOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const candidateNameKeys = candidateKeysForMatch(input.candidateName);
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
    const supportOppose = parseStrictIndependentExpenditureTarget({
      explanation: record.furtherExplanation,
      candidateNameKeys,
    });
    if (!supportOppose) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const committeeName = record.committeeName?.trim() ?? "";
    const committeeKey = normalizeCommitteeKey(record.committeeKey ?? committeeName);
    const amountCents = amountToCents(record.amount);
    if (
      !committeeName ||
      !committeeKey ||
      amountCents === null ||
      amountCents <= 0 ||
      !isIndependentExpenditurePurpose(record.purpose) ||
      !isCycleYear({ rawDate: record.date, electionYear })
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
      continue;
    }
    groups.set(key, {
      committeeKey,
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
