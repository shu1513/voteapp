import type { AlaskaApocIndependentExpenditureRow } from "./alaskaApocClient.js";
import { parseAlaskaApocDateYear } from "./alaskaApocClient.js";
import { normalizeAlaskaCandidateNameKeys } from "./alaskaCandidateCommitteeResolver.js";

export type AlaskaSupportOppose = "support" | "oppose";

export type AlaskaOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: AlaskaSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type AlaskaOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: AlaskaOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type AlaskaOutsideSpendingAggregationInput = {
  candidateName: string;
  electionYear: number;
  expenditureRows: readonly AlaskaApocIndependentExpenditureRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type AlaskaOutsideSpendingAggregationResult = {
  summary: AlaskaOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: AlaskaSupportOppose;
  amountCents: number;
  sourceUrl: string | null;
};

const DEFAULT_MAX_GROUPS = 50;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Alaska outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Alaska outside spending aggregation ${fieldName}: ${value}`);
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

function rowYear(row: AlaskaApocIndependentExpenditureRow): number | null {
  return row.reportYear ?? parseAlaskaApocDateYear(row.date);
}

function isCycleYear(input: { row: AlaskaApocIndependentExpenditureRow; electionYear: number }): boolean {
  const year = rowYear(input.row);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isFiledStatus(status: string): boolean {
  const key = normalizeTextKey(status);
  return !/\b(REJECTED|VOID|VOIDED|DELETED|WITHDRAWN)\b/.test(key);
}

// IE rows mention candidates in free text, so keys must appear contiguously
// (an ordered-token subsequence would false-match across unrelated words in a
// long description). Fields are matched separately so a key cannot match
// across a field seam. Keys come from the resolver's expansion - first+last
// around middles, quoted call names, one-sided nicknames - so an IE mention
// of "Louise Stutes" still matches the VoteApp name "Louise B. Stutes".
function rowMentionsCandidate(input: {
  row: AlaskaApocIndependentExpenditureRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const fields = [input.row.candidateProposition, input.row.recipient, input.row.description].map(
    (field) => ` ${normalizeTextKey(field)} `
  );
  for (const key of input.candidateNameKeys) {
    if (key.length === 0) {
      continue;
    }
    const padded = ` ${key} `;
    if (fields.some((field) => field.includes(padded))) {
      return true;
    }
  }
  return false;
}

export function supportOpposeFromAlaskaApocPosition(position: string): AlaskaSupportOppose | null {
  const key = normalizeTextKey(position);
  if (/\b(SUPPORT|SUPPORTS|SUPPORTED|FOR|IN FAVOR)\b/.test(key)) {
    return "support";
  }
  if (/\b(OPPOSE|OPPOSES|OPPOSED|AGAINST)\b/.test(key)) {
    return "oppose";
  }
  return null;
}

function groupId(row: AlaskaApocIndependentExpenditureRow): string {
  const filerId = row.filerId.trim();
  return filerId || normalizeTextKey(row.filerName);
}

function groupKey(input: { committeeId: string; supportOppose: AlaskaSupportOppose }): string {
  return `${normalizeTextKey(input.committeeId)}\u0000${input.supportOppose}`;
}

function addGroup(
  groups: Map<string, GroupAccumulator>,
  input: {
    committeeId: string;
    committeeName: string;
    supportOppose: AlaskaSupportOppose;
    amountCents: number;
    sourceUrl: string | null;
  }
): void {
  const key = groupKey(input);
  const existing = groups.get(key);
  if (existing) {
    existing.amountCents += input.amountCents;
    existing.sourceUrl ??= input.sourceUrl;
    return;
  }
  groups.set(key, {
    committeeId: input.committeeId,
    committeeName: input.committeeName,
    supportOppose: input.supportOppose,
    amountCents: input.amountCents,
    sourceUrl: input.sourceUrl,
  });
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
}): AlaskaOutsideSpendingGroup[] {
  const includedBySide: Record<AlaskaSupportOppose, number> = {
    support: 0,
    oppose: 0,
  };
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.committeeName.localeCompare(right.committeeName)
    )
    .filter((group) => {
      if (includedBySide[group.supportOppose] >= input.maxGroups) {
        return false;
      }
      includedBySide[group.supportOppose] += 1;
      return true;
    })
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: group.sourceUrl,
    }));
}

export function aggregateAlaskaOutsideSpending(
  input: AlaskaOutsideSpendingAggregationInput
): AlaskaOutsideSpendingAggregationResult {
  const candidateName = requireNonEmpty(input.candidateName, "Alaska candidate name");
  // VoteApp side expands nicknames; IE row text always matches literally.
  const candidateNameKeys = normalizeAlaskaCandidateNameKeys(candidateName, { expandNicknames: true });
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const fallbackSourceUrl = input.sourceUrl ?? null;
  const groups = new Map<string, GroupAccumulator>();
  let matchedExpenditureRowCount = 0;
  let includedExpenditureRowCount = 0;
  let skippedExpenditureRowCount = 0;
  let supportTotalCents = 0;
  let opposeTotalCents = 0;

  for (const row of input.expenditureRows) {
    if (!rowMentionsCandidate({ row, candidateNameKeys })) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const supportOppose = supportOpposeFromAlaskaApocPosition(row.position);
    const amountCents = amountToCents(row.amount);
    const committeeId = groupId(row);
    const committeeName = row.filerName.trim();
    if (
      !supportOppose ||
      amountCents === null ||
      amountCents <= 0 ||
      !committeeId ||
      !committeeName ||
      !isCycleYear({ row, electionYear }) ||
      !isFiledStatus(row.status)
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
    addGroup(groups, {
      committeeId,
      committeeName,
      supportOppose,
      amountCents,
      sourceUrl: row.sourceUrl ?? fallbackSourceUrl,
    });
  }

  if (groups.size === 0) {
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
      groups: toGroups({ groups: groups.values(), maxGroups }),
      sourceUrl: fallbackSourceUrl,
    },
    matchedExpenditureRowCount,
    includedExpenditureRowCount,
    skippedExpenditureRowCount,
  };
}
