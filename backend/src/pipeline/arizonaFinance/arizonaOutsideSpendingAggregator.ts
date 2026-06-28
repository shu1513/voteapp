import type {
  ArizonaSpotlightIndependentExpenditure,
  ArizonaSpotlightSupportOppose,
} from "./arizonaSpotlightClient.js";

export type ArizonaSupportOppose = "support" | "oppose";

export type ArizonaOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: ArizonaSupportOppose;
  amount: number;
  expenditureCount: number;
  sourceUrl: string | null;
};

export type ArizonaOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: ArizonaOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type ArizonaOutsideSpendingAggregationInput = {
  electionYear: number;
  independentExpenditures: readonly ArizonaSpotlightIndependentExpenditure[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type ArizonaOutsideSpendingAggregationResult = {
  summary: ArizonaOutsideSpendingSummary | null;
  matchedIndependentExpenditureCount: number;
  includedIndependentExpenditureCount: number;
  skippedIndependentExpenditureCount: number;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: ArizonaSupportOppose;
  amountCents: number;
  expenditureCount: number;
  sourceUrl: string | null;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2002 || value > 2100) {
    throw new Error(`Invalid Arizona outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Arizona outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeId(value: string): string {
  return value.trim().toUpperCase();
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

function parseDateYear(value: string | undefined): number | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  return null;
}

function isCycleYear(input: { expenditure: ArizonaSpotlightIndependentExpenditure; electionYear: number }): boolean {
  const year = parseDateYear(input.expenditure.transactionDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

export function normalizeArizonaSupportOppose(
  value: ArizonaSpotlightSupportOppose | ArizonaSupportOppose | undefined
): ArizonaSupportOppose | null {
  if (value === "Support" || value === "support") {
    return "support";
  }
  if (value === "Oppose" || value === "oppose") {
    return "oppose";
  }
  return null;
}

function groupKey(input: { committeeId: string; supportOppose: ArizonaSupportOppose }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}`;
}

export function aggregateArizonaOutsideSpending(
  input: ArizonaOutsideSpendingAggregationInput
): ArizonaOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const groups = new Map<string, GroupAccumulator>();
  let matchedIndependentExpenditureCount = 0;
  let includedIndependentExpenditureCount = 0;
  let skippedIndependentExpenditureCount = 0;
  let supportTotalCents = 0;
  let opposeTotalCents = 0;

  for (const expenditure of input.independentExpenditures) {
    matchedIndependentExpenditureCount += 1;
    const supportOppose = normalizeArizonaSupportOppose(expenditure.supportOppose);
    const amountCents = amountToCents(expenditure.amount);
    const committeeId = expenditure.committeeId.trim();
    const committeeName = expenditure.committeeName.trim().replace(/\s+/g, " ");
    if (
      !supportOppose ||
      amountCents === null ||
      amountCents <= 0 ||
      !committeeId ||
      !committeeName ||
      !isCycleYear({ expenditure, electionYear })
    ) {
      skippedIndependentExpenditureCount += 1;
      continue;
    }

    includedIndependentExpenditureCount += 1;
    if (supportOppose === "support") {
      supportTotalCents += amountCents;
    } else {
      opposeTotalCents += amountCents;
    }

    const key = groupKey({ committeeId, supportOppose });
    const existing = groups.get(key);
    if (!existing) {
      groups.set(key, {
        committeeId,
        committeeName,
        supportOppose,
        amountCents,
        expenditureCount: 1,
        sourceUrl: expenditure.sourceUrl ?? input.sourceUrl ?? null,
      });
      continue;
    }
    existing.amountCents += amountCents;
    existing.expenditureCount += 1;
    existing.sourceUrl ??= expenditure.sourceUrl ?? input.sourceUrl ?? null;
  }

  if (includedIndependentExpenditureCount === 0) {
    return {
      summary: null,
      matchedIndependentExpenditureCount,
      includedIndependentExpenditureCount,
      skippedIndependentExpenditureCount,
    };
  }

  return {
    summary: {
      supportTotal: centsToDollars(supportTotalCents),
      opposeTotal: centsToDollars(opposeTotalCents),
      groups: [...groups.values()]
        .sort((left, right) => right.amountCents - left.amountCents || left.committeeName.localeCompare(right.committeeName))
        .slice(0, maxGroups)
        .map((group) => ({
          committeeId: group.committeeId,
          committeeName: group.committeeName,
          supportOppose: group.supportOppose,
          amount: centsToDollars(group.amountCents),
          expenditureCount: group.expenditureCount,
          sourceUrl: group.sourceUrl,
        })),
      sourceUrl: input.sourceUrl ?? null,
    },
    matchedIndependentExpenditureCount,
    includedIndependentExpenditureCount,
    skippedIndependentExpenditureCount,
  };
}
