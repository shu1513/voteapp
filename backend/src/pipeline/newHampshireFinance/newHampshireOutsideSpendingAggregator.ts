import type { NewHampshireIndependentExpenditureRow } from "./newHampshireCfsClient.js";
import { selectCurrentNewHampshireIndependentExpenditureReportVersions } from "./newHampshirePhaseZero.js";

export type NewHampshireSupportOppose = "support" | "oppose";

export type NewHampshireOutsideSpendingGroup = {
  filerEntityId: number;
  filerName: string;
  supportOppose: NewHampshireSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type NewHampshireOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: NewHampshireOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type NewHampshireOutsideSpendingAggregationInput = {
  candidateAliases: readonly string[];
  electionYear: number;
  expenditureRows: readonly NewHampshireIndependentExpenditureRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type NewHampshireOutsideSpendingAggregationResult = {
  summary: NewHampshireOutsideSpendingSummary | null;
  sourceRowCount: number;
  currentVersionRowCount: number;
  supersededRowCount: number;
  matchedTargetRowCount: number;
  includedRowCount: number;
  blankTargetRowCount: number;
  blankStanceRowCount: number;
  nonPositiveRowCount: number;
};

type GroupAccumulator = {
  filerEntityId: number;
  filerName: string;
  supportOppose: NewHampshireSupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2016 || value > 2100) {
    throw new Error(`Invalid New Hampshire outside spending election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Hampshire outside spending ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeNamePart(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeNewHampshireCandidateAlias(value: string): string {
  const commaIndex = value.indexOf(",");
  if (commaIndex > 0) {
    const lastName = normalizeNamePart(value.slice(0, commaIndex));
    const givenNames = normalizeNamePart(value.slice(commaIndex + 1));
    if (lastName && givenNames) return `${givenNames} ${lastName}`;
  }
  return normalizeNamePart(value);
}

function normalizeCandidateAliases(values: readonly string[]): ReadonlySet<string> {
  const aliases = new Set(values.map(normalizeNewHampshireCandidateAlias).filter(Boolean));
  if (aliases.size === 0) {
    throw new Error("New Hampshire outside spending candidateAliases must contain a name");
  }
  return aliases;
}

function amountToCents(amount: number, transactionId: number): number {
  const cents = Math.round(amount * 100);
  if (!Number.isFinite(amount) || !Number.isSafeInteger(cents)) {
    throw new Error(`Invalid New Hampshire IE amount for transaction ${transactionId}: ${amount}`);
  }
  return cents;
}

function validateRows(input: {
  rows: readonly NewHampshireIndependentExpenditureRow[];
  expectedCycleName: string;
}): void {
  const identities = new Set<string>();
  for (const row of input.rows) {
    if (row.transactionTypeCode !== "TIE" || row.transactionSubTypeCode !== "TIE") {
      throw new Error(`New Hampshire IE search returned non-IE transaction ${row.transactionId}`);
    }
    if (row.electionCycle !== input.expectedCycleName) {
      throw new Error(
        `New Hampshire IE search returned cycle ${JSON.stringify(row.electionCycle)}; ` +
          `expected ${input.expectedCycleName}`
      );
    }
    if ((row.filerReportId === null) !== (row.filerReportVersionId === null)) {
      throw new Error(`New Hampshire IE transaction ${row.transactionId} has partial report identity`);
    }
    if (row.stance !== null && row.stance !== "Support" && row.stance !== "Oppose") {
      throw new Error(`New Hampshire IE search returned unknown stance ${JSON.stringify(row.stance)}`);
    }
    amountToCents(row.transactionAmount, row.transactionId);

    const identity = `${row.transactionId}:${row.transactionVersionId}:${row.guid}`;
    if (identities.has(identity)) {
      throw new Error(`New Hampshire IE search returned duplicate identity ${identity}`);
    }
    identities.add(identity);
  }
}

function groupKey(filerEntityId: number, supportOppose: NewHampshireSupportOppose): string {
  return `${filerEntityId}\u0000${supportOppose}`;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): NewHampshireOutsideSpendingGroup[] {
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.filerName.localeCompare(right.filerName) ||
        left.filerEntityId - right.filerEntityId
    )
    .slice(0, input.maxGroups)
    .map((group) => ({
      filerEntityId: group.filerEntityId,
      filerName: group.filerName,
      supportOppose: group.supportOppose,
      amount: group.amountCents / 100,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateNewHampshireOutsideSpending(
  input: NewHampshireOutsideSpendingAggregationInput
): NewHampshireOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const candidateAliases = normalizeCandidateAliases(input.candidateAliases);
  const expectedCycleName = `${electionYear} Election Cycle`;
  const sourceUrl = input.sourceUrl ?? null;
  validateRows({ rows: input.expenditureRows, expectedCycleName });

  const currentRows = selectCurrentNewHampshireIndependentExpenditureReportVersions(
    input.expenditureRows
  );
  const groups = new Map<string, GroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let matchedTargetRowCount = 0;
  let includedRowCount = 0;
  let blankTargetRowCount = 0;
  let blankStanceRowCount = 0;
  let nonPositiveRowCount = 0;

  for (const row of currentRows) {
    if (row.candidateMeasure === null) blankTargetRowCount += 1;
    if (row.stance === null) blankStanceRowCount += 1;
    if (row.candidateMeasure === null) continue;
    if (!candidateAliases.has(normalizeNewHampshireCandidateAlias(row.candidateMeasure))) {
      continue;
    }
    matchedTargetRowCount += 1;
    if (row.stance === null) continue;

    const amountCents = amountToCents(row.transactionAmount, row.transactionId);
    if (amountCents <= 0) {
      nonPositiveRowCount += 1;
      continue;
    }

    const supportOppose: NewHampshireSupportOppose =
      row.stance === "Support" ? "support" : "oppose";
    includedRowCount += 1;
    if (supportOppose === "support") supportTotalCents += amountCents;
    else opposeTotalCents += amountCents;

    const key = groupKey(row.filerEntityId, supportOppose);
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    groups.set(key, {
      filerEntityId: row.filerEntityId,
      filerName: row.filerName,
      supportOppose,
      amountCents,
    });
  }

  const grouped = toGroups({ groups: groups.values(), maxGroups, sourceUrl });
  return {
    summary:
      grouped.length > 0
        ? {
            supportTotal: supportTotalCents / 100,
            opposeTotal: opposeTotalCents / 100,
            groups: grouped,
            sourceUrl,
          }
        : null,
    sourceRowCount: input.expenditureRows.length,
    currentVersionRowCount: currentRows.length,
    supersededRowCount: input.expenditureRows.length - currentRows.length,
    matchedTargetRowCount,
    includedRowCount,
    blankTargetRowCount,
    blankStanceRowCount,
    nonPositiveRowCount,
  };
}
