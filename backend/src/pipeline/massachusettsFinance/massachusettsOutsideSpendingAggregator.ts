import type { MassachusettsOcpfReportDetail, MassachusettsOcpfExpenditureItem } from "./massachusettsOcpfClient.js";

export type MassachusettsSupportOppose = "support" | "oppose";

export type MassachusettsOutsideSpendingGroup = {
  iepacCpfId: string;
  iepacName: string;
  supportOppose: MassachusettsSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type MassachusettsOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: MassachusettsOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type MassachusettsOutsideSpendingAggregationInput = {
  candidateCpfId: string;
  electionYear: number;
  reportDetails: readonly MassachusettsOcpfReportDetail[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type MassachusettsOutsideSpendingAggregationResult = {
  summary: MassachusettsOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  iepacCpfId: string;
  iepacName: string;
  supportOppose: MassachusettsSupportOppose;
  amountCents: number;
  sourceUrl: string | null;
};

const DEFAULT_MAX_GROUPS = 50;
const INDEPENDENT_EXPENDITURE_RECORD_TYPE = "INDEPENDENT EXPENDITURE";

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Massachusetts outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Massachusetts outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCpfId(value: string | undefined): string {
  return (value ?? "").trim();
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

function parseMassachusettsOcpfDateYear(raw: string | undefined): number | null {
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

function isElectionYearExpenditure(input: { item: MassachusettsOcpfExpenditureItem; electionYear: number }): boolean {
  const year = parseMassachusettsOcpfDateYear(input.item.date);
  return year === input.electionYear;
}

function isCandidateCpfExpenditure(input: { item: MassachusettsOcpfExpenditureItem; candidateCpfId: string }): boolean {
  return normalizeCpfId(input.item.relatedCpfId) === input.candidateCpfId;
}

function groupKey(input: { iepacCpfId: string; supportOppose: MassachusettsSupportOppose }): string {
  return `${input.iepacCpfId}\u0000${input.supportOppose}`;
}

function recordTypeKey(item: MassachusettsOcpfExpenditureItem): string {
  return normalizeTextKey(item.recordTypeDescription);
}

export function supportOpposeFromMassachusettsOcpfIsSupported(
  value: boolean | null
): MassachusettsSupportOppose | null {
  if (value === true) {
    return "support";
  }
  if (value === false) {
    return "oppose";
  }
  return null;
}

export function isMassachusettsIndependentExpenditure(item: MassachusettsOcpfExpenditureItem): boolean {
  return recordTypeKey(item) === INDEPENDENT_EXPENDITURE_RECORD_TYPE;
}

function addGroup(
  groups: Map<string, GroupAccumulator>,
  input: {
    iepacCpfId: string;
    iepacName: string;
    supportOppose: MassachusettsSupportOppose;
    amountCents: number;
    sourceUrl: string | null;
  }
): void {
  const key = groupKey(input);
  const existing = groups.get(key);
  if (!existing) {
    groups.set(key, {
      iepacCpfId: input.iepacCpfId,
      iepacName: input.iepacName,
      supportOppose: input.supportOppose,
      amountCents: input.amountCents,
      sourceUrl: input.sourceUrl,
    });
    return;
  }

  existing.amountCents += input.amountCents;
  if (!existing.sourceUrl && input.sourceUrl) {
    existing.sourceUrl = input.sourceUrl;
  }
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
}): MassachusettsOutsideSpendingGroup[] {
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.iepacName.localeCompare(right.iepacName)
    )
    .slice(0, input.maxGroups)
    .map((group) => ({
      iepacCpfId: group.iepacCpfId,
      iepacName: group.iepacName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: group.sourceUrl,
    }));
}

export function aggregateMassachusettsOutsideSpending(
  input: MassachusettsOutsideSpendingAggregationInput
): MassachusettsOutsideSpendingAggregationResult {
  const candidateCpfId = requireNonEmpty(input.candidateCpfId, "Massachusetts candidate CPF ID");
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const fallbackSourceUrl = input.sourceUrl ?? null;
  const groups = new Map<string, GroupAccumulator>();
  let matchedExpenditureRowCount = 0;
  let includedExpenditureRowCount = 0;
  let skippedExpenditureRowCount = 0;
  let supportTotalCents = 0;
  let opposeTotalCents = 0;

  for (const report of input.reportDetails) {
    const iepacCpfId = normalizeCpfId(report.cpfId);
    const iepacName = report.committeeName?.trim().replace(/\s+/g, " ") ?? "";
    const sourceUrl = report.sourceUrl ?? fallbackSourceUrl;

    for (const item of report.expenditures) {
      if (!isCandidateCpfExpenditure({ item, candidateCpfId })) {
        continue;
      }
      matchedExpenditureRowCount += 1;

      const supportOppose = supportOpposeFromMassachusettsOcpfIsSupported(item.isSupported);
      const amountCents = amountToCents(item.amount);
      if (
        !iepacCpfId ||
        !iepacName ||
        supportOppose === null ||
        amountCents === null ||
        amountCents <= 0 ||
        !isElectionYearExpenditure({ item, electionYear }) ||
        !isMassachusettsIndependentExpenditure(item)
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
        iepacCpfId,
        iepacName,
        supportOppose,
        amountCents,
        sourceUrl,
      });
    }
  }

  const groupedRows = toGroups({ groups: groups.values(), maxGroups });
  return {
    summary:
      groupedRows.length > 0
        ? {
            supportTotal: centsToDollars(supportTotalCents),
            opposeTotal: centsToDollars(opposeTotalCents),
            groups: groupedRows,
            sourceUrl: fallbackSourceUrl,
          }
        : null,
    matchedExpenditureRowCount,
    includedExpenditureRowCount,
    skippedExpenditureRowCount,
  };
}
