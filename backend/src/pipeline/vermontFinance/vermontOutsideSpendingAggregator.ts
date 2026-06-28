import type { VermontExpenditureRow } from "./vermontCampaignFinanceClient.js";
import { normalizeVermontCandidateNameKeys } from "./vermontCandidateCommitteeResolver.js";

export type VermontSupportOppose = "support" | "oppose";

export type VermontOutsideSpendingGroup = {
  filerRegistrationGuid: string;
  filerName: string;
  supportOppose: VermontSupportOppose;
  supportMechanism: "vt_pac_contribution_to_registrant";
  amount: number;
  expenditureCount: number;
  entityId: number | null;
  sourceUrl: string | null;
};

export type VermontOutsideSpendingSummary = {
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  sourceUrl: string | null;
  groups: VermontOutsideSpendingGroup[];
};

export type VermontOutsideSpendingAggregationInput = {
  candidateName: string;
  candidateEntityId?: number | null;
  electionYear: number;
  expenditureRows: readonly VermontExpenditureRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type VermontOutsideSpendingAggregationResult = {
  summary: VermontOutsideSpendingSummary;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  filerRegistrationGuid: string;
  filerName: string;
  supportOppose: VermontSupportOppose;
  supportMechanism: "vt_pac_contribution_to_registrant";
  amountCents: number;
  expenditureCount: number;
  entityId: number | null;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Vermont outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Vermont outside spending aggregation ${fieldName}: ${value}`);
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

function isPacFiler(row: VermontExpenditureRow): boolean {
  const filerType = normalizeTextKey(`${row.filerTypeCode ?? ""} ${row.filerTypeDescription ?? ""}`);
  return /\b(PAC|POLITICAL ACTION COMMITTEE)\b/.test(filerType);
}

function isCandidatePayee(row: VermontExpenditureRow): boolean {
  return /\bCANDIDATE\b/.test(normalizeTextKey(row.payeeType));
}

function isContributionToRegistrantCategory(row: VermontExpenditureRow): boolean {
  const category = normalizeTextKey(
    `${row.transactionCategoryCode ?? ""} ${row.transactionCategoryDescription ?? ""} ${row.expenditurePurpose ?? ""} ${
      row.description ?? ""
    }`
  );
  if (!category || /\b(REFUND|RETURNED|REIMBURSEMENT|FEE|FEES)\b/.test(category)) {
    return false;
  }
  return /\b(CONTRIBUTION|DONATION|DONATE|SUPPORT)\b/.test(category);
}

function rowPayeeCandidateNames(row: VermontExpenditureRow): string[] {
  const fullName = [row.candidateFirstName, row.candidateMiddleName, row.candidateLastName]
    .map((part) => part?.trim())
    .filter((part): part is string => Boolean(part))
    .join(" ");
  return [row.sourceName, row.candidateMentioned, fullName].filter(
    (name): name is string => typeof name === "string" && name.trim().length > 0
  );
}

function rowMatchesTargetCandidate(input: {
  row: VermontExpenditureRow;
  candidateNameKeys: ReadonlySet<string>;
  candidateEntityId: number | null;
}): boolean {
  if (input.candidateEntityId !== null && input.row.entityId === input.candidateEntityId) {
    return true;
  }
  for (const name of rowPayeeCandidateNames(input.row)) {
    for (const key of normalizeVermontCandidateNameKeys(name)) {
      if (input.candidateNameKeys.has(key)) {
        return true;
      }
    }
  }
  return false;
}

function groupKey(input: { filerRegistrationGuid: string; supportOppose: VermontSupportOppose }): string {
  return `${input.filerRegistrationGuid.trim()}\u0000${input.supportOppose}`;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  sourceUrl: string | null;
}): VermontOutsideSpendingGroup[] {
  return [...input.groups]
    .sort((left, right) => right.amountCents - left.amountCents || left.filerName.localeCompare(right.filerName))
    .map((group) => ({
      filerRegistrationGuid: group.filerRegistrationGuid,
      filerName: group.filerName,
      supportOppose: group.supportOppose,
      supportMechanism: group.supportMechanism,
      amount: centsToDollars(group.amountCents),
      expenditureCount: group.expenditureCount,
      entityId: group.entityId,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateVermontOutsideSpending(
  input: VermontOutsideSpendingAggregationInput
): VermontOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeVermontCandidateNameKeys(input.candidateName);
  const candidateEntityId =
    input.candidateEntityId === undefined || input.candidateEntityId === null ? null : input.candidateEntityId;
  if (candidateEntityId !== null && (!Number.isInteger(candidateEntityId) || candidateEntityId <= 0)) {
    throw new Error(`Invalid Vermont outside spending aggregation candidateEntityId: ${input.candidateEntityId}`);
  }
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const sourceUrl = input.sourceUrl ?? null;
  const groups = new Map<string, GroupAccumulator>();
  let matchedExpenditureRowCount = 0;
  let includedExpenditureRowCount = 0;
  let skippedExpenditureRowCount = 0;

  for (const row of input.expenditureRows) {
    if (!rowMatchesTargetCandidate({ row, candidateNameKeys, candidateEntityId })) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const amountCents = amountToCents(row.transactionAmount);
    const filerRegistrationGuid = row.filerRegistrationGuid.trim();
    const filerName = row.filerName.trim();
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !filerRegistrationGuid ||
      !filerName ||
      row.electionYear !== electionYear ||
      !isPacFiler(row) ||
      !isCandidatePayee(row) ||
      !isContributionToRegistrantCategory(row)
    ) {
      skippedExpenditureRowCount += 1;
      continue;
    }

    includedExpenditureRowCount += 1;
    const rowSupportOppose: VermontSupportOppose = "support";
    const key = groupKey({ filerRegistrationGuid, supportOppose: rowSupportOppose });
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      existing.expenditureCount += 1;
      existing.entityId = existing.entityId ?? row.entityId;
      continue;
    }
    groups.set(key, {
      filerRegistrationGuid,
      filerName,
      supportOppose: rowSupportOppose,
      supportMechanism: "vt_pac_contribution_to_registrant",
      amountCents,
      expenditureCount: 1,
      entityId: row.entityId,
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
