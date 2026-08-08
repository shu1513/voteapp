import {
  isGeorgiaRecognizedTransactionStatus,
  type GeorgiaEthicsHost,
  type GeorgiaIndependentExpenditureRow,
} from "./georgiaEthicsClient.js";

// Outside-spending (independent expenditure) aggregation for Georgia
// (georgia_plan.md F5/F6, decision D6). Attribution is an ID join — a
// target's filerRegistrationGuid against the candidate's PeachFile
// registration guid — never a name match.
//
// D6 allocation (release-blocking rule): Georgia discloses no per-target
// amount, so only a transaction with EXACTLY ONE target row — of any kind —
// can allocate, and it allocates its full amountApplied to that target.
// A candidate target plus a ballot target is still unallocatable. Everything
// else referencing the candidate is quarantined from the totals and reported
// as excluded DOLLARS, not just excluded counts (14% of the probed store is
// multi-target, up to 65 targets on one transaction). Also quarantined:
// single-target rows whose target is not reasonTypeCode "CAN", carries no
// recognizable stance, or whose spender identity (registration guid / name)
// is missing, and rows with a non-positive amount (the outside-group schema
// requires amount >= 0; no negative IE was observed in the probed store).
//
// The spender's group identity is its registration guid — IE rows carry no
// filerEntityId (spike bytes), and the probed store shows exactly one guid
// per spender name.

export type GeorgiaSupportOppose = "support" | "oppose";

export type GeorgiaOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: GeorgiaSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type GeorgiaOutsideSpendingAggregationInput = {
  host: GeorgiaEthicsHost;
  rows: readonly GeorgiaIndependentExpenditureRow[];
  candidateRegistrationGuid: string;
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type GeorgiaOutsideSpendingAggregationResult = {
  outsideGroups: GeorgiaOutsideSpendingGroup[];
  supportTotal: number;
  opposeTotal: number;
  storeRowCount: number;
  // Rows where at least one target references the candidate's registration.
  candidateTargetRowCount: number;
  attributedRowCount: number;
  attributedAmount: number;
  // D6 quarantine: candidate-referencing rows with more than one target.
  multiTargetRowCount: number;
  multiTargetAmount: number;
  // Single-target candidate rows failing the remaining D6 gates (non-CAN
  // reason, missing stance, missing spender identity, non-positive amount).
  malformedRowCount: number;
  malformedAmount: number;
  unrecognizedStatusRowCount: number;
  unrecognizedStatusAmount: number;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: GeorgiaSupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Georgia outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeGuid(value: string | null | undefined): string | null {
  const trimmed = value?.trim().toLowerCase();
  return trimmed ? trimmed : null;
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

function supportOpposeFromStance(stance: string | null): GeorgiaSupportOppose | null {
  // The stance STRING is authoritative — the sibling supportOppose
  // "True"/"False" field is a boolean convenience mirror, and Georgia's
  // boolean mirrors are broken upstream (D8 string-codes-only rule).
  const normalized = stance?.trim().toUpperCase();
  if (normalized === "SUPPORT") {
    return "support";
  }
  if (normalized === "OPPOSE") {
    return "oppose";
  }
  return null;
}

export function aggregateGeorgiaOutsideSpending(
  input: GeorgiaOutsideSpendingAggregationInput
): GeorgiaOutsideSpendingAggregationResult {
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const candidateRegistrationGuid = normalizeGuid(input.candidateRegistrationGuid);
  if (!candidateRegistrationGuid) {
    throw new Error("Georgia outside spending aggregation needs the candidate registration guid");
  }
  const sourceUrl = input.sourceUrl ?? null;

  const groups = new Map<string, GroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let candidateTargetRowCount = 0;
  let attributedRowCount = 0;
  let attributedCents = 0;
  let multiTargetRowCount = 0;
  let multiTargetCents = 0;
  let malformedRowCount = 0;
  let malformedCents = 0;
  let unrecognizedStatusRowCount = 0;
  let unrecognizedStatusCents = 0;

  for (const row of input.rows) {
    const referencesCandidate = row.candidateMeasures.some(
      (target) => normalizeGuid(target.filerRegistrationGuid) === candidateRegistrationGuid
    );
    if (!referencesCandidate) {
      continue;
    }
    candidateTargetRowCount += 1;
    const amountCents = amountToCents(row.amountApplied) ?? 0;

    if (!isGeorgiaRecognizedTransactionStatus(input.host, row.transactionStatusCode)) {
      unrecognizedStatusRowCount += 1;
      unrecognizedStatusCents += amountCents;
      continue;
    }
    if (row.candidateMeasures.length !== 1) {
      multiTargetRowCount += 1;
      multiTargetCents += amountCents;
      continue;
    }

    const target = row.candidateMeasures[0]!;
    const supportOppose = supportOpposeFromStance(target.stance);
    const spenderGuid = normalizeGuid(row.filerRegistrationGuid);
    const spenderName = row.filerName?.trim().replace(/\s+/g, " ") ?? "";
    if (
      target.reasonTypeCode?.trim().toUpperCase() !== "CAN" ||
      supportOppose === null ||
      spenderGuid === null ||
      spenderName === "" ||
      amountCents <= 0
    ) {
      malformedRowCount += 1;
      malformedCents += amountCents;
      continue;
    }

    attributedRowCount += 1;
    attributedCents += amountCents;
    if (supportOppose === "support") {
      supportTotalCents += amountCents;
    } else {
      opposeTotalCents += amountCents;
    }

    const key = `${spenderGuid}\u0000${supportOppose}`;
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    groups.set(key, {
      committeeId: spenderGuid,
      committeeName: spenderName,
      supportOppose,
      amountCents,
    });
  }

  const outsideGroups = [...groups.values()]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.committeeName.localeCompare(right.committeeName)
    )
    .slice(0, maxGroups)
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl,
    }));

  return {
    outsideGroups,
    supportTotal: centsToDollars(supportTotalCents),
    opposeTotal: centsToDollars(opposeTotalCents),
    storeRowCount: input.rows.length,
    candidateTargetRowCount,
    attributedRowCount,
    attributedAmount: centsToDollars(attributedCents),
    multiTargetRowCount,
    multiTargetAmount: centsToDollars(multiTargetCents),
    malformedRowCount,
    malformedAmount: centsToDollars(malformedCents),
    unrecognizedStatusRowCount,
    unrecognizedStatusAmount: centsToDollars(unrecognizedStatusCents),
  };
}
