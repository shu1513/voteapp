// Outside spending for one Connecticut candidate from eCRIS SEEC Form 40
// independent-expenditure lines.
//
// Rules (each verified against the 2026 corpus and filed PDFs, 2026-09-01):
// - A line counts only when it names exactly ONE candidate on the stance side
//   and that candidate is this one. Lines naming several candidates carry a
//   single amount for all of them (the filing's "Section G Addendum"), so
//   there is no per-candidate amount to attribute.
// - The line's office set must be exactly one office and it must be the
//   candidate's office. eCRIS carries no district, so name + office is the
//   whole identity; a differently-officed namesake is rejected, not guessed.
// - Only "Expenses Paid by Committee" sections count (G on Form 40, P on the
//   PAC form). "Expenses Incurred but Not Paid" (Section I) lines reappear as
//   Section G lines once paid (Impact CT / Landscape Media $500: Section I on
//   the 01/20 24-hour report, Section G on the 01/23 report), so counting
//   both would double-count. Reimbursement itemizations are excluded too.
// - Every remaining line is a distinct expenditure. Same-amount lines with the
//   same payee and date are separate Section G entries in the filed PDF (one
//   per candidate), never a repeat of one line.
// - A line that names this candidate on BOTH sides is contradictory and is
//   skipped rather than counted twice.

import {
  connecticutCandidateNameMatches,
  normalizeConnecticutCandidateNameKeys,
} from "./connecticutCandidateCommitteeResolver.js";
import type { ConnecticutEcrisIndependentExpenditureRow } from "./connecticutEcrisIndependentExpenditureParsers.js";
import { connecticutEcrisOfficeCanonicalName } from "./connecticutFinanceEligibleOffices.js";

export type ConnecticutOutsideSupportOppose = "support" | "oppose";

export type ConnecticutOutsideSpendingGroup = {
  /** Normalized committee-name key; eCRIS exposes no committee id on this search. */
  committeeId: string;
  committeeName: string;
  supportOppose: ConnecticutOutsideSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type ConnecticutOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: ConnecticutOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type ConnecticutOutsideSpendingAggregationInput = {
  candidateName: string;
  /** App office canonical name, e.g. "State Lower Chamber Legislator". */
  officeName: string;
  electionYear: number;
  expenditureRows: readonly ConnecticutEcrisIndependentExpenditureRow[];
  sourceUrl?: string | null;
  maxGroupsPerStance?: number;
};

export type ConnecticutOutsideSpendingAggregationResult = {
  summary: ConnecticutOutsideSpendingSummary;
  sourceRowCount: number;
  /** Lines naming this candidate on a stance side, before any exclusion. */
  targetedRowCount: number;
  includedRowCount: number;
  skippedMultiCandidateRowCount: number;
  skippedOfficeMismatchRowCount: number;
  skippedConflictingStanceRowCount: number;
  skippedYearMismatchRowCount: number;
  skippedUnpaidRowCount: number;
  skippedNonPositiveRowCount: number;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: ConnecticutOutsideSupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS_PER_STANCE = 50;
const PAID_SECTION_PATTERN = /\bexpenses paid by committee\b/i;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2008 || value > 2100) {
    throw new Error(`Invalid Connecticut outside spending election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Connecticut outside spending ${fieldName}: ${value}`);
  }
  return normalized;
}

export function normalizeConnecticutOutsideCommitteeId(committeeName: string): string {
  return committeeName
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isPaidSection(formSection: string): boolean {
  return PAID_SECTION_PATTERN.test(formSection);
}

type StanceMatch = "not_targeted" | "matched" | "multi_candidate" | "office_mismatch";

function matchStanceSide(input: {
  candidates: readonly string[];
  offices: readonly string[];
  candidateName: string;
  candidateNameKeys: ReadonlySet<string>;
  officeName: string;
}): StanceMatch {
  const named = input.candidates.some((ecrisName) =>
    connecticutCandidateNameMatches({
      candidateName: input.candidateName,
      candidateNameKeys: input.candidateNameKeys,
      ecrisName,
    })
  );
  if (!named) {
    return "not_targeted";
  }
  if (input.candidates.length !== 1) {
    return "multi_candidate";
  }
  if (input.offices.length !== 1 || connecticutEcrisOfficeCanonicalName(input.offices[0]) !== input.officeName) {
    return "office_mismatch";
  }
  return "matched";
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroupsPerStance: number;
  sourceUrl: string | null;
}): ConnecticutOutsideSpendingGroup[] {
  const counts: Record<ConnecticutOutsideSupportOppose, number> = { support: 0, oppose: 0 };
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.committeeName.localeCompare(right.committeeName) ||
        left.committeeId.localeCompare(right.committeeId)
    )
    .filter((group) => {
      if (counts[group.supportOppose] >= input.maxGroupsPerStance) return false;
      counts[group.supportOppose] += 1;
      return true;
    })
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: group.amountCents / 100,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateConnecticutOutsideSpending(
  input: ConnecticutOutsideSpendingAggregationInput
): ConnecticutOutsideSpendingAggregationResult {
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroupsPerStance = normalizePositiveInteger(
    input.maxGroupsPerStance,
    DEFAULT_MAX_GROUPS_PER_STANCE,
    "maxGroupsPerStance"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const candidateNameKeys = normalizeConnecticutCandidateNameKeys(candidateName, { expandNicknames: true });
  if (candidateNameKeys.size === 0) {
    throw new Error("candidate name is required");
  }

  const groups = new Map<string, GroupAccumulator>();
  const totalsCents: Record<ConnecticutOutsideSupportOppose, number> = { support: 0, oppose: 0 };
  let targetedRowCount = 0;
  let includedRowCount = 0;
  let skippedMultiCandidateRowCount = 0;
  let skippedOfficeMismatchRowCount = 0;
  let skippedConflictingStanceRowCount = 0;
  let skippedYearMismatchRowCount = 0;
  let skippedUnpaidRowCount = 0;
  let skippedNonPositiveRowCount = 0;

  for (const row of input.expenditureRows) {
    const support = matchStanceSide({
      candidates: row.supportingCandidates,
      offices: row.supportingOffices,
      candidateName,
      candidateNameKeys,
      officeName,
    });
    const oppose = matchStanceSide({
      candidates: row.opposingCandidates,
      offices: row.opposingOffices,
      candidateName,
      candidateNameKeys,
      officeName,
    });
    if (support === "not_targeted" && oppose === "not_targeted") {
      continue;
    }
    targetedRowCount += 1;

    if (support !== "not_targeted" && oppose !== "not_targeted") {
      skippedConflictingStanceRowCount += 1;
      continue;
    }
    const stance = support === "not_targeted" ? oppose : support;
    if (stance === "multi_candidate") {
      skippedMultiCandidateRowCount += 1;
      continue;
    }
    if (stance === "office_mismatch") {
      skippedOfficeMismatchRowCount += 1;
      continue;
    }
    if (row.fileYear !== electionYear) {
      skippedYearMismatchRowCount += 1;
      continue;
    }
    if (!isPaidSection(row.formSection)) {
      skippedUnpaidRowCount += 1;
      continue;
    }
    if (row.amountCents === null || row.amountCents <= 0) {
      skippedNonPositiveRowCount += 1;
      continue;
    }

    const committeeName = row.committeeName.replace(/\s+/g, " ").trim();
    const committeeId = normalizeConnecticutOutsideCommitteeId(committeeName);
    if (!committeeId) {
      throw new Error("Connecticut independent expenditure line has no committee name");
    }
    const supportOppose: ConnecticutOutsideSupportOppose = support === "matched" ? "support" : "oppose";
    includedRowCount += 1;
    totalsCents[supportOppose] += row.amountCents;

    const key = `${committeeId} ${supportOppose}`;
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += row.amountCents;
      continue;
    }
    groups.set(key, { committeeId, committeeName, supportOppose, amountCents: row.amountCents });
  }

  return {
    summary: {
      supportTotal: totalsCents.support / 100,
      opposeTotal: totalsCents.oppose / 100,
      groups: toGroups({ groups: groups.values(), maxGroupsPerStance, sourceUrl }),
      sourceUrl,
    },
    sourceRowCount: input.expenditureRows.length,
    targetedRowCount,
    includedRowCount,
    skippedMultiCandidateRowCount,
    skippedOfficeMismatchRowCount,
    skippedConflictingStanceRowCount,
    skippedYearMismatchRowCount,
    skippedUnpaidRowCount,
    skippedNonPositiveRowCount,
  };
}
