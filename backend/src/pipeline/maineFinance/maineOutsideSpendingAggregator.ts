import { parseMaineCfisMoney, type MaineCfisExpenditureRow } from "./maineCfisArtifactReader.js";
import { normalizeMaineCandidateNameKeys } from "./maineCandidateCommitteeResolver.js";

export type MaineSupportOppose = "support" | "oppose";

export type MaineOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: MaineSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type MaineOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: MaineOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type MaineOutsideSpendingAggregationInput = {
  candidateName: string;
  electionYear: number;
  expenditureRows: readonly MaineCfisExpenditureRow[];
  candidateId?: string | null;
  officeName?: string | null;
  district?: string | null;
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type MaineOutsideSpendingAggregationResult = {
  summary: MaineOutsideSpendingSummary | null;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: MaineSupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Maine outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Maine outside spending aggregation ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

function normalizeOptionalText(value: string | null | undefined): string {
  return (value ?? "").trim();
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|TO|ELECT|COMMITTEE|FRIENDS|CITIZENS)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const amount = parseMaineCfisMoney(raw);
  if (amount === null || !Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseMaineCfisDateYear(raw: string): number | null {
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
  const year = parseMaineCfisDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function candidateNameMatches(input: { candidateName: string; candidateNameKeys: ReadonlySet<string> }): boolean {
  const rowKeys = normalizeMaineCandidateNameKeys(input.candidateName);
  if (rowKeys.size === 0) {
    return false;
  }
  for (const key of rowKeys) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function supportOpposeFromValue(value: string): MaineSupportOppose | null {
  const normalized = normalizeTextKey(value);
  if (normalized === "SUPPORT" || normalized === "SUPPORTED" || normalized === "FOR" || normalized === "IN SUPPORT") {
    return "support";
  }
  if (normalized === "OPPOSE" || normalized === "OPPOSED" || normalized === "AGAINST" || normalized === "IN OPPOSITION") {
    return "oppose";
  }
  return null;
}

function isIndependentExpenditure(row: MaineCfisExpenditureRow): boolean {
  const ieReport = normalizeTextKey(row["IE Report"]);
  return ieReport === "Y" || ieReport === "YES" || ieReport === "TRUE";
}

function isCandidateCommitteeType(value: string): boolean {
  const committeeType = normalizeTextKey(value);
  return (
    committeeType === "CANDIDATE" ||
    committeeType === "CANDIDATE COMMITTEE" ||
    committeeType === "PUBLIC FINANCING" ||
    committeeType === "PUBLIC FINANCING COMMITTEE"
  );
}

function officeMatches(input: { rowOfficeName: string; officeName?: string | null }): boolean {
  const officeName = normalizeOptionalText(input.officeName);
  if (!officeName) {
    return true;
  }
  return normalizeTextKey(input.rowOfficeName) === normalizeTextKey(officeName);
}

function districtMatches(input: { rowDistrict: string; district?: string | null }): boolean {
  const district = normalizeOptionalText(input.district);
  if (!district) {
    return true;
  }
  return normalizeTextKey(input.rowDistrict) === normalizeTextKey(district);
}

function candidateIdMatches(input: { rowCandidateId: string; candidateId?: string | null }): boolean {
  const candidateId = normalizeOptionalText(input.candidateId);
  if (!candidateId) {
    return true;
  }
  return normalizeId(input.rowCandidateId) === normalizeId(candidateId);
}

function targetMatchesCandidate(input: {
  row: MaineCfisExpenditureRow;
  candidateNameKeys: ReadonlySet<string>;
  candidateId?: string | null;
  officeName?: string | null;
  district?: string | null;
}): boolean {
  return (
    candidateIdMatches({ rowCandidateId: input.row["Candidate ID"], candidateId: input.candidateId }) &&
    candidateNameMatches({ candidateName: input.row.Candidate, candidateNameKeys: input.candidateNameKeys }) &&
    officeMatches({ rowOfficeName: input.row["Candidate Office"], officeName: input.officeName }) &&
    districtMatches({ rowDistrict: input.row["Candidate District"], district: input.district })
  );
}

function groupKey(input: { committeeId: string; supportOppose: MaineSupportOppose }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): MaineOutsideSpendingGroup[] {
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

export function aggregateMaineOutsideSpending(
  input: MaineOutsideSpendingAggregationInput
): MaineOutsideSpendingAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const candidateNameKeys = normalizeMaineCandidateNameKeys(input.candidateName);
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
    if (
      !targetMatchesCandidate({
        row,
        candidateNameKeys,
        candidateId: input.candidateId,
        officeName: input.officeName,
        district: input.district,
      })
    ) {
      continue;
    }
    matchedExpenditureRowCount += 1;

    const committeeId = normalizeId(row.OrgID);
    const committeeName = row["Committee Name"].trim();
    const supportOppose = supportOpposeFromValue(row["Support/Oppose Candidate"]);
    const amountCents = parseAmountCents(row["Expenditure Amount"]);
    if (
      !committeeId ||
      !committeeName ||
      !supportOppose ||
      amountCents === null ||
      amountCents <= 0 ||
      !isIndependentExpenditure(row) ||
      isCandidateCommitteeType(row["Committee Type"]) ||
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
