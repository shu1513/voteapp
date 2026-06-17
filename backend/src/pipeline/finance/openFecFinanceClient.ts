import {
  DEFAULT_OPEN_FEC_PER_PAGE,
  MAX_OPEN_FEC_PER_PAGE,
  OPEN_FEC_API_BASE_URL,
  OpenFecClientError,
  type OpenFecClientOptions,
  fetchOpenFecJsonWithKeyRotation,
} from "../presidential/openFecClient.js";

export type OpenFecFinanceCandidateTotals = {
  fecCandidateId: string;
  electionYear: number;
  totalReceipts?: number;
  totalDisbursements?: number;
  cashOnHand?: number;
  debtsOwed?: number;
  individualItemizedTotal?: number;
  individualUnitemizedTotal?: number;
  otherCommitteeContributions?: number;
  transfersFromAffiliatedCommittees?: number;
  sourceUrl: string;
};

export type OpenFecFinanceCommittee = {
  committeeId: string;
  name: string;
  designation?: string;
  designationFull?: string;
  committeeType?: string;
  committeeTypeFull?: string;
  cycles: number[];
  sourceUrl: string;
};

export type OpenFecFinanceCommitteeTotals = {
  committeeId: string;
  electionYear: number;
  totalReceipts?: number;
  totalDisbursements?: number;
  cashOnHand?: number;
  debtsOwed?: number;
  sourceUrl: string;
};

export type OpenFecFinanceAggregateType = "occupation" | "employer" | "state" | "contribution_size";

export type OpenFecFinanceAggregate = {
  type: OpenFecFinanceAggregateType;
  label: string;
  amount: number;
  count?: number;
};

export type OpenFecOutsideSpendingTotals = {
  fecCandidateId: string;
  electionYear: number;
  supportTotal: number;
  opposeTotal: number;
  sourceUrl: string;
};

export type OpenFecOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  count?: number;
  sourceUrl: string;
};

export type OpenFecFinanceAggregateInput = {
  committeeId: string;
  electionYear: number;
  perPage?: number;
};

export type OpenFecOutsideSpendingGroupsInput = {
  fecCandidateId: string;
  electionYear: number;
  supportOppose?: "support" | "oppose";
  perPage?: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1970 || value > 2100) {
    throw new OpenFecClientError("invalid_request", `Invalid OpenFEC election year: ${value}`);
  }
  return value;
}

function normalizeFecCandidateId(value: string): string {
  const normalized = value.trim().toUpperCase();
  if (!/^[HPS][0-9A-Z]{8}$/.test(normalized)) {
    throw new OpenFecClientError("invalid_request", `Invalid FEC candidate ID: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeId(value: string): string {
  const normalized = value.trim().toUpperCase();
  if (!/^C\d{8}$/.test(normalized)) {
    throw new OpenFecClientError("invalid_request", `Invalid FEC committee ID: ${value}`);
  }
  return normalized;
}

function normalizePerPage(value: number | undefined): number {
  const normalized = value ?? DEFAULT_OPEN_FEC_PER_PAGE;
  if (!Number.isInteger(normalized) || normalized <= 0 || normalized > MAX_OPEN_FEC_PER_PAGE) {
    throw new OpenFecClientError(
      "invalid_request",
      `OpenFEC perPage must be an integer between 1 and ${MAX_OPEN_FEC_PER_PAGE}`
    );
  }
  return normalized;
}

function getString(row: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return undefined;
}

function getNumber(row: Record<string, unknown>, ...keys: string[]): number | undefined {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return undefined;
}

function getIntegerArray(row: Record<string, unknown>, ...keys: string[]): number[] {
  const years: number[] = [];
  const seen = new Set<number>();
  for (const key of keys) {
    const raw = row[key];
    if (!Array.isArray(raw)) {
      continue;
    }
    for (const item of raw) {
      const parsed = typeof item === "number" ? item : Number.parseInt(String(item), 10);
      if (!Number.isInteger(parsed) || seen.has(parsed)) {
        continue;
      }
      seen.add(parsed);
      years.push(parsed);
    }
  }
  return years.sort((left, right) => left - right);
}

function sourceUrl(path: string, params: Record<string, string | number | undefined>): string {
  const url = new URL(`https://www.fec.gov${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

function apiUrl(path: string, params: Record<string, string | number | undefined>): string {
  const url = new URL(`${OPEN_FEC_API_BASE_URL}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

function supportOpposeFromIndicator(value: string | undefined): "support" | "oppose" | null {
  const normalized = value?.trim().toUpperCase();
  if (normalized === "S" || normalized === "SUPPORT") {
    return "support";
  }
  if (normalized === "O" || normalized === "OPPOSE") {
    return "oppose";
  }
  return null;
}

function indicatorFromSupportOppose(value: "support" | "oppose" | undefined): string | undefined {
  if (value === "support") {
    return "S";
  }
  if (value === "oppose") {
    return "O";
  }
  return undefined;
}

function extractResults(payload: unknown): unknown[] {
  if (!isRecord(payload) || !Array.isArray(payload.results)) {
    throw new OpenFecClientError("bad_response", "OpenFEC finance response is missing results array");
  }
  return payload.results;
}

function parseCandidateTotalsRow(row: unknown, fecCandidateId: string, electionYear: number): OpenFecFinanceCandidateTotals | null {
  if (!isRecord(row)) {
    return null;
  }
  return {
    fecCandidateId,
    electionYear,
    ...(getNumber(row, "receipts", "total_receipts") !== undefined
      ? { totalReceipts: getNumber(row, "receipts", "total_receipts") }
      : {}),
    ...(getNumber(row, "disbursements", "total_disbursements") !== undefined
      ? { totalDisbursements: getNumber(row, "disbursements", "total_disbursements") }
      : {}),
    ...(getNumber(row, "cash_on_hand_end_period", "cash_on_hand") !== undefined
      ? { cashOnHand: getNumber(row, "cash_on_hand_end_period", "cash_on_hand") }
      : {}),
    ...(getNumber(row, "debts_owed_by_committee", "debts_owed") !== undefined
      ? { debtsOwed: getNumber(row, "debts_owed_by_committee", "debts_owed") }
      : {}),
    ...(getNumber(row, "individual_itemized_contributions", "individual_itemized") !== undefined
      ? { individualItemizedTotal: getNumber(row, "individual_itemized_contributions", "individual_itemized") }
      : {}),
    ...(getNumber(row, "individual_unitemized_contributions", "individual_unitemized") !== undefined
      ? { individualUnitemizedTotal: getNumber(row, "individual_unitemized_contributions", "individual_unitemized") }
      : {}),
    ...(getNumber(row, "other_political_committee_contributions", "other_committee_contributions") !== undefined
      ? {
          otherCommitteeContributions: getNumber(
            row,
            "other_political_committee_contributions",
            "other_committee_contributions"
          ),
        }
      : {}),
    ...(getNumber(row, "transfers_from_affiliated_committee", "transfers_from_affiliated_committees") !== undefined
      ? {
          transfersFromAffiliatedCommittees: getNumber(
            row,
            "transfers_from_affiliated_committee",
            "transfers_from_affiliated_committees"
          ),
        }
      : {}),
    sourceUrl: sourceUrl(`/data/candidate/${encodeURIComponent(fecCandidateId)}/`, { cycle: electionYear }),
  };
}

function parseCommitteeRow(row: unknown): OpenFecFinanceCommittee | null {
  if (!isRecord(row)) {
    return null;
  }
  const committeeId = getString(row, "committee_id", "committeeId");
  const name = getString(row, "name", "committee_name", "committeeName");
  if (!committeeId || !name || !/^C\d{8}$/i.test(committeeId)) {
    return null;
  }
  const normalizedCommitteeId = committeeId.toUpperCase();
  return {
    committeeId: normalizedCommitteeId,
    name,
    ...(getString(row, "designation") ? { designation: getString(row, "designation") } : {}),
    ...(getString(row, "designation_full", "designationFull")
      ? { designationFull: getString(row, "designation_full", "designationFull") }
      : {}),
    ...(getString(row, "committee_type", "committeeType")
      ? { committeeType: getString(row, "committee_type", "committeeType") }
      : {}),
    ...(getString(row, "committee_type_full", "committeeTypeFull")
      ? { committeeTypeFull: getString(row, "committee_type_full", "committeeTypeFull") }
      : {}),
    cycles: getIntegerArray(row, "cycles", "election_years", "electionYears"),
    sourceUrl: sourceUrl(`/data/committee/${encodeURIComponent(normalizedCommitteeId)}/`, {}),
  };
}

function parseCommitteeTotalsRow(row: unknown, committeeId: string, electionYear: number): OpenFecFinanceCommitteeTotals | null {
  if (!isRecord(row)) {
    return null;
  }
  return {
    committeeId,
    electionYear,
    ...(getNumber(row, "receipts", "total_receipts") !== undefined
      ? { totalReceipts: getNumber(row, "receipts", "total_receipts") }
      : {}),
    ...(getNumber(row, "disbursements", "total_disbursements") !== undefined
      ? { totalDisbursements: getNumber(row, "disbursements", "total_disbursements") }
      : {}),
    ...(getNumber(row, "cash_on_hand_end_period", "cash_on_hand") !== undefined
      ? { cashOnHand: getNumber(row, "cash_on_hand_end_period", "cash_on_hand") }
      : {}),
    ...(getNumber(row, "debts_owed_by_committee", "debts_owed") !== undefined
      ? { debtsOwed: getNumber(row, "debts_owed_by_committee", "debts_owed") }
      : {}),
    sourceUrl: sourceUrl(`/data/committee/${encodeURIComponent(committeeId)}/`, { cycle: electionYear }),
  };
}

function parseAggregateRow(row: unknown, type: OpenFecFinanceAggregateType): OpenFecFinanceAggregate | null {
  if (!isRecord(row)) {
    return null;
  }
  const label = getString(row, type, "label", "size", "contributor_state", "state");
  const amount = getNumber(row, "total", "amount", "contribution_receipt_amount", "total_amount");
  if (!label || amount === undefined || amount < 0) {
    return null;
  }
  const count = getNumber(row, "count", "contribution_count", "total_count");
  return {
    type,
    label,
    amount,
    ...(count !== undefined ? { count } : {}),
  };
}

function parseOutsideSpendingGroupRow(row: unknown, fecCandidateId: string, electionYear: number): OpenFecOutsideSpendingGroup | null {
  if (!isRecord(row)) {
    return null;
  }
  const committeeId = getString(row, "committee_id", "committeeId");
  const committeeName = getString(row, "committee_name", "committeeName", "name");
  const supportOppose = supportOpposeFromIndicator(getString(row, "support_oppose_indicator", "supportOppose"));
  const amount = getNumber(row, "total", "amount", "expenditure_amount", "independent_expenditure_amount");
  if (!committeeId || !committeeName || !/^C\d{8}$/i.test(committeeId) || !supportOppose || amount === undefined || amount < 0) {
    return null;
  }
  const count = getNumber(row, "count", "expenditure_count", "total_count");
  return {
    committeeId: committeeId.toUpperCase(),
    committeeName,
    supportOppose,
    amount,
    ...(count !== undefined ? { count } : {}),
    sourceUrl: sourceUrl("/data/independent-expenditures/", {
      candidate_id: fecCandidateId,
      committee_id: committeeId.toUpperCase(),
      cycle: electionYear,
      support_oppose_indicator: supportOppose === "support" ? "S" : "O",
    }),
  };
}

export function buildOpenFecCandidateTotalsUrl(fecCandidateId: string, electionYear: number): string {
  return apiUrl(`/candidate/${encodeURIComponent(normalizeFecCandidateId(fecCandidateId))}/totals/`, {
    cycle: normalizeElectionYear(electionYear),
  });
}

export function buildOpenFecCandidateCommitteesUrl(fecCandidateId: string, electionYear: number): string {
  return apiUrl(`/candidate/${encodeURIComponent(normalizeFecCandidateId(fecCandidateId))}/committees/`, {
    cycle: normalizeElectionYear(electionYear),
    per_page: MAX_OPEN_FEC_PER_PAGE,
  });
}

export function buildOpenFecCommitteeTotalsUrl(committeeId: string, electionYear: number): string {
  return apiUrl(`/committee/${encodeURIComponent(normalizeCommitteeId(committeeId))}/totals/`, {
    cycle: normalizeElectionYear(electionYear),
  });
}

export function buildOpenFecCommitteeAggregateUrl(input: OpenFecFinanceAggregateInput & { type: OpenFecFinanceAggregateType }): string {
  const endpointByType: Record<OpenFecFinanceAggregateType, string> = {
    occupation: "/schedules/schedule_a/by_occupation/",
    employer: "/schedules/schedule_a/by_employer/",
    state: "/schedules/schedule_a/by_state/",
    contribution_size: "/schedules/schedule_a/by_size/",
  };
  return apiUrl(endpointByType[input.type], {
    committee_id: normalizeCommitteeId(input.committeeId),
    cycle: normalizeElectionYear(input.electionYear),
    per_page: normalizePerPage(input.perPage),
    sort: "-total",
  });
}

export function buildOpenFecOutsideSpendingTotalsByCandidateUrl(fecCandidateId: string, electionYear: number): string {
  return apiUrl("/schedules/schedule_e/totals/by_candidate/", {
    candidate_id: normalizeFecCandidateId(fecCandidateId),
    cycle: normalizeElectionYear(electionYear),
  });
}

export function buildOpenFecOutsideSpendingGroupsByCandidateUrl(input: OpenFecOutsideSpendingGroupsInput): string {
  return apiUrl("/schedules/schedule_e/by_candidate/", {
    candidate_id: normalizeFecCandidateId(input.fecCandidateId),
    cycle: normalizeElectionYear(input.electionYear),
    support_oppose_indicator: indicatorFromSupportOppose(input.supportOppose),
    per_page: normalizePerPage(input.perPage),
    sort: "-total",
  });
}

export async function getCandidateTotals(
  fecCandidateId: string,
  electionYear: number,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceCandidateTotals | null> {
  const normalizedFecCandidateId = normalizeFecCandidateId(fecCandidateId);
  const normalizedElectionYear = normalizeElectionYear(electionYear);
  const payload = await fetchOpenFecJsonWithKeyRotation(
    buildOpenFecCandidateTotalsUrl(normalizedFecCandidateId, normalizedElectionYear),
    options
  );
  const row = extractResults(payload)[0];
  return parseCandidateTotalsRow(row, normalizedFecCandidateId, normalizedElectionYear);
}

export async function listCandidateCommittees(
  fecCandidateId: string,
  electionYear: number,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceCommittee[]> {
  const payload = await fetchOpenFecJsonWithKeyRotation(buildOpenFecCandidateCommitteesUrl(fecCandidateId, electionYear), options);
  return extractResults(payload)
    .map((row) => parseCommitteeRow(row))
    .filter((row): row is OpenFecFinanceCommittee => row !== null);
}

export async function getCommitteeTotals(
  committeeId: string,
  electionYear: number,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceCommitteeTotals | null> {
  const normalizedCommitteeId = normalizeCommitteeId(committeeId);
  const normalizedElectionYear = normalizeElectionYear(electionYear);
  const payload = await fetchOpenFecJsonWithKeyRotation(
    buildOpenFecCommitteeTotalsUrl(normalizedCommitteeId, normalizedElectionYear),
    options
  );
  const row = extractResults(payload)[0];
  return parseCommitteeTotalsRow(row, normalizedCommitteeId, normalizedElectionYear);
}

async function getCommitteeAggregates(
  input: OpenFecFinanceAggregateInput & { type: OpenFecFinanceAggregateType },
  options: OpenFecClientOptions
): Promise<OpenFecFinanceAggregate[]> {
  const payload = await fetchOpenFecJsonWithKeyRotation(buildOpenFecCommitteeAggregateUrl(input), options);
  return extractResults(payload)
    .map((row) => parseAggregateRow(row, input.type))
    .filter((row): row is OpenFecFinanceAggregate => row !== null);
}

export function getCommitteeAggregatesByOccupation(
  input: OpenFecFinanceAggregateInput,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceAggregate[]> {
  return getCommitteeAggregates({ ...input, type: "occupation" }, options);
}

export function getCommitteeAggregatesByEmployer(
  input: OpenFecFinanceAggregateInput,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceAggregate[]> {
  return getCommitteeAggregates({ ...input, type: "employer" }, options);
}

export function getCommitteeAggregatesByState(
  input: OpenFecFinanceAggregateInput,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceAggregate[]> {
  return getCommitteeAggregates({ ...input, type: "state" }, options);
}

export function getCommitteeAggregatesBySize(
  input: OpenFecFinanceAggregateInput,
  options: OpenFecClientOptions
): Promise<OpenFecFinanceAggregate[]> {
  return getCommitteeAggregates({ ...input, type: "contribution_size" }, options);
}

export async function getOutsideSpendingTotalsByCandidate(
  fecCandidateId: string,
  electionYear: number,
  options: OpenFecClientOptions
): Promise<OpenFecOutsideSpendingTotals> {
  const normalizedFecCandidateId = normalizeFecCandidateId(fecCandidateId);
  const normalizedElectionYear = normalizeElectionYear(electionYear);
  const payload = await fetchOpenFecJsonWithKeyRotation(
    buildOpenFecOutsideSpendingTotalsByCandidateUrl(normalizedFecCandidateId, normalizedElectionYear),
    options
  );

  let supportTotal = 0;
  let opposeTotal = 0;
  for (const row of extractResults(payload)) {
    if (!isRecord(row)) {
      continue;
    }
    const supportOppose = supportOpposeFromIndicator(getString(row, "support_oppose_indicator", "supportOppose"));
    const amount = getNumber(row, "total", "amount", "expenditure_amount", "independent_expenditure_amount");
    if (!supportOppose || amount === undefined || amount < 0) {
      continue;
    }
    if (supportOppose === "support") {
      supportTotal += amount;
    } else {
      opposeTotal += amount;
    }
  }

  return {
    fecCandidateId: normalizedFecCandidateId,
    electionYear: normalizedElectionYear,
    supportTotal,
    opposeTotal,
    sourceUrl: sourceUrl("/data/independent-expenditures/", {
      candidate_id: normalizedFecCandidateId,
      cycle: normalizedElectionYear,
    }),
  };
}

export async function listOutsideSpendingGroupsByCandidate(
  input: OpenFecOutsideSpendingGroupsInput,
  options: OpenFecClientOptions
): Promise<OpenFecOutsideSpendingGroup[]> {
  const normalizedFecCandidateId = normalizeFecCandidateId(input.fecCandidateId);
  const normalizedElectionYear = normalizeElectionYear(input.electionYear);
  const payload = await fetchOpenFecJsonWithKeyRotation(
    buildOpenFecOutsideSpendingGroupsByCandidateUrl({
      ...input,
      fecCandidateId: normalizedFecCandidateId,
      electionYear: normalizedElectionYear,
    }),
    options
  );
  return extractResults(payload)
    .map((row) => parseOutsideSpendingGroupRow(row, normalizedFecCandidateId, normalizedElectionYear))
    .filter((row): row is OpenFecOutsideSpendingGroup => row !== null)
    .filter((row) => !input.supportOppose || row.supportOppose === input.supportOppose);
}
