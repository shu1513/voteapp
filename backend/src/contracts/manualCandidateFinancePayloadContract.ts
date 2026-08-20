import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export const MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION = "manual_candidate_finance.v1" as const;

export const MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE =
  "Covers only campaign-finance reports filed with the Mississippi Secretary of State; county and municipal clerk filings are not included." as const;

const MISSISSIPPI_SOS_WORKFLOW_ID = "g729911d7-f399-46d6-a1ca-f15c1294f82d";
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type ManualCandidateFinanceReportedTotals = {
  contributions_this_period: number | null;
  contributions_calendar_ytd: number | null;
  disbursements_this_period: number | null;
  disbursements_calendar_ytd: number | null;
  cash_on_hand: number | null;
  debts_owed: number | null;
};

export type ManualCandidateFinanceItemizedReceipt = {
  received_date: string;
  amount: number;
  occupation: string | null;
  employer: string | null;
};

type ManualCandidateFinanceCommonPayload = {
  schema_version: typeof MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION;
  state: "MS";
  filing_id: string;
  amends_filing_id: string | null;
  report_date: string;
  source_url: string;
  coverage_note: typeof MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE;
  researched_at: string;
  reported_totals: ManualCandidateFinanceReportedTotals;
  itemized_receipts: ManualCandidateFinanceItemizedReceipt[];
};

export type ManualCandidateFinanceCandidateReportPayload = ManualCandidateFinanceCommonPayload & {
  filing_type: "candidate_report";
  candidate_id: string;
  election_id: string;
  candidate_name: string;
};

export type ManualCandidateFinanceOutsideSpender = {
  source_entity_id: string | null;
  name: string;
};

export type ManualCandidateFinanceCandidateEdge = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  support_oppose: "support" | "oppose";
  amount: number | null;
};

export type ManualCandidateFinanceIndependentExpenditurePayload = ManualCandidateFinanceCommonPayload & {
  filing_type: "independent_expenditure";
  outside_spender: ManualCandidateFinanceOutsideSpender;
  candidate_edges: ManualCandidateFinanceCandidateEdge[];
};

export type ManualCandidateFinancePayload =
  | ManualCandidateFinanceCandidateReportPayload
  | ManualCandidateFinanceIndependentExpenditurePayload;

type ParseResult<T> = { ok: true; value: T } | { ok: false; reason: string };

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseNonEmptyString(value: unknown, path: string): ParseResult<string> {
  if (typeof value !== "string" || value.trim().length === 0) {
    return { ok: false, reason: `${path} must be a non-empty string` };
  }
  return { ok: true, value: value.trim() };
}

function parseNullableString(value: unknown, path: string): ParseResult<string | null> {
  if (value === null) {
    return { ok: true, value: null };
  }
  return parseNonEmptyString(value, path);
}

function parseUuid(value: unknown, path: string): ParseResult<string> {
  const parsed = parseNonEmptyString(value, path);
  if (!parsed.ok) {
    return parsed;
  }
  if (!UUID_PATTERN.test(parsed.value)) {
    return { ok: false, reason: `${path} must be a UUID` };
  }
  return { ok: true, value: parsed.value.toLowerCase() };
}

function isIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10));
  const parsed = new Date(Date.UTC(year!, month! - 1, day));
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() === month! - 1 &&
    parsed.getUTCDate() === day
  );
}

function parseIsoDate(value: unknown, path: string): ParseResult<string> {
  if (typeof value !== "string" || !isIsoDate(value)) {
    return { ok: false, reason: `${path} must be a valid YYYY-MM-DD date` };
  }
  return { ok: true, value };
}

function parseResearchedAt(value: unknown): ParseResult<string> {
  if (typeof value !== "string") {
    return { ok: false, reason: "payload.researched_at must be an ISO timestamp with a timezone" };
  }
  const match =
    /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-](\d{2}):(\d{2}))$/.exec(value);
  if (!match) {
    return { ok: false, reason: "payload.researched_at must be an ISO timestamp with a timezone" };
  }
  const hour = Number.parseInt(match[2]!, 10);
  const minute = Number.parseInt(match[3]!, 10);
  const second = Number.parseInt(match[4]!, 10);
  const offsetHour = match[5] === undefined ? 0 : Number.parseInt(match[5], 10);
  const offsetMinute = match[6] === undefined ? 0 : Number.parseInt(match[6], 10);
  if (
    !isIsoDate(match[1]!) ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59 ||
    !Number.isFinite(Date.parse(value))
  ) {
    return { ok: false, reason: "payload.researched_at must be an ISO timestamp with a timezone" };
  }
  return { ok: true, value };
}

function parseNullableAmount(value: unknown, path: string): ParseResult<number | null> {
  if (value === null) {
    return { ok: true, value: null };
  }
  const cents = typeof value === "number" ? Math.round(value * 100) : Number.NaN;
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    !Number.isSafeInteger(cents) ||
    Math.abs(value * 100 - cents) > 1e-7
  ) {
    return { ok: false, reason: `${path} must be a non-negative dollar amount with at most two decimals or null` };
  }
  return { ok: true, value };
}

function parsePositiveAmount(value: unknown, path: string): ParseResult<number> {
  const cents = typeof value === "number" ? Math.round(value * 100) : Number.NaN;
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value <= 0 ||
    !Number.isSafeInteger(cents) ||
    Math.abs(value * 100 - cents) > 1e-7
  ) {
    return { ok: false, reason: `${path} must be a positive dollar amount with at most two decimals` };
  }
  return { ok: true, value };
}

function parseReportedTotals(value: unknown): ParseResult<ManualCandidateFinanceReportedTotals> {
  if (!isObject(value)) {
    return { ok: false, reason: "payload.reported_totals must be an object" };
  }

  const fields = [
    "contributions_this_period",
    "contributions_calendar_ytd",
    "disbursements_this_period",
    "disbursements_calendar_ytd",
    "cash_on_hand",
    "debts_owed",
  ] as const;
  const totals = {} as ManualCandidateFinanceReportedTotals;
  for (const field of fields) {
    if (!(field in value)) {
      return {
        ok: false,
        reason: `payload.reported_totals.${field} is required; use null when the filing does not report it`,
      };
    }
    const parsed = parseNullableAmount(value[field], `payload.reported_totals.${field}`);
    if (!parsed.ok) {
      return parsed;
    }
    totals[field] = parsed.value;
  }
  return { ok: true, value: totals };
}

function parseItemizedReceipts(value: unknown): ParseResult<ManualCandidateFinanceItemizedReceipt[]> {
  if (!Array.isArray(value)) {
    return { ok: false, reason: "payload.itemized_receipts must be an array" };
  }

  const receipts: ManualCandidateFinanceItemizedReceipt[] = [];
  for (const [index, rawReceipt] of value.entries()) {
    const path = `payload.itemized_receipts[${index}]`;
    if (!isObject(rawReceipt)) {
      return { ok: false, reason: `${path} must be an object` };
    }
    const receivedDate = parseIsoDate(rawReceipt.received_date, `${path}.received_date`);
    if (!receivedDate.ok) {
      return receivedDate;
    }
    const amount = parsePositiveAmount(rawReceipt.amount, `${path}.amount`);
    if (!amount.ok) {
      return amount;
    }
    const occupation = parseNullableString(rawReceipt.occupation, `${path}.occupation`);
    if (!occupation.ok) {
      return occupation;
    }
    const employer = parseNullableString(rawReceipt.employer, `${path}.employer`);
    if (!employer.ok) {
      return employer;
    }
    receipts.push({
      received_date: receivedDate.value,
      amount: amount.value,
      occupation: occupation.value,
      employer: employer.value,
    });
  }
  return { ok: true, value: receipts };
}

function parseMississippiSosSourceUrl(value: unknown, filingId: string): ParseResult<string> {
  const raw = parseNonEmptyString(value, "payload.source_url");
  if (!raw.ok) {
    return raw;
  }
  const normalized = normalizeHttpUrl(raw.value);
  if (!normalized) {
    return { ok: false, reason: "payload.source_url must be a valid HTTP(S) URL" };
  }

  const url = new URL(normalized);
  const filingIds = url.searchParams.getAll("FilingId");
  const workflowIds = url.searchParams.getAll("WorkflowId");
  if (
    url.protocol !== "https:" ||
    url.hostname.toLowerCase() !== "cfportal.sos.ms.gov" ||
    !url.pathname.toLowerCase().endsWith("/executeworkflow.aspx") ||
    filingIds.length !== 1 ||
    filingIds[0]!.toLowerCase() !== filingId ||
    workflowIds.length !== 1 ||
    workflowIds[0] !== MISSISSIPPI_SOS_WORKFLOW_ID
  ) {
    return {
      ok: false,
      reason: "payload.source_url must be the Mississippi SOS filing URL whose FilingId matches payload.filing_id",
    };
  }
  return { ok: true, value: normalized };
}

function parseCandidateEdges(value: unknown): ParseResult<ManualCandidateFinanceCandidateEdge[]> {
  if (!Array.isArray(value) || value.length === 0) {
    return { ok: false, reason: "payload.candidate_edges must contain at least one edge" };
  }

  const edges: ManualCandidateFinanceCandidateEdge[] = [];
  const seen = new Set<string>();
  for (const [index, rawEdge] of value.entries()) {
    const path = `payload.candidate_edges[${index}]`;
    if (!isObject(rawEdge)) {
      return { ok: false, reason: `${path} must be an object` };
    }
    const candidateId = parseUuid(rawEdge.candidate_id, `${path}.candidate_id`);
    if (!candidateId.ok) {
      return candidateId;
    }
    const electionId = parseUuid(rawEdge.election_id, `${path}.election_id`);
    if (!electionId.ok) {
      return electionId;
    }
    const candidateName = parseNonEmptyString(rawEdge.candidate_name, `${path}.candidate_name`);
    if (!candidateName.ok) {
      return candidateName;
    }
    if (rawEdge.support_oppose !== "support" && rawEdge.support_oppose !== "oppose") {
      return { ok: false, reason: `${path}.support_oppose must be support or oppose` };
    }
    if (!("amount" in rawEdge)) {
      return { ok: false, reason: `${path}.amount is required; use null when the filing does not allocate it` };
    }
    const amount = parseNullableAmount(rawEdge.amount, `${path}.amount`);
    if (!amount.ok) {
      return amount;
    }

    const duplicateKey = `${candidateId.value}\u0000${electionId.value}\u0000${rawEdge.support_oppose}`;
    if (seen.has(duplicateKey)) {
      return { ok: false, reason: `${path} duplicates an earlier candidate edge` };
    }
    seen.add(duplicateKey);
    edges.push({
      candidate_id: candidateId.value,
      election_id: electionId.value,
      candidate_name: candidateName.value,
      support_oppose: rawEdge.support_oppose,
      amount: amount.value,
    });
  }
  return { ok: true, value: edges };
}

export function parseManualCandidateFinancePayload(
  payload: unknown
): { ok: true; payload: ManualCandidateFinancePayload } | { ok: false; reason: string } {
  if (!isObject(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }
  if (payload.schema_version !== MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION) {
    return {
      ok: false,
      reason: `payload.schema_version must be ${MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION}`,
    };
  }
  if (payload.state !== "MS") {
    return { ok: false, reason: "payload.state must be MS" };
  }
  if (payload.coverage_note !== MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE) {
    return { ok: false, reason: "payload.coverage_note must state the Mississippi SOS-only coverage boundary" };
  }

  const filingId = parseUuid(payload.filing_id, "payload.filing_id");
  if (!filingId.ok) {
    return filingId;
  }
  if (!("amends_filing_id" in payload)) {
    return {
      ok: false,
      reason: "payload.amends_filing_id is required; use null only after verifying the filing is not an amendment",
    };
  }
  const amendsFilingId =
    payload.amends_filing_id === null
      ? ({ ok: true, value: null } as const)
      : parseUuid(payload.amends_filing_id, "payload.amends_filing_id");
  if (!amendsFilingId.ok) {
    return amendsFilingId;
  }
  if (amendsFilingId.value === filingId.value) {
    return { ok: false, reason: "payload.amends_filing_id must not equal payload.filing_id" };
  }
  const reportDate = parseIsoDate(payload.report_date, "payload.report_date");
  if (!reportDate.ok) {
    return reportDate;
  }
  const sourceUrl = parseMississippiSosSourceUrl(payload.source_url, filingId.value);
  if (!sourceUrl.ok) {
    return sourceUrl;
  }
  const researchedAt = parseResearchedAt(payload.researched_at);
  if (!researchedAt.ok) {
    return researchedAt;
  }
  const reportedTotals = parseReportedTotals(payload.reported_totals);
  if (!reportedTotals.ok) {
    return reportedTotals;
  }
  const itemizedReceipts = parseItemizedReceipts(payload.itemized_receipts);
  if (!itemizedReceipts.ok) {
    return itemizedReceipts;
  }

  const common = {
    schema_version: MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION,
    state: "MS" as const,
    filing_id: filingId.value,
    amends_filing_id: amendsFilingId.value,
    report_date: reportDate.value,
    source_url: sourceUrl.value,
    coverage_note: MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE,
    researched_at: researchedAt.value,
    reported_totals: reportedTotals.value,
    itemized_receipts: itemizedReceipts.value,
  };

  if (payload.filing_type === "candidate_report") {
    const candidateId = parseUuid(payload.candidate_id, "payload.candidate_id");
    if (!candidateId.ok) {
      return candidateId;
    }
    const electionId = parseUuid(payload.election_id, "payload.election_id");
    if (!electionId.ok) {
      return electionId;
    }
    const candidateName = parseNonEmptyString(payload.candidate_name, "payload.candidate_name");
    if (!candidateName.ok) {
      return candidateName;
    }
    return {
      ok: true,
      payload: {
        ...common,
        filing_type: "candidate_report",
        candidate_id: candidateId.value,
        election_id: electionId.value,
        candidate_name: candidateName.value,
      },
    };
  }

  if (payload.filing_type === "independent_expenditure") {
    if (!isObject(payload.outside_spender)) {
      return { ok: false, reason: "payload.outside_spender must be an object" };
    }
    const sourceEntityId =
      payload.outside_spender.source_entity_id === null
        ? ({ ok: true, value: null } as const)
        : parseUuid(payload.outside_spender.source_entity_id, "payload.outside_spender.source_entity_id");
    if (!sourceEntityId.ok) {
      return sourceEntityId;
    }
    const spenderName = parseNonEmptyString(payload.outside_spender.name, "payload.outside_spender.name");
    if (!spenderName.ok) {
      return spenderName;
    }
    const candidateEdges = parseCandidateEdges(payload.candidate_edges);
    if (!candidateEdges.ok) {
      return candidateEdges;
    }
    const disbursementsThisPeriod = reportedTotals.value.disbursements_this_period;
    if (disbursementsThisPeriod !== null) {
      let allocatedCents = 0;
      for (const edge of candidateEdges.value) {
        if (edge.amount === null) {
          continue;
        }
        allocatedCents += Math.round(edge.amount * 100);
        if (!Number.isSafeInteger(allocatedCents)) {
          return { ok: false, reason: "payload.candidate_edges allocated amount exceeds the safe cent range" };
        }
      }
      if (allocatedCents > Math.round(disbursementsThisPeriod * 100)) {
        return {
          ok: false,
          reason: "payload.candidate_edges allocated amount must not exceed payload.reported_totals.disbursements_this_period",
        };
      }
    }
    return {
      ok: true,
      payload: {
        ...common,
        filing_type: "independent_expenditure",
        outside_spender: {
          source_entity_id: sourceEntityId.value,
          name: spenderName.value,
        },
        candidate_edges: candidateEdges.value,
      },
    };
  }

  return {
    ok: false,
    reason: "payload.filing_type must be candidate_report or independent_expenditure",
  };
}
