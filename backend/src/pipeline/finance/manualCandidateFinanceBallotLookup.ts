import type { Pool, PoolClient } from "pg";

import {
  MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE,
  parseManualCandidateFinancePayload,
  type ManualCandidateFinanceIndependentExpenditurePayload,
  type ManualCandidateFinancePayload,
} from "../../contracts/manualCandidateFinancePayloadContract.js";
import {
  buildStateFinanceSummaryRequests,
  candidateElectionKey,
  electionYear,
  type BallotLookupFinanceBreakdown,
  type BallotLookupFinanceOutsideGroup,
  type BallotLookupFinanceSummary,
  type BallotLookupFinanceUnallocatedOutsideEdge,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { manualCandidateFinancePayloadSha256 } from "./manualCandidateFinancePersistence.js";
import {
  compileManualCandidateFinancePreview,
  type ManualCandidateFinanceCandidatePreview,
  type ManualCandidateFinanceOutsideGroupPreview,
} from "./manualCandidateFinancePreview.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type ManualCandidateFinanceElectionRow = StateFinanceRequestElectionRow & {
  election_date: string;
};

type StoredFilingRow = {
  filing_id: string;
  payload: unknown;
  payload_sha256: string;
};

const DIRECT_COVERAGE_NOTE =
  `${MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE} ` +
  "Occupation and employer breakdowns cover only itemized receipts transcribed for the selected latest report; an empty breakdown does not prove zero receipts.";

const UNALLOCATED_OUTSIDE_NOTE =
  " Candidate-direction totals remain null when a filing does not report a candidate-level amount.";

function normalizedTextKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLocaleUpperCase("en-US");
}

function payloadTargetsCandidate(
  payload: ManualCandidateFinancePayload,
  candidateId: string,
  electionId: string
): boolean {
  if (payload.filing_type === "candidate_report") {
    return payload.candidate_id === candidateId && payload.election_id === electionId;
  }
  return payload.candidate_edges.some(
    (edge) => edge.candidate_id === candidateId && edge.election_id === electionId
  );
}

function parseStoredFiling(row: StoredFilingRow): ManualCandidateFinancePayload {
  const parsed = parseManualCandidateFinancePayload(row.payload);
  if (!parsed.ok) {
    throw new Error(`Stored manual candidate-finance filing ${row.filing_id} is invalid: ${parsed.reason}`);
  }
  if (parsed.payload.filing_id !== row.filing_id) {
    throw new Error(
      `Stored manual candidate-finance filing key ${row.filing_id} does not match payload filing_id ${parsed.payload.filing_id}`
    );
  }
  if (manualCandidateFinancePayloadSha256(parsed.payload) !== row.payload_sha256) {
    throw new Error(`Stored manual candidate-finance filing ${row.filing_id} has a payload hash mismatch`);
  }
  return parsed.payload;
}

function latestResearchTimestamp(
  candidate: ManualCandidateFinanceCandidatePreview,
  payloads: readonly ManualCandidateFinancePayload[]
): string {
  const canonicalSourceUrls = new Set(candidate.sourceUrls);
  const timestamps = payloads
    .filter(
      (payload) =>
        canonicalSourceUrls.has(payload.source_url) &&
        payloadTargetsCandidate(payload, candidate.candidateId, candidate.electionId)
    )
    .map((payload) => payload.researched_at)
    .sort();
  const latest = timestamps.at(-1);
  if (!latest) {
    throw new Error(
      `Manual candidate-finance preview ${candidate.candidateId}/${candidate.electionId} has no research timestamp`
    );
  }
  return latest;
}

function spenderMatches(
  filing: ManualCandidateFinanceIndependentExpenditurePayload,
  group: ManualCandidateFinanceOutsideGroupPreview
): boolean {
  if (group.sourceEntityId !== null) {
    return filing.outside_spender.source_entity_id === group.sourceEntityId;
  }
  return (
    filing.outside_spender.source_entity_id === null &&
    normalizedTextKey(filing.outside_spender.name) === normalizedTextKey(group.name)
  );
}

function singleOutsideGroupSourceUrl(input: {
  candidate: ManualCandidateFinanceCandidatePreview;
  group: ManualCandidateFinanceOutsideGroupPreview;
  payloads: readonly ManualCandidateFinancePayload[];
}): string | null {
  const canonicalSourceUrls = new Set(input.candidate.sourceUrls);
  const sourceUrls = new Set(
    input.payloads
      .filter(
        (payload): payload is ManualCandidateFinanceIndependentExpenditurePayload =>
          payload.filing_type === "independent_expenditure"
      )
      .filter(
        (filing) =>
          canonicalSourceUrls.has(filing.source_url) &&
          spenderMatches(filing, input.group) &&
          filing.candidate_edges.some(
            (edge) =>
              edge.candidate_id === input.candidate.candidateId &&
              edge.election_id === input.candidate.electionId &&
              edge.support_oppose === input.group.supportOppose &&
              edge.amount !== null
          )
      )
      .map((filing) => filing.source_url)
  );
  // A group can combine multiple filings, while the shared response permits
  // only one URL. Never attach one partial filing to an aggregate amount.
  return sourceUrls.size === 1 ? (sourceUrls.values().next().value ?? null) : null;
}

function mapBreakdown(
  breakdown: { categoryName: string; amount: number; receiptCount: number },
  sourceUrl: string
): BallotLookupFinanceBreakdown {
  return {
    category_name: breakdown.categoryName,
    amount: breakdown.amount,
    // The manual contract preserves receipts but has no contributor identity,
    // so repeated receipts cannot safely be collapsed into a donor count.
    contributor_count: null,
    source_url: sourceUrl,
  };
}

function mapOutsideGroup(input: {
  candidate: ManualCandidateFinanceCandidatePreview;
  group: ManualCandidateFinanceOutsideGroupPreview;
  payloads: readonly ManualCandidateFinancePayload[];
}): BallotLookupFinanceOutsideGroup {
  return {
    committee_id:
      input.group.sourceEntityId ?? `manual-name:${normalizedTextKey(input.group.name)}`,
    committee_name: input.group.name,
    support_oppose: input.group.supportOppose,
    amount: input.group.amount,
    source_url: singleOutsideGroupSourceUrl(input),
  };
}

function mapUnallocatedOutsideEdge(
  edge: ManualCandidateFinanceCandidatePreview["outsideSpending"]["unallocatedEdges"][number]
): BallotLookupFinanceUnallocatedOutsideEdge {
  return {
    filing_id: edge.filingId,
    report_date: edge.reportDate,
    committee_id: edge.sourceEntityId ?? `manual-name:${normalizedTextKey(edge.spenderName)}`,
    committee_name: edge.spenderName,
    support_oppose: edge.supportOppose,
    source_url: edge.sourceUrl,
  };
}

function toFinanceSummary(input: {
  candidate: ManualCandidateFinanceCandidatePreview;
  cycle: number;
  payloads: readonly ManualCandidateFinancePayload[];
}): BallotLookupFinanceSummary {
  const report = input.candidate.selectedCandidateReport;
  const occupations =
    report?.occupationBreakdowns.map((breakdown) => mapBreakdown(breakdown, report.sourceUrl)) ?? [];
  const employers =
    report?.employerBreakdowns.map((breakdown) => mapBreakdown(breakdown, report.sourceUrl)) ?? [];
  const outsideGroups = input.candidate.outsideSpending.groups.map((group) =>
    mapOutsideGroup({ candidate: input.candidate, group, payloads: input.payloads })
  );
  const hasUnallocatedOutsideSpending = input.candidate.outsideSpending.unallocatedEdges.length > 0;

  return {
    source: "MISSISSIPPI_SOS",
    cycle: input.cycle,
    fec_candidate_id: null,
    controlled_committee_id: null,
    last_synced_at: latestResearchTimestamp(input.candidate, input.payloads),
    direct_campaign: {
      total_raised: report?.reportedTotals.contributions_calendar_ytd ?? null,
      total_spent: report?.reportedTotals.disbursements_calendar_ytd ?? null,
      cash_on_hand: report?.reportedTotals.cash_on_hand ?? null,
      debts_owed: report?.reportedTotals.debts_owed ?? null,
      top_occupations: occupations,
      top_employers: employers,
      top_industries: [],
      direct_coverage_note: DIRECT_COVERAGE_NOTE,
    },
    outside_spending: {
      support_total: input.candidate.outsideSpending.supportTotal,
      oppose_total: input.candidate.outsideSpending.opposeTotal,
      outside_coverage_note:
        MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE +
        (hasUnallocatedOutsideSpending ? UNALLOCATED_OUTSIDE_NOTE : ""),
      top_supporting_groups: outsideGroups.filter((group) => group.support_oppose === "support"),
      top_opposing_groups: outsideGroups.filter((group) => group.support_oppose === "oppose"),
      unallocated_candidate_edges:
        input.candidate.outsideSpending.unallocatedEdges.map(mapUnallocatedOutsideEdge),
      top_supporting_industries: [],
      top_opposing_industries: [],
    },
    backing_summary: {
      top_direct_donor_occupations: occupations,
      top_outside_supporting_industries: [],
    },
  };
}

export async function loadManualCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly ManualCandidateFinanceElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  const requests = buildStateFinanceSummaryRequests("MS", candidateRows, electionRows);
  if (requests.length === 0) {
    return new Map();
  }

  // The ledger is intentionally small and append-only. Reading it in full is
  // important: an amendment can remove a candidate, so selecting filings only
  // through current target rows could accidentally revive the superseded edge.
  const stored = await db.query<StoredFilingRow>(
    `
      SELECT filing_id::text AS filing_id, payload, payload_sha256
      FROM public.manual_candidate_finance_filings
      ORDER BY report_date, filing_id
    `
  );
  if (stored.rows.length === 0) {
    return new Map();
  }

  const payloads = stored.rows.map(parseStoredFiling);
  const preview = compileManualCandidateFinancePreview(payloads);
  const requestedKeys = new Set(
    requests.map((request) => candidateElectionKey(request.candidate_id, request.election_id))
  );
  const cycleByElectionId = new Map(
    electionRows.map((row) => [row.election_id, electionYear(row.election_date)])
  );
  const summaries = new Map<string, BallotLookupFinanceSummary>();

  for (const candidate of preview.candidates) {
    const key = candidateElectionKey(candidate.candidateId, candidate.electionId);
    if (!requestedKeys.has(key)) {
      continue;
    }
    const cycle = cycleByElectionId.get(candidate.electionId);
    if (cycle == null) {
      throw new Error(`Manual candidate-finance election ${candidate.electionId} has no election date`);
    }
    summaries.set(key, toFinanceSummary({ candidate, cycle, payloads }));
  }
  return summaries;
}
