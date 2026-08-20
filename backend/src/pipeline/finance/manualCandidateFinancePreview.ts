import {
  MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION,
  type ManualCandidateFinanceCandidateReportPayload,
  type ManualCandidateFinanceIndependentExpenditurePayload,
  type ManualCandidateFinanceItemizedReceipt,
  type ManualCandidateFinancePayload,
  type ManualCandidateFinanceReportedTotals,
} from "../../contracts/manualCandidateFinancePayloadContract.js";

export type ManualCandidateFinancePreviewWarningCode =
  | "ambiguous_latest_candidate_report"
  | "candidate_name_variant"
  | "latest_report_receipts_only"
  | "unallocated_outside_spending";

export type ManualCandidateFinancePreviewWarning = {
  code: ManualCandidateFinancePreviewWarningCode;
  filingIds: string[];
  message: string;
};

export type ManualCandidateFinanceReceiptBreakdown = {
  categoryName: string;
  amount: number;
  receiptCount: number;
};

export type ManualCandidateFinanceSelectedReportPreview = {
  filingId: string;
  reportDate: string;
  sourceUrl: string;
  reportedTotals: ManualCandidateFinanceReportedTotals;
  receiptCount: number;
  occupationBreakdowns: ManualCandidateFinanceReceiptBreakdown[];
  employerBreakdowns: ManualCandidateFinanceReceiptBreakdown[];
  receiptsWithoutOccupation: number;
  receiptsWithoutEmployer: number;
};

export type ManualCandidateFinanceOutsideGroupPreview = {
  sourceEntityId: string | null;
  name: string;
  supportOppose: "support" | "oppose";
  amount: number;
};

export type ManualCandidateFinanceUnallocatedEdgePreview = {
  filingId: string;
  reportDate: string;
  sourceUrl: string;
  sourceEntityId: string | null;
  spenderName: string;
  supportOppose: "support" | "oppose";
  filingDisbursementsThisPeriod: number | null;
};

export type ManualCandidateFinanceCandidatePreview = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  selectedCandidateReport: ManualCandidateFinanceSelectedReportPreview | null;
  outsideSpending: {
    supportTotal: number | null;
    opposeTotal: number | null;
    knownAllocatedSupportAmount: number;
    knownAllocatedOpposeAmount: number;
    groups: ManualCandidateFinanceOutsideGroupPreview[];
    unallocatedEdges: ManualCandidateFinanceUnallocatedEdgePreview[];
  };
  sourceUrls: string[];
  warnings: ManualCandidateFinancePreviewWarning[];
};

export type ManualCandidateFinancePreview = {
  schemaVersion: typeof MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION;
  state: "MS";
  inputFilingCount: number;
  uniqueFilingCount: number;
  candidates: ManualCandidateFinanceCandidatePreview[];
};

type CandidateAccumulator = {
  candidateId: string;
  electionId: string;
  candidateNames: string[];
  candidateReports: ManualCandidateFinanceCandidateReportPayload[];
  outsideEdges: Array<{
    filing: ManualCandidateFinanceIndependentExpenditurePayload;
    edge: ManualCandidateFinanceIndependentExpenditurePayload["candidate_edges"][number];
  }>;
  sourceUrls: Set<string>;
};

type ReceiptAggregate = {
  categoryName: string;
  amountCents: number;
  receiptCount: number;
};

function candidateKey(candidateId: string, electionId: string): string {
  return `${candidateId}\u0000${electionId}`;
}

function normalizedTextKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLocaleUpperCase("en-US");
}

function moneyToCents(value: number): number {
  const cents = Math.round(value * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(value * 100 - cents) > 1e-7) {
    throw new Error(`Manual candidate-finance amount is not cent-precise: ${value}`);
  }
  return cents;
}

function centsToMoney(value: number): number {
  return value / 100;
}

function addCents(left: number, right: number): number {
  const total = left + right;
  if (!Number.isSafeInteger(total)) {
    throw new Error("Manual candidate-finance aggregation exceeds JavaScript's safe cent range");
  }
  return total;
}

function stablePayloadFingerprint(payload: ManualCandidateFinancePayload): string {
  return JSON.stringify(payload);
}

function addCandidateName(accumulator: CandidateAccumulator, candidateName: string): void {
  if (!accumulator.candidateNames.includes(candidateName)) {
    accumulator.candidateNames.push(candidateName);
  }
}

function getCandidateAccumulator(
  candidates: Map<string, CandidateAccumulator>,
  candidateId: string,
  electionId: string,
  candidateName: string
): CandidateAccumulator {
  const key = candidateKey(candidateId, electionId);
  const existing = candidates.get(key);
  if (existing) {
    addCandidateName(existing, candidateName);
    return existing;
  }
  const created: CandidateAccumulator = {
    candidateId,
    electionId,
    candidateNames: [candidateName],
    candidateReports: [],
    outsideEdges: [],
    sourceUrls: new Set<string>(),
  };
  candidates.set(key, created);
  return created;
}

function addReceiptAggregate(
  aggregates: Map<string, ReceiptAggregate>,
  categoryName: string,
  amount: number
): void {
  const normalizedName = categoryName.trim().replace(/\s+/g, " ");
  const key = normalizedTextKey(normalizedName);
  const existing = aggregates.get(key);
  if (existing) {
    existing.amountCents = addCents(existing.amountCents, moneyToCents(amount));
    existing.receiptCount += 1;
    return;
  }
  aggregates.set(key, {
    categoryName: normalizedName,
    amountCents: moneyToCents(amount),
    receiptCount: 1,
  });
}

function toReceiptBreakdowns(aggregates: Iterable<ReceiptAggregate>): ManualCandidateFinanceReceiptBreakdown[] {
  return [...aggregates]
    .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
    .map((aggregate) => ({
      categoryName: aggregate.categoryName,
      amount: centsToMoney(aggregate.amountCents),
      receiptCount: aggregate.receiptCount,
    }));
}

function compileReceiptBreakdowns(receipts: readonly ManualCandidateFinanceItemizedReceipt[]): {
  occupationBreakdowns: ManualCandidateFinanceReceiptBreakdown[];
  employerBreakdowns: ManualCandidateFinanceReceiptBreakdown[];
  receiptsWithoutOccupation: number;
  receiptsWithoutEmployer: number;
} {
  const occupations = new Map<string, ReceiptAggregate>();
  const employers = new Map<string, ReceiptAggregate>();
  let receiptsWithoutOccupation = 0;
  let receiptsWithoutEmployer = 0;
  for (const receipt of receipts) {
    if (receipt.occupation === null) {
      receiptsWithoutOccupation += 1;
    } else {
      addReceiptAggregate(occupations, receipt.occupation, receipt.amount);
    }
    if (receipt.employer === null) {
      receiptsWithoutEmployer += 1;
    } else {
      addReceiptAggregate(employers, receipt.employer, receipt.amount);
    }
  }
  return {
    occupationBreakdowns: toReceiptBreakdowns(occupations.values()),
    employerBreakdowns: toReceiptBreakdowns(employers.values()),
    receiptsWithoutOccupation,
    receiptsWithoutEmployer,
  };
}

function selectLatestCandidateReport(accumulator: CandidateAccumulator): {
  report: ManualCandidateFinanceCandidateReportPayload | null;
  warning: ManualCandidateFinancePreviewWarning | null;
} {
  if (accumulator.candidateReports.length === 0) {
    return { report: null, warning: null };
  }
  const latestDate = accumulator.candidateReports.reduce(
    (latest, report) => (report.report_date > latest ? report.report_date : latest),
    accumulator.candidateReports[0]!.report_date
  );
  const latestReports = accumulator.candidateReports.filter((report) => report.report_date === latestDate);
  if (latestReports.length !== 1) {
    const filingIds = latestReports.map((report) => report.filing_id).sort();
    return {
      report: null,
      warning: {
        code: "ambiguous_latest_candidate_report",
        filingIds,
        message: `Multiple candidate reports share latest report date ${latestDate}; no cover totals were selected because amendment order is not encoded.`,
      },
    };
  }
  return { report: latestReports[0]!, warning: null };
}

function compileCandidatePreview(accumulator: CandidateAccumulator): ManualCandidateFinanceCandidatePreview {
  const warnings: ManualCandidateFinancePreviewWarning[] = [];
  const selected = selectLatestCandidateReport(accumulator);
  if (selected.warning) {
    warnings.push(selected.warning);
  }

  const candidateReportName = selected.report?.candidate_name ?? accumulator.candidateReports[0]?.candidate_name;
  const candidateName = candidateReportName ?? accumulator.candidateNames[0]!;
  const distinctNameKeys = new Set(accumulator.candidateNames.map(normalizedTextKey));
  if (distinctNameKeys.size > 1) {
    warnings.push({
      code: "candidate_name_variant",
      filingIds: [
        ...accumulator.candidateReports.map((report) => report.filing_id),
        ...accumulator.outsideEdges.map(({ filing }) => filing.filing_id),
      ].filter((value, index, values) => values.indexOf(value) === index).sort(),
      message: `Candidate ID appears with multiple names: ${accumulator.candidateNames.join(" | ")}.`,
    });
  }

  let selectedCandidateReport: ManualCandidateFinanceSelectedReportPreview | null = null;
  if (selected.report) {
    const receiptBreakdowns = compileReceiptBreakdowns(selected.report.itemized_receipts);
    selectedCandidateReport = {
      filingId: selected.report.filing_id,
      reportDate: selected.report.report_date,
      sourceUrl: selected.report.source_url,
      reportedTotals: selected.report.reported_totals,
      receiptCount: selected.report.itemized_receipts.length,
      ...receiptBreakdowns,
    };
    warnings.push({
      code: "latest_report_receipts_only",
      filingIds: [selected.report.filing_id],
      message: "Occupation and employer breakdowns cover only itemized receipts supplied for the selected latest report; an empty array does not prove zero receipts, and these are not claimed as cycle-wide totals.",
    });
  }

  const knownByDirection = { support: 0, oppose: 0 };
  const edgeCountByDirection = { support: 0, oppose: 0 };
  const unresolvedByDirection = { support: 0, oppose: 0 };
  const groupAggregates = new Map<
    string,
    { sourceEntityId: string | null; name: string; supportOppose: "support" | "oppose"; amountCents: number }
  >();
  const unallocatedEdges: ManualCandidateFinanceUnallocatedEdgePreview[] = [];

  for (const { filing, edge } of accumulator.outsideEdges) {
    edgeCountByDirection[edge.support_oppose] += 1;
    if (edge.amount === null) {
      unresolvedByDirection[edge.support_oppose] += 1;
      unallocatedEdges.push({
        filingId: filing.filing_id,
        reportDate: filing.report_date,
        sourceUrl: filing.source_url,
        sourceEntityId: filing.outside_spender.source_entity_id,
        spenderName: filing.outside_spender.name,
        supportOppose: edge.support_oppose,
        filingDisbursementsThisPeriod: filing.reported_totals.disbursements_this_period,
      });
      continue;
    }

    const amountCents = moneyToCents(edge.amount);
    knownByDirection[edge.support_oppose] = addCents(knownByDirection[edge.support_oppose], amountCents);
    const groupKey = `${filing.outside_spender.source_entity_id ?? ""}\u0000${normalizedTextKey(filing.outside_spender.name)}\u0000${edge.support_oppose}`;
    const existing = groupAggregates.get(groupKey);
    if (existing) {
      existing.amountCents = addCents(existing.amountCents, amountCents);
    } else {
      groupAggregates.set(groupKey, {
        sourceEntityId: filing.outside_spender.source_entity_id,
        name: filing.outside_spender.name,
        supportOppose: edge.support_oppose,
        amountCents,
      });
    }
  }

  if (unallocatedEdges.length > 0) {
    warnings.push({
      code: "unallocated_outside_spending",
      filingIds: [...new Set(unallocatedEdges.map((edge) => edge.filingId))].sort(),
      message: `${unallocatedEdges.length} outside-spending edge(s) have no candidate-level amount; direction totals remain null instead of copying or splitting filing totals.`,
    });
  }

  const directionTotal = (direction: "support" | "oppose"): number | null =>
    edgeCountByDirection[direction] > 0 && unresolvedByDirection[direction] === 0
      ? centsToMoney(knownByDirection[direction])
      : null;

  return {
    candidateId: accumulator.candidateId,
    electionId: accumulator.electionId,
    candidateName,
    selectedCandidateReport,
    outsideSpending: {
      supportTotal: directionTotal("support"),
      opposeTotal: directionTotal("oppose"),
      knownAllocatedSupportAmount: centsToMoney(knownByDirection.support),
      knownAllocatedOpposeAmount: centsToMoney(knownByDirection.oppose),
      groups: [...groupAggregates.values()]
        .sort(
          (left, right) =>
            right.amountCents - left.amountCents ||
            left.supportOppose.localeCompare(right.supportOppose) ||
            left.name.localeCompare(right.name)
        )
        .map((group) => ({
          sourceEntityId: group.sourceEntityId,
          name: group.name,
          supportOppose: group.supportOppose,
          amount: centsToMoney(group.amountCents),
        })),
      unallocatedEdges: unallocatedEdges.sort(
        (left, right) =>
          left.reportDate.localeCompare(right.reportDate) ||
          left.filingId.localeCompare(right.filingId) ||
          left.supportOppose.localeCompare(right.supportOppose)
      ),
    },
    sourceUrls: [...accumulator.sourceUrls].sort(),
    warnings,
  };
}

export function compileManualCandidateFinancePreview(
  payloads: readonly ManualCandidateFinancePayload[]
): ManualCandidateFinancePreview {
  if (payloads.length === 0) {
    throw new Error("Manual candidate-finance preview requires at least one filing payload");
  }

  const filings = new Map<string, ManualCandidateFinancePayload>();
  for (const payload of payloads) {
    const existing = filings.get(payload.filing_id);
    if (!existing) {
      filings.set(payload.filing_id, payload);
      continue;
    }
    if (stablePayloadFingerprint(existing) !== stablePayloadFingerprint(payload)) {
      throw new Error(`Conflicting manual candidate-finance payloads share filing_id ${payload.filing_id}`);
    }
  }

  const candidates = new Map<string, CandidateAccumulator>();
  for (const payload of filings.values()) {
    if (payload.filing_type === "candidate_report") {
      const candidate = getCandidateAccumulator(
        candidates,
        payload.candidate_id,
        payload.election_id,
        payload.candidate_name
      );
      candidate.candidateReports.push(payload);
      candidate.sourceUrls.add(payload.source_url);
      continue;
    }
    for (const edge of payload.candidate_edges) {
      const candidate = getCandidateAccumulator(
        candidates,
        edge.candidate_id,
        edge.election_id,
        edge.candidate_name
      );
      candidate.outsideEdges.push({ filing: payload, edge });
      candidate.sourceUrls.add(payload.source_url);
    }
  }

  return {
    schemaVersion: MANUAL_CANDIDATE_FINANCE_SCHEMA_VERSION,
    state: "MS",
    inputFilingCount: payloads.length,
    uniqueFilingCount: filings.size,
    candidates: [...candidates.values()]
      .map(compileCandidatePreview)
      .sort(
        (left, right) =>
          left.electionId.localeCompare(right.electionId) ||
          left.candidateName.localeCompare(right.candidateName) ||
          left.candidateId.localeCompare(right.candidateId)
      ),
  };
}
