import { ohioPersonNamesMatch, ohioSosOfficeTokenForOfficeName } from "./ohioCandidateCommitteeResolver.js";
import {
  reconcileOhioSos31uReport,
  type OhioSos31uDetailRow,
  type OhioSos31uReconciliation,
} from "./ohioSos31uDetail.js";
import {
  isOhioSos31uExpenditureRow,
  type OhioSosCoverPageRow,
  type OhioSosExpenditureRow,
} from "./ohioSosBulkFiles.js";

// Form 31-U outside-spending aggregation, the two-stage model of
// ohio_plan.md decision 4. Stage one reads the annual expenditure bulk
// rows (candidate, PAC, AND party committees all file 31-U) to discover
// report keys and pin each report's spender identity — the detail export
// has no MASTER_KEY, so the spender is always carried in from the annual
// (MASTER_KEY, REPORT_KEY) pair, never re-derived by name. Stage two
// aggregates the per-report detail rows, which alone carry target, office,
// and direction.
//
// Every report is gated by reconciliation before any of its rows are
// aggregated: the detail total must equal the annual bulk total, and, when
// a cover page exists for the report, its VALUE_IND_EXPENDITURES must
// agree too. On the real 2026-cycle files both held exactly wherever
// present; 14 of 28 cycle report keys have no cover row at all
// (federal-calendar super-PAC reports are absent from the cover exports),
// so a missing cover row is structural, not a mismatch.
//
// Target matching is decision 5 as revised at the spike: strict normalized
// name match, unique across the provided candidate set; the row's office
// token (same vocabulary as the active-candidate list — HOUSE / SENATE /
// blank on the largest rows) is a confirming filter when present, never a
// requirement. No fuzzy matching; ambiguous or unmatched targets are
// quarantined with diagnostics. Identical-looking rows are all counted —
// V-PAC really filed three byte-identical $150,000 rows (decision 3).

export type OhioOutsideSpendingCandidateTarget = {
  // Opaque caller identity (e.g. candidate id or candidate|election pair);
  // results are keyed by it.
  candidateKey: string;
  candidateName: string;
  officeName: string;
};

export type OhioFinanceOutsideGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  sourceUrl: string | null;
};

export type OhioOutsideSpendingCandidateResult = {
  candidateKey: string;
  supportTotal: number;
  opposeTotal: number;
  groups: OhioFinanceOutsideGroup[];
};

export type OhioOutsideSpendingQuarantineReason =
  | "unknown_report_key"
  | "invalid_spender"
  | "annual_detail_mismatch"
  | "cover_mismatch";

export type OhioOutsideSpendingReportResult = {
  reportKey: string;
  spenderCommitteeId: string | null;
  spenderCommitteeName: string | null;
  reconciliation: OhioSos31uReconciliation;
  // Cover-page VALUE_IND_EXPENDITURES; null when the report has no cover
  // row (structural for federal-calendar reports — not a mismatch).
  coverIeCents: number | null;
  quarantined: boolean;
  quarantineReason: OhioOutsideSpendingQuarantineReason | null;
};

export type OhioOutsideSpendingTargetDiagnostic = {
  value: string;
  rowCount: number;
  amountCents: number;
};

export type OhioOutsideSpendingAggregationResult = {
  // Only candidates with at least one attributed directional row.
  candidates: OhioOutsideSpendingCandidateResult[];
  reports: OhioOutsideSpendingReportResult[];
  // Report keys discovered in the annual files with no detail report in the
  // bundle — their money is invisible until the detail is fetched.
  missingDetailReportKeys: string[];
  // Directional rows in healthy reports whose target resolved to zero /
  // multiple candidates (decision 5: quarantine, never guess).
  unmatchedTargets: OhioOutsideSpendingTargetDiagnostic[];
  ambiguousTargets: OhioOutsideSpendingTargetDiagnostic[];
  attributedRowCount: number;
  attributedCents: number;
  quarantinedReportCount: number;
  // Annual 31-U rows dropped in stage one (blank AMOUNT or non-numeric
  // MASTER_KEY) — visible so a growing count is investigated.
  annualSkippedRowCount: number;
};

type AnnualSpender = {
  committeeId: string;
  committeeName: string;
  totalCents: number;
  rowCount: number;
  invalid: boolean;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: "support" | "oppose";
  amountCents: number;
};

type CandidateAccumulator = {
  target: OhioOutsideSpendingCandidateTarget;
  officeToken: string | null;
  supportCents: number;
  opposeCents: number;
  groups: Map<string, GroupAccumulator>;
};

const DEFAULT_MAX_GROUPS = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Ohio outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Ohio outside spending aggregation ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeOfficeToken(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function addTargetDiagnostic(
  map: Map<string, OhioOutsideSpendingTargetDiagnostic>,
  value: string,
  amountCents: number
): void {
  const existing = map.get(value);
  if (existing) {
    existing.rowCount += 1;
    existing.amountCents += amountCents;
    return;
  }
  map.set(value, { value, rowCount: 1, amountCents });
}

export function aggregateOhioOutsideSpending(input: {
  electionYear: number;
  // Annual expenditure rows from the six cycle files; non-31-U rows are
  // ignored here so the caller can stream whole files through.
  annualExpenditureRows: readonly OhioSosExpenditureRow[];
  detailReports: readonly { reportKey: string; rows: readonly OhioSos31uDetailRow[] }[];
  // Cover rows from all three cover files (for the third reconciliation
  // leg); rows for unrelated report keys are ignored.
  coverRows: readonly OhioSosCoverPageRow[];
  candidates: readonly OhioOutsideSpendingCandidateTarget[];
  toleranceCents?: number;
  sourceUrl?: string | null;
  maxGroups?: number;
}): OhioOutsideSpendingAggregationResult {
  normalizeElectionYear(input.electionYear);
  const toleranceCents = input.toleranceCents ?? 0;
  if (!Number.isInteger(toleranceCents) || toleranceCents < 0) {
    throw new Error(`Invalid Ohio outside spending reconciliation tolerance: ${input.toleranceCents}`);
  }
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const sourceUrl = input.sourceUrl ?? null;

  // --- Stage one: spender identity + annual totals per report key. ---
  const spendersByReportKey = new Map<string, AnnualSpender>();
  let annualSkippedRowCount = 0;
  for (const row of input.annualExpenditureRows) {
    if (!isOhioSos31uExpenditureRow(row)) {
      continue;
    }
    const reportKey = row.reportKey.trim();
    const committeeId = row.masterKey.trim();
    if (!reportKey || row.amountCents === null || !/^[0-9]+$/.test(committeeId)) {
      annualSkippedRowCount += 1;
      continue;
    }
    const existing = spendersByReportKey.get(reportKey);
    if (!existing) {
      spendersByReportKey.set(reportKey, {
        committeeId,
        committeeName: row.committeeName.trim(),
        totalCents: row.amountCents,
        rowCount: 1,
        invalid: false,
      });
      continue;
    }
    existing.totalCents += row.amountCents;
    existing.rowCount += 1;
    // Two spenders under one report key is upstream corruption; the whole
    // report fails closed rather than guessing which identity is real.
    if (existing.committeeId !== committeeId) {
      existing.invalid = true;
    }
  }

  const coverIeByReportKey = new Map<string, number | null>();
  for (const row of input.coverRows) {
    const reportKey = row.reportKey.trim();
    if (spendersByReportKey.has(reportKey) && !coverIeByReportKey.has(reportKey)) {
      coverIeByReportKey.set(reportKey, row.valueIndependentExpendituresCents);
    }
  }

  // --- Candidate matching setup (decision 5). ---
  const candidateAccumulators = input.candidates.map((target): CandidateAccumulator => {
    if (!target.candidateKey.trim()) {
      throw new Error("Ohio outside spending candidate target needs a candidateKey");
    }
    return {
      target,
      officeToken: ohioSosOfficeTokenForOfficeName(target.officeName),
      supportCents: 0,
      opposeCents: 0,
      groups: new Map(),
    };
  });

  function matchTarget(row: OhioSos31uDetailRow): CandidateAccumulator[] | "no_target" {
    const targetValue = row.candidateNameOrBallotIssue?.trim() ?? "";
    if (!targetValue) {
      return "no_target";
    }
    let matched = candidateAccumulators.filter((accumulator) =>
      ohioPersonNamesMatch(accumulator.target.candidateName, targetValue)
    );
    const rowOfficeToken = normalizeOfficeToken(row.office);
    if (rowOfficeToken) {
      // Office confirms but never requires (decision 5): a blank office
      // must not quarantine, but a stated office that contradicts the
      // candidate's must — money for a same-named candidate in another
      // office stays unattributed. A candidate whose own office token is
      // unknown cannot be confirmed and drops out the same way.
      matched = matched.filter((accumulator) => accumulator.officeToken === rowOfficeToken);
    }
    return matched;
  }

  // --- Stage two: per-report reconciliation gate, then attribution. ---
  const seenReportKeys = new Set<string>();
  const reports: OhioOutsideSpendingReportResult[] = [];
  const unmatchedTargets = new Map<string, OhioOutsideSpendingTargetDiagnostic>();
  const ambiguousTargets = new Map<string, OhioOutsideSpendingTargetDiagnostic>();
  let attributedRowCount = 0;
  let attributedCents = 0;
  let quarantinedReportCount = 0;

  for (const detailReport of input.detailReports) {
    const reportKey = detailReport.reportKey.trim();
    if (seenReportKeys.has(reportKey)) {
      throw new Error(`Duplicate Ohio 31-U detail report key: ${reportKey}`);
    }
    seenReportKeys.add(reportKey);

    const spender = spendersByReportKey.get(reportKey) ?? null;
    const reconciliation = reconcileOhioSos31uReport({
      reportKey,
      annualTotalCents: spender?.totalCents ?? 0,
      detailRows: detailReport.rows,
      toleranceCents,
    });
    const coverIeCents = coverIeByReportKey.get(reportKey) ?? null;

    let quarantineReason: OhioOutsideSpendingQuarantineReason | null = null;
    if (spender === null) {
      quarantineReason = "unknown_report_key";
    } else if (spender.invalid) {
      quarantineReason = "invalid_spender";
    } else if (!reconciliation.matches) {
      quarantineReason = "annual_detail_mismatch";
    } else if (coverIeCents !== null && Math.abs(coverIeCents - spender.totalCents) > toleranceCents) {
      quarantineReason = "cover_mismatch";
    }

    reports.push({
      reportKey,
      spenderCommitteeId: spender && !spender.invalid ? spender.committeeId : null,
      spenderCommitteeName: spender && !spender.invalid ? spender.committeeName : null,
      reconciliation,
      coverIeCents,
      quarantined: quarantineReason !== null,
      quarantineReason,
    });
    if (quarantineReason !== null) {
      quarantinedReportCount += 1;
      continue;
    }

    for (const row of detailReport.rows) {
      if (row.direction === null || row.amountCents === null || row.amountCents <= 0) {
        continue;
      }
      const matched = matchTarget(row);
      if (matched === "no_target" || matched.length === 0) {
        addTargetDiagnostic(
          unmatchedTargets,
          row.candidateNameOrBallotIssue?.trim() || "(blank)",
          row.amountCents
        );
        continue;
      }
      if (matched.length > 1) {
        addTargetDiagnostic(ambiguousTargets, row.candidateNameOrBallotIssue!.trim(), row.amountCents);
        continue;
      }

      const accumulator = matched[0]!;
      attributedRowCount += 1;
      attributedCents += row.amountCents;
      if (row.direction === "support") {
        accumulator.supportCents += row.amountCents;
      } else {
        accumulator.opposeCents += row.amountCents;
      }
      const groupKey = `${spender!.committeeId}\u0000${row.direction}`;
      const group = accumulator.groups.get(groupKey);
      if (group) {
        group.amountCents += row.amountCents;
      } else {
        accumulator.groups.set(groupKey, {
          committeeId: spender!.committeeId,
          committeeName: spender!.committeeName,
          supportOppose: row.direction,
          amountCents: row.amountCents,
        });
      }
    }
  }

  const missingDetailReportKeys = [...spendersByReportKey.keys()]
    .filter((reportKey) => !seenReportKeys.has(reportKey))
    .sort();

  const candidates = candidateAccumulators
    .filter((accumulator) => accumulator.groups.size > 0)
    .map((accumulator) => ({
      candidateKey: accumulator.target.candidateKey,
      supportTotal: centsToDollars(accumulator.supportCents),
      opposeTotal: centsToDollars(accumulator.opposeCents),
      groups: [...accumulator.groups.values()]
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
        })),
    }))
    .sort((left, right) => left.candidateKey.localeCompare(right.candidateKey));

  return {
    candidates,
    reports: reports.sort((left, right) => left.reportKey.localeCompare(right.reportKey)),
    missingDetailReportKeys,
    unmatchedTargets: [...unmatchedTargets.values()].sort((left, right) => right.amountCents - left.amountCents),
    ambiguousTargets: [...ambiguousTargets.values()].sort((left, right) => right.amountCents - left.amountCents),
    attributedRowCount,
    attributedCents,
    quarantinedReportCount,
    annualSkippedRowCount,
  };
}
