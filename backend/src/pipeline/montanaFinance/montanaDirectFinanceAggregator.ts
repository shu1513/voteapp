// Montana direct-finance aggregation (docs/plans/montana-finance.md, Phase 2a).
//
// Sources, per the plan's surface assignments (all verified live 2026-08-28
// on Bedey's full 8-report cycle):
// - Totals: report-detail JSON lists over the canonical C5s, gated by the
//   cash-begin chain. directContributionTotal = individual + committee +
//   candidate lists (totalAmt: in-kind counts as dollars) + the chain's
//   derived unitemized lump. totalDisbursements = expendOther + pettyCash
//   (campaign spending; the `payment` list is debt/loan REPAYMENT — cash out
//   of the bank for the chain, but not spending, and CERS's own EXPEND
//   export excludes it). cashOnHand = chain-derived ending balance.
// - Occupation breakdown: CONTR CSV export, Individual Contributions rows
//   only (100% fill verified; filed values only).
// - Contribution sizes: report-detail JSON individual rows (the CSV's Date
//   Paid is synthetic, and the JSON rows are the entry-level records).
//
// Cross-checks (fail closed): the CSV and JSON surfaces must agree to the
// cent on individual and committee contributions, and the EXPEND CSV must
// equal expendOther + pettyCash — a mismatch means a mixed harvest or a
// definitional drift, and no snapshot may be written from it.

import type {
  MontanaCersExportRow,
  MontanaCersReportDetailArtifact,
  MontanaCersReportInventoryRow,
} from "./montanaCersParsers.js";
import {
  computeMontanaReportCashFlows,
  reconcileMontanaCashBeginChain,
  type MontanaChainReconciliation,
} from "./montanaChainReconciliation.js";

export const MONTANA_UNKNOWN_OCCUPATION_LABEL = "Unknown";

const CSV_INDIVIDUAL_LINE_ITEM = "Individual Contributions";
const CSV_COMMITTEE_LINE_ITEM = "Independent Committee Contributions";
/** CSV roll-up family for small itemized contributions (observed on Eddy). */
const CSV_SMALL_CONTRIBUTION_LINE_ITEM = "Contributions Less Than $35 Each";
/** Montana's cumulative itemization threshold (MCA 13-37-229: $50). */
const SMALL_CONTRIBUTION_ROW_CENTS = 5_000;

/**
 * Inter-side fund transfers are booked as ordinary expenditures (verified
 * live 2026-08-28, Eddy / Supreme Court: an expendOther Primary row
 * "Transfer of primary funds to general, no primary" paid to her bank, with
 * the near-matching amount arriving in `refunds` on the General side). They
 * are real cash flows for the chain but NOT campaign spending, so both
 * surfaces partition them out of "spent" by this purpose test. A transfer
 * worded outside this pattern would count as spending on BOTH surfaces
 * (cross-checks still pass; totals overstate) — the Phase 3 spot check
 * covers that residual risk.
 */
export function isMontanaSideTransferPurpose(purpose: string | null): boolean {
  return purpose !== null && /\btransfer\b/i.test(purpose) && /\b(?:primary|general)\b/i.test(purpose);
}

export type MontanaCanonicalReportWithDetail = {
  inventory: MontanaCersReportInventoryRow;
  artifact: MontanaCersReportDetailArtifact;
};

export type MontanaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MontanaDirectFinanceAggregationResult = {
  chain: MontanaChainReconciliation;
  /** Dollars. Individual + committee + candidate lists + derived lump. */
  directContributionTotal: number;
  /** Dollars. expendOther + pettyCash lists (campaign spending). */
  totalDisbursements: number;
  /** Dollars. Chain-derived ending balance of the latest canonical report. */
  cashOnHand: number;
  /** Dollars. Chain-residual unitemized small-donor money. */
  derivedUnitemizedTotal: number;
  /** Dollars. Debt/loan repayments (the `payment` list) — diagnostic only. */
  debtRepaymentTotal: number;
  /** Dollars. Inter-side fund transfers excluded from spending — diagnostic only. */
  sideTransferTotal: number;
  /** Dollars. Cash loan proceeds — diagnostic only, excluded from raised. */
  loanProceedsTotal: number;
  directBreakdowns: MontanaFinanceDirectBreakdown[];
  canonicalReportCount: number;
  individualRowCount: number;
  committeeRowCount: number;
  candidateRowCount: number;
};

export class MontanaDirectFinanceAggregationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MontanaDirectFinanceAggregationError";
  }
}

function keyText(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ").toLocaleUpperCase("en-US");
}

function sizeBucket(amountCents: number): string {
  if (amountCents < 10_000) return "$1-$99";
  if (amountCents < 25_000) return "$100-$249";
  if (amountCents < 50_000) return "$250-$499";
  if (amountCents < 100_000) return "$500-$999";
  if (amountCents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

type Aggregate = {
  categoryType: MontanaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributors: Set<string>;
};

function addAggregate(
  aggregates: Map<string, Aggregate>,
  categoryType: Aggregate["categoryType"],
  categoryName: string,
  amountCents: number,
  contributor: string
): void {
  const key = `${categoryType}\u0000${keyText(categoryName)}`;
  const aggregate = aggregates.get(key);
  if (aggregate) {
    aggregate.amountCents += amountCents;
    aggregate.contributors.add(contributor);
  } else {
    aggregates.set(key, { categoryType, categoryName, amountCents, contributors: new Set([contributor]) });
  }
}

function requireCentAgreement(label: string, csvCents: number, jsonCents: number): void {
  if (csvCents !== jsonCents) {
    throw new MontanaDirectFinanceAggregationError(
      `Montana CSV/JSON ${label} totals disagree: CSV ${csvCents}c vs report-detail ${jsonCents}c — mixed harvest or definitional drift`
    );
  }
}

export function aggregateMontanaDirectFinance(input: {
  canonicalReports: readonly MontanaCanonicalReportWithDetail[];
  contributionRows: readonly MontanaCersExportRow[];
  expenditureRows: readonly MontanaCersExportRow[];
  sourceUrl?: string | null;
  maxOccupationBreakdowns?: number;
}): MontanaDirectFinanceAggregationResult {
  const maxOccupationBreakdowns = input.maxOccupationBreakdowns ?? 50;
  if (!Number.isSafeInteger(maxOccupationBreakdowns) || maxOccupationBreakdowns <= 0) {
    throw new MontanaDirectFinanceAggregationError(
      `Invalid Montana occupation breakdown limit: ${input.maxOccupationBreakdowns}`
    );
  }
  if (input.canonicalReports.length === 0) {
    throw new MontanaDirectFinanceAggregationError("Montana aggregation requires at least one canonical report");
  }
  for (const report of input.canonicalReports) {
    if (report.inventory.reportId !== report.artifact.reportId) {
      throw new MontanaDirectFinanceAggregationError(
        `Montana report detail ${report.artifact.reportId} paired with inventory row ${report.inventory.reportId}`
      );
    }
  }

  // Chain gate first: no chain, no snapshot.
  const chain = reconcileMontanaCashBeginChain(
    input.canonicalReports.map((report) => ({
      inventory: report.inventory,
      flows: computeMontanaReportCashFlows(report.artifact),
    }))
  );
  if (!chain.ok || chain.derivedEndingBalanceCents === null || chain.derivedUnitemizedTotalCents === null) {
    const broken = chain.links.filter((link) => !link.ok);
    throw new MontanaDirectFinanceAggregationError(
      `Montana cash-begin chain failed: ${
        broken.length > 0
          ? broken
              .map((link) => `${link.side} ${link.reportId}->${link.nextReportId} lump ${link.lumpCents}c (${link.failure})`)
              .join("; ")
          : "missing begin anchors"
      }`
    );
  }

  // JSON detail sums. Candidate reports must not carry electioneering rows —
  // that flag belongs to committee filings, so its presence here is drift.
  let individualCents = 0;
  let committeeCents = 0;
  let candidateCents = 0;
  let spendingCents = 0;
  let sideTransferCents = 0;
  let debtRepaymentCents = 0;
  let loanProceedsCents = 0;
  let individualRowCount = 0;
  let committeeRowCount = 0;
  let candidateRowCount = 0;
  const aggregates = new Map<string, Aggregate>();
  let anonymousSizeRowIndex = 0;
  for (const report of input.canonicalReports) {
    for (const [listName, rows] of Object.entries(report.artifact.lists)) {
      for (const row of rows) {
        if (row.electioneeringInd === "Y") {
          throw new MontanaDirectFinanceAggregationError(
            `Montana candidate report ${report.artifact.reportId} has an electioneering row in list ${listName}`
          );
        }
      }
    }
    for (const row of report.artifact.lists.individual) {
      individualCents += row.totalAmtCents;
      individualRowCount += 1;
      if (row.totalAmtCents > 0) {
        anonymousSizeRowIndex += 1;
        const contributor = keyText(row.entityName) || `UNKNOWN-${anonymousSizeRowIndex}`;
        addAggregate(aggregates, "contribution_size", sizeBucket(row.totalAmtCents), row.totalAmtCents, contributor);
      }
    }
    for (const row of report.artifact.lists.committee) {
      committeeCents += row.totalAmtCents;
      committeeRowCount += 1;
    }
    for (const row of report.artifact.lists.candidate) {
      candidateCents += row.totalAmtCents;
      candidateRowCount += 1;
    }
    for (const row of [...report.artifact.lists.expendOther, ...report.artifact.lists.pettyCash]) {
      if (isMontanaSideTransferPurpose(row.purposeDescr)) {
        sideTransferCents += row.totalAmtCents;
      } else {
        spendingCents += row.totalAmtCents;
      }
    }
    for (const row of report.artifact.lists.payment) {
      debtRepaymentCents += row.totalAmtCents;
    }
    for (const row of report.artifact.lists.loan) {
      loanProceedsCents += row.cashAmtCents;
    }
  }

  // CSV cross-checks. Committee totals agree to the cent on both probed
  // filers. Individual totals need a threshold-aware bound: the public CSV
  // applies Montana's cumulative itemization threshold, so sub-threshold
  // entries that the report-detail JSON itemizes are dropped or rolled into
  // the "Contributions Less Than $35 Each" family (verified live 2026-08-28,
  // Eddy: CSV short by $12,916.81, fully explained by her sub-$50 rows;
  // Bedey's CSV is complete and the difference is zero). The CSV may only
  // ever be SMALLER, and only by money attributable to small rows.
  let csvIndividualCents = 0;
  let csvCommitteeCents = 0;
  for (const row of input.contributionRows) {
    if (row.lineItem === CSV_INDIVIDUAL_LINE_ITEM || row.lineItem === CSV_SMALL_CONTRIBUTION_LINE_ITEM) {
      csvIndividualCents += row.amountCents;
    } else if (row.lineItem === CSV_COMMITTEE_LINE_ITEM) {
      csvCommitteeCents += row.amountCents;
    }
  }
  const smallIndividualRowBudgetCents = input.canonicalReports.reduce(
    (sum, report) =>
      sum +
      report.artifact.lists.individual.reduce(
        (listSum, row) =>
          row.totalAmtCents > 0 && row.totalAmtCents < SMALL_CONTRIBUTION_ROW_CENTS
            ? listSum + row.totalAmtCents
            : listSum,
        0
      ),
    0
  );
  const individualShortfallCents = individualCents - csvIndividualCents;
  if (individualShortfallCents < 0 || individualShortfallCents > smallIndividualRowBudgetCents) {
    throw new MontanaDirectFinanceAggregationError(
      `Montana CSV/JSON individual-contribution totals disagree beyond the itemization threshold: ` +
        `CSV ${csvIndividualCents}c vs report-detail ${individualCents}c ` +
        `(shortfall ${individualShortfallCents}c, small-row budget ${smallIndividualRowBudgetCents}c)`
    );
  }
  requireCentAgreement("committee-contribution", csvCommitteeCents, committeeCents);
  // Expenditure cross-check with the same transfer partition on both
  // surfaces: campaign spending must agree to the cent, and any transfer
  // rows the CSV carries must equal the JSON transfer rows (a CSV without
  // them — the observed shape — contributes zero).
  let csvSpendingCents = 0;
  let csvTransferCents = 0;
  for (const row of input.expenditureRows) {
    if (isMontanaSideTransferPurpose(row.purpose)) {
      csvTransferCents += row.amountCents;
    } else {
      csvSpendingCents += row.amountCents;
    }
  }
  requireCentAgreement("expenditure", csvSpendingCents, spendingCents);
  if (csvTransferCents !== 0 && csvTransferCents !== sideTransferCents) {
    throw new MontanaDirectFinanceAggregationError(
      `Montana CSV/JSON side-transfer totals disagree: CSV ${csvTransferCents}c vs report-detail ${sideTransferCents}c`
    );
  }

  // Occupation breakdown from the CSV's individual rows (filed values only).
  for (const [index, row] of input.contributionRows.entries()) {
    if (row.lineItem !== CSV_INDIVIDUAL_LINE_ITEM || row.amountCents <= 0) {
      continue;
    }
    const occupation =
      row.occupation === null || keyText(row.occupation) === "UNKNOWN"
        ? MONTANA_UNKNOWN_OCCUPATION_LABEL
        : row.occupation;
    const contributor = keyText(row.entityName) || `UNKNOWN-CSV-${index}`;
    addAggregate(aggregates, "occupation", occupation, row.amountCents, contributor);
  }

  const byType = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of aggregates.values()) {
    const rows = byType.get(aggregate.categoryType) ?? [];
    rows.push(aggregate);
    byType.set(aggregate.categoryType, rows);
  }
  const directBreakdowns: MontanaFinanceDirectBreakdown[] = [];
  for (const type of ["occupation", "contribution_size"] as const) {
    const limit = type === "occupation" ? maxOccupationBreakdowns : Number.POSITIVE_INFINITY;
    for (const aggregate of (byType.get(type) ?? [])
      .sort((a, b) => b.amountCents - a.amountCents || a.categoryName.localeCompare(b.categoryName))
      .slice(0, limit)) {
      directBreakdowns.push({
        categoryType: type,
        categoryName: aggregate.categoryName,
        amount: aggregate.amountCents / 100,
        contributorCount: aggregate.contributors.size,
        sourceUrl: input.sourceUrl ?? null,
      });
    }
  }

  return {
    chain,
    directContributionTotal:
      (individualCents + committeeCents + candidateCents + chain.derivedUnitemizedTotalCents) / 100,
    totalDisbursements: spendingCents / 100,
    cashOnHand: chain.derivedEndingBalanceCents / 100,
    derivedUnitemizedTotal: chain.derivedUnitemizedTotalCents / 100,
    debtRepaymentTotal: debtRepaymentCents / 100,
    sideTransferTotal: sideTransferCents / 100,
    loanProceedsTotal: loanProceedsCents / 100,
    directBreakdowns,
    canonicalReportCount: input.canonicalReports.length,
    individualRowCount,
    committeeRowCount,
    candidateRowCount,
  };
}
