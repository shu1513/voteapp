import {
  readErtsArtifact,
  readErtsTextArtifact,
} from "./rhodeIslandErtsArtifactCache.js";
import {
  ertsPdfGuidFromUrl,
  selectErtsCycleCf2Filings,
  type ErtsCycleFilingSelection,
} from "./rhodeIslandErtsArtifactAcquisition.js";
import {
  ertsUsDateToIso,
  extractErtsPdfPageItems,
  parseErtsFilingListPage,
  parseErtsFilingVersionsPage,
  readErtsCf2SummaryValues,
  ERTS_CF2_SUMMARY_LABELS,
} from "./rhodeIslandErtsParsers.js";

// CF-2 report selection for Rhode Island (rhode_island_plan.md decisions 2
// and 4). Cycle totals are per-period CF-2 sums, never "the latest CF-2":
// this module picks the authoritative (in-force) version of every CF-2 family
// whose reporting period overlaps the cycle, reads the pinned summary labels
// off its cached text-layer PDF, and derives each period's cash receipts and
// disbursements from the form's own arithmetic. Decision 4 is baked in — the
// public transaction data is current-ledger state (spike result 5, 5/5
// families), so the in-force version is simply the LAST row of the filing's
// `grdAmendments` list (oldest-first, spike-confirmed) and no original-vs-
// amended transaction disambiguation exists here.
//
// Cache only: everything reads artifacts the acquisition installed; nothing
// touches the portal. Fail-closed: an ambiguous or arithmetically unusable
// period QUARANTINES the organization (reasons reported, totals withheld)
// instead of publishing a guess.

// CF-2 page-1 label semantics, proven against real spike PDFs (McKee Q4 2025,
// hand-checked 2026-08-14): the form's SUMMARY OF ACTIVITY FOR PERIOD reads
//   3. Total Cash          = 1. Beginning Cash Balance + all cash receipts
//                            (returned contributions/checks print as
//                            parenthesized negatives inside the receipts)
//   5. Ending Cash Balance = 3. Total Cash - 4. Cash Disbursements
// so cash receipts = TotalCash - Beginning and disbursements = TotalCash -
// Ending, both exact integer-cent arithmetic on pinned labels. In-kind is
// reported on its own line 6 and is NOT part of cash receipts.
export const ERTS_CF2_BEGINNING_CASH_LABEL = "1. Beginning Cash Balance";
export const ERTS_CF2_TOTAL_CASH_LABEL = "3. Total Cash";
export const ERTS_CF2_ENDING_CASH_LABEL = "5. Ending Cash Balance";

export type RhodeIslandCf2PeriodValues = {
  filingId: string;
  reportType: string;
  beginIso: string;
  endIso: string;
  // Version-list provenance of the in-force version this period's values
  // came from ("" on an original, "Amended" on an amendment).
  amendmentLabel: string;
  filedAt: string;
  pdfUrl: string;
  versionCount: number;
  // Every pinned CF-2 page-1 label, in cents (readErtsCf2SummaryValues).
  values: Map<string, number>;
  beginningCashCents: number;
  totalCashCents: number;
  endingCashCents: number;
  // Derived: TotalCash - Beginning. Includes loans, interest, public funds
  // and negative return lines — the CF-2's own cash-receipts arithmetic.
  cashReceiptsCents: number;
  // Derived: TotalCash - Ending.
  disbursementsCents: number;
};

export type RhodeIslandCf2QuarantineReason = {
  reason:
    | "missing_cf2_label"
    | "duplicate_period_window"
    | "overlapping_periods"
    | "period_outside_cycle"
    | "unusable_period_window";
  detail: string;
};

export type RhodeIslandCf2CycleTotals = {
  totalReceiptsCents: number;
  totalDisbursementsCents: number;
  // The latest period's `5. Ending Cash Balance` — signed
  // (allowNegativeCashOnHand; RI CF-2s carry liabilities).
  cashOnHandCents: number;
  // Period end of the CF-2 the cash-on-hand value came from.
  cashOnHandAsOfIso: string;
};

export type RhodeIslandCf2CycleSelection = {
  orgId: string;
  cycleBeginIso: string;
  cycleEndIso: string;
  // Authoritative periods sorted by period begin. Populated even when
  // quarantined, so diagnostics can show what was read.
  periods: RhodeIslandCf2PeriodValues[];
  // Non-empty means the organization's totals must not publish this run.
  quarantineReasons: RhodeIslandCf2QuarantineReason[];
  // Filing-list triage counts (unfiled / non-CF-2 / out-of-cycle rows).
  filingSelection: ErtsCycleFilingSelection;
  // Null when there is no publishable arithmetic: no CF-2 period in the
  // cycle (a CF-5 deferral is not a zero — decision 12) or any quarantine.
  cycleTotals: RhodeIslandCf2CycleTotals | null;
};

function requireIsoDate(value: string, label: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Rhode Island CF-2 selection ${label} is not an ISO date: ${JSON.stringify(value)}`);
  }
  return value;
}

/**
 * Pairwise period-overlap check (decision 2: "an overlap quarantines the
 * org, never double-counts"). Periods are closed date ranges; two periods
 * overlap when neither ends before the other begins. Exact duplicates are
 * reported separately — two distinct CF-2 families claiming the same window
 * is an ambiguous lineage, and the authoritative one cannot be guessed.
 */
export function findRhodeIslandCf2PeriodConflicts(
  periods: readonly { filingId: string; beginIso: string; endIso: string }[]
): RhodeIslandCf2QuarantineReason[] {
  const reasons: RhodeIslandCf2QuarantineReason[] = [];
  for (let left = 0; left < periods.length; left += 1) {
    for (let right = left + 1; right < periods.length; right += 1) {
      const a = periods[left];
      const b = periods[right];
      if (a.beginIso === b.beginIso && a.endIso === b.endIso) {
        reasons.push({
          reason: "duplicate_period_window",
          detail: `filings ${a.filingId} and ${b.filingId} both claim ${a.beginIso}..${a.endIso}`,
        });
        continue;
      }
      if (a.beginIso <= b.endIso && b.beginIso <= a.endIso) {
        reasons.push({
          reason: "overlapping_periods",
          detail: `filing ${a.filingId} (${a.beginIso}..${a.endIso}) overlaps filing ${b.filingId} (${b.beginIso}..${b.endIso})`,
        });
      }
    }
  }
  return reasons;
}

/**
 * Compute cycle totals from non-conflicting periods (decision 2):
 * total receipts and disbursements are sums of each period's CF-2
 * arithmetic; cash on hand is the LATEST period's ending balance with its
 * period end as the as-of evidence.
 */
export function computeRhodeIslandCf2CycleTotals(
  periods: readonly RhodeIslandCf2PeriodValues[]
): RhodeIslandCf2CycleTotals | null {
  if (periods.length === 0) {
    return null;
  }
  let latest = periods[0];
  let totalReceiptsCents = 0;
  let totalDisbursementsCents = 0;
  for (const period of periods) {
    totalReceiptsCents += period.cashReceiptsCents;
    totalDisbursementsCents += period.disbursementsCents;
    if (period.endIso > latest.endIso) {
      latest = period;
    }
  }
  return {
    totalReceiptsCents,
    totalDisbursementsCents,
    cashOnHandCents: latest.endingCashCents,
    cashOnHandAsOfIso: latest.endIso,
  };
}

/**
 * Select the cycle's authoritative CF-2 periods from cached artifacts.
 * Reads: the organization filing list, each in-cycle CF-2 family's version
 * list, and the in-force version's PDF. A missing or stale artifact throws
 * (the cache module's fail-closed contract) — the caller isolates the
 * organization; a defect in the DATA (missing label, overlapping periods)
 * quarantines instead, so the run can report it.
 */
export async function selectRhodeIslandCf2CyclePeriods(input: {
  cacheDir: string;
  orgId: string;
  cycleBeginIso: string;
  cycleEndIso: string;
}): Promise<RhodeIslandCf2CycleSelection> {
  const cycleBeginIso = requireIsoDate(input.cycleBeginIso, "cycle begin");
  const cycleEndIso = requireIsoDate(input.cycleEndIso, "cycle end");
  if (cycleBeginIso > cycleEndIso) {
    throw new Error(`Rhode Island CF-2 selection cycle window is inverted: ${cycleBeginIso}..${cycleEndIso}`);
  }

  const filings = await readErtsTextArtifact({
    cacheDir: input.cacheDir,
    key: { type: "organization_filings", orgId: input.orgId },
  });
  const filingSelection = selectErtsCycleCf2Filings({
    rows: parseErtsFilingListPage(filings.text),
    cycleBeginIso,
    cycleEndIso,
  });

  const periods: RhodeIslandCf2PeriodValues[] = [];
  const quarantineReasons: RhodeIslandCf2QuarantineReason[] = [];
  // A filed CF-2 row whose period is regex-shaped but not a real calendar
  // date (the filing-list parser only pins MM/DD/YYYY digits) is a reporting
  // period this selection cannot place — publishing the remaining periods
  // would silently understate the cycle.
  if (filingSelection.unusablePeriodRowCount > 0) {
    quarantineReasons.push({
      reason: "unusable_period_window",
      detail:
        `${filingSelection.unusablePeriodRowCount} filed CF-2 row(s) carry a period that does not parse as ` +
        "a calendar date — the cycle's period set is incomplete",
    });
  }
  for (const row of filingSelection.selected) {
    const filingId = row.filingId as string;
    const versionsArtifact = await readErtsTextArtifact({
      cacheDir: input.cacheDir,
      key: { type: "filing_versions", filingId },
    });
    const versions = parseErtsFilingVersionsPage(versionsArtifact.text);
    // Oldest-first (spike-confirmed): the last row is the in-force version.
    const inForce = versions[versions.length - 1];
    const pdf = await readErtsArtifact({
      cacheDir: input.cacheDir,
      key: { type: "filing_pdf", filingId, guid: ertsPdfGuidFromUrl(inForce.pdfUrl) },
    });
    const values = readErtsCf2SummaryValues(await extractErtsPdfPageItems(new Uint8Array(pdf.bytes)), ERTS_CF2_SUMMARY_LABELS);

    // ERTS generates every CF-2 PDF from one form layout; a pinned label the
    // extraction cannot find is layout drift, and a silently skipped label
    // would silently weaken the reconciliation the totals depend on.
    const missing = ERTS_CF2_SUMMARY_LABELS.filter((label) => !values.has(label));
    if (missing.length > 0) {
      quarantineReasons.push({
        reason: "missing_cf2_label",
        detail: `filing ${filingId} (${row.periodBegin}..${row.periodEnd}) PDF did not yield: ${missing.join(", ")}`,
      });
      continue;
    }

    const beginIso = ertsUsDateToIso(row.periodBegin) as string;
    const endIso = ertsUsDateToIso(row.periodEnd) as string;
    // The selection window is overlap-based, so a period could straddle the
    // cycle boundary; summing a straddling CF-2 would count another cycle's
    // money. RI periods are cycle-aligned in practice — a straddle is a
    // finding, not a case to apportion.
    if (beginIso < cycleBeginIso || endIso > cycleEndIso) {
      quarantineReasons.push({
        reason: "period_outside_cycle",
        detail: `filing ${filingId} period ${beginIso}..${endIso} extends beyond the cycle ${cycleBeginIso}..${cycleEndIso}`,
      });
      continue;
    }

    const beginningCashCents = values.get(ERTS_CF2_BEGINNING_CASH_LABEL) as number;
    const totalCashCents = values.get(ERTS_CF2_TOTAL_CASH_LABEL) as number;
    const endingCashCents = values.get(ERTS_CF2_ENDING_CASH_LABEL) as number;
    periods.push({
      filingId,
      reportType: row.reportType,
      beginIso,
      endIso,
      amendmentLabel: inForce.amendmentLabel,
      filedAt: inForce.filedAt,
      pdfUrl: inForce.pdfUrl,
      versionCount: versions.length,
      values,
      beginningCashCents,
      totalCashCents,
      endingCashCents,
      cashReceiptsCents: totalCashCents - beginningCashCents,
      disbursementsCents: totalCashCents - endingCashCents,
    });
  }

  periods.sort((a, b) => (a.beginIso < b.beginIso ? -1 : a.beginIso > b.beginIso ? 1 : 0));
  quarantineReasons.push(...findRhodeIslandCf2PeriodConflicts(periods));

  return {
    orgId: input.orgId,
    cycleBeginIso,
    cycleEndIso,
    periods,
    quarantineReasons,
    filingSelection,
    cycleTotals: quarantineReasons.length === 0 ? computeRhodeIslandCf2CycleTotals(periods) : null,
  };
}
