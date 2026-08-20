// PAC funder aggregation for Austin (plan Phase 3b): who gave to ONE outside
// spender inside one candidate's cycle window. Input is every Contributions
// row under the spender's exact name (`recipient`, window-bounded at the
// fetch) plus Report Detail facts for the reports those rows sit on.
//
// The plan sketched a PAC-form effective-report selection here, but the live
// receipt shapes (2026-08-19) match the DCE dataset, not the candidate cover
// reports, so this reuses the outside-spending recipe instead:
//   - regular PAC reports (GPAC semiannual/pre-election, MPAC monthly, SPAC)
//     carry DISJOINT periods per filer — nothing to select between;
//   - a CORPAC re-lists every receipt of its period (RECA 2024: the CORPAC
//     repeats its same-period MPAC, cover and rows; Vibrant Austin's CORPAC
//     10-28..12-04 re-lists rows from an ATX.1, a PACATX.7 AND a GPAC), so
//     rule 0 from austinOutsideSpendingAggregator applies: the latest-filed
//     correction per period wins, its period voids rows on non-correction
//     reports, losing corrections drop entirely;
//   - special reports (ATX.1 / PACATX.7 / ATX.8, no cover totals) re-list on
//     the next regular report (ANCHOR COULTER on an ATX.1 and the MPAC;
//     OUR FIGHT OUR FUTURE PAC on a PACATX.7, a GPAC and an ATX.8), so the
//     remaining rows collapse by (donor key, date, amount) across reports.
//     Two genuinely identical same-day gifts merge — an undercount, never an
//     overcount (the Phase 0 trade).
//
// Scope (the Houston/Texas funder precedent, and what the shared read side
// can actually show — donor rows surface only as industry evidence):
//   - pledges and misfiled expenditures are not money received;
//   - only ENTITY donors become donor rows: an individual's gift carries no
//     organization to attribute an industry to (their dollars are reported,
//     never dropped silently);
//   - an ENTITY whose name is itself a PAC/campaign committee is
//     intermediated money — no industry can honestly be read off the name
//     (Vibrant Austin's "Way To Lead PAC" rows), so those dollars are
//     excluded from donor rows too and reported separately;
//   - refunds net per donor; donors at or below net zero drop.

import { normalizeAustinFinanceTextKey } from "./austinFinanceKeys.js";
import { isAustinNonReceiptContributionType } from "./austinDirectFinanceAggregator.js";
import type { AustinReportFacts } from "./austinOutsideSpendingAggregator.js";
import type { AustinContributionRow } from "./austinSocrataClient.js";

/** Corrections of REGULAR reports — full re-lists of their period. */
const REGULAR_CORRECTION_FORM_CODES: ReadonlySet<string> = new Set(["CORPAC", "CORCOH"]);

/**
 * True when an organization name can carry an industry label — i.e. it is
 * not itself a political committee or campaign. The Houston regex set
 * (isHoustonIndustryEligibleOrganizationName), applied to the same problem.
 */
export function isAustinIndustryEligibleOrganizationName(value: string): boolean {
  const normalized = normalizeAustinFinanceTextKey(value);
  if (!normalized) return false;
  return !(
    /\bPAC\b/.test(normalized) ||
    /\bPOLITICAL ACTION (COMMITTEE|FUND)\b/.test(normalized) ||
    /\bCAMPAIGN(S| COMMITTEE| FUND)?\b/.test(normalized) ||
    /\b(FRIENDS|CITIZENS) (OF|FOR)\b/.test(normalized) ||
    /\bFOR (STATE |US |U S )?(REPRESENTATIVE|SENATE|SENATOR|CONGRESS|GOVERNOR|MAYOR)\b/.test(normalized)
  );
}

export type AustinPacFunderDonor = {
  /** First source spelling seen (rows arrive in transaction-id order). */
  donorName: string;
  donorKey: string;
  /** Net cents over the window — always positive here. */
  amountCents: number;
  /** Positive receipts behind the net figure. */
  receiptCount: number;
};

export type AustinPacFunderAggregation = {
  /** Net-positive ENTITY donors with industry-eligible names, amount desc. */
  donors: AustinPacFunderDonor[];
  /** Rows with a contribution date inside the window. */
  windowRowCount: number;
  /** Pledge / misfiled-expenditure rows set aside. */
  nonReceiptRowCount: number;
  /** Rows voided by a correction re-listing their period (rule 0). */
  supersededRowCount: number;
  /** Rows folded by the (donor, date, amount) dedupe (special re-lists). */
  duplicateReceiptCount: number;
  /** INDIVIDUAL / untyped donor money — out of funder scope, reported. */
  individualCents: number;
  /** ENTITY money under PAC/committee-shaped names — intermediated, reported. */
  ineligibleOrgCents: number;
  /** Σ donors[].amountCents. */
  entityDonorCents: number;
};

export function aggregateAustinPacFunders(input: {
  /** Contributions rows for the spender (`recipient` = its exact name). */
  contributions: readonly AustinContributionRow[];
  /** Report Detail facts for the reports behind those rows. */
  reportsById: ReadonlyMap<string, AustinReportFacts>;
  /** Inclusive ISO date window (the candidate's cycle window). */
  windowFrom: string;
  windowTo: string;
}): AustinPacFunderAggregation {
  if (input.windowFrom > input.windowTo)
    throw new Error(
      `Austin PAC funder window is inverted: ${input.windowFrom}..${input.windowTo}`,
    );
  const windowRows: AustinContributionRow[] = [];
  let windowRowCount = 0;
  let nonReceiptRowCount = 0;
  for (const row of input.contributions) {
    if (
      row.contributionDate === null ||
      row.contributionDate < input.windowFrom ||
      row.contributionDate > input.windowTo
    )
      continue;
    windowRowCount += 1;
    if (isAustinNonReceiptContributionType(row.contributionType)) {
      nonReceiptRowCount += 1;
      continue;
    }
    windowRows.push(row);
  }

  // Rule 0: latest-filed correction per period wins; its period voids rows
  // on non-correction reports; a losing correction drops entirely. One
  // filer here, so the key is the period alone.
  const correctionWinners = new Map<
    string,
    { reportId: string; dateFiled: string; periodFrom: string; periodTo: string }
  >();
  for (const row of windowRows) {
    const report = input.reportsById.get(row.reportId);
    if (
      !report ||
      !REGULAR_CORRECTION_FORM_CODES.has(report.formTypeCode) ||
      report.periodFrom === null ||
      report.periodTo === null
    )
      continue;
    const key = `${report.periodFrom}|${report.periodTo}`;
    const current = correctionWinners.get(key);
    if (
      !current ||
      current.dateFiled < report.dateFiled ||
      (current.dateFiled === report.dateFiled && current.reportId < row.reportId)
    )
      correctionWinners.set(key, {
        reportId: row.reportId,
        dateFiled: report.dateFiled,
        periodFrom: report.periodFrom,
        periodTo: report.periodTo,
      });
  }
  const winners = [...correctionWinners.values()];
  let supersededRowCount = 0;
  const seenReceipts = new Set<string>();
  let duplicateReceiptCount = 0;
  let individualCents = 0;
  let ineligibleOrgCents = 0;
  const donorMap = new Map<
    string,
    { donorName: string; amountCents: number; receiptCount: number }
  >();
  for (const row of windowRows) {
    if (winners.length > 0) {
      const report = input.reportsById.get(row.reportId);
      const isCorrection =
        report !== undefined && REGULAR_CORRECTION_FORM_CODES.has(report.formTypeCode);
      const superseded = isCorrection
        ? !winners.some((winner) => winner.reportId === row.reportId)
        : winners.some(
            (winner) =>
              winner.periodFrom <= row.contributionDate! &&
              row.contributionDate! <= winner.periodTo,
          );
      if (superseded) {
        supersededRowCount += 1;
        continue;
      }
    }
    const donorKey = normalizeAustinFinanceTextKey(row.donor);
    if (!donorKey) continue;
    const receiptKey = `${donorKey}|${row.contributionDate}|${row.amountCents}`;
    if (seenReceipts.has(receiptKey)) {
      duplicateReceiptCount += 1;
      continue;
    }
    seenReceipts.add(receiptKey);
    if (row.donorType !== "ENTITY") {
      individualCents += row.amountCents;
      continue;
    }
    if (!isAustinIndustryEligibleOrganizationName(row.donor)) {
      ineligibleOrgCents += row.amountCents;
      continue;
    }
    const donor = donorMap.get(donorKey) ?? {
      donorName: row.donor,
      amountCents: 0,
      receiptCount: 0,
    };
    donor.amountCents += row.amountCents;
    donor.receiptCount += row.amountCents > 0 ? 1 : 0;
    donorMap.set(donorKey, donor);
  }

  const donors: AustinPacFunderDonor[] = [...donorMap.entries()]
    .filter(([, donor]) => donor.amountCents > 0)
    .map(([donorKey, donor]) => ({ donorKey, ...donor }))
    .sort(
      (a, b) => b.amountCents - a.amountCents || a.donorName.localeCompare(b.donorName),
    );
  return {
    donors,
    windowRowCount,
    nonReceiptRowCount,
    supersededRowCount,
    duplicateReceiptCount,
    individualCents,
    ineligibleOrgCents,
    entityDonorCents: donors.reduce((sum, donor) => sum + donor.amountCents, 0),
  };
}
