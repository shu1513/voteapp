// Outside-spending aggregation for Austin (plan Phase 3, gotchas 3-5). Input
// is the whole Direct Campaign Expenditure (DCE) dataset and the whole
// Committee Purpose dataset (both a few hundred rows, fetched once per batch
// run) plus one linked candidate's identity: display name, exact filer
// name, office code, election date, and the cycle window.
//
// A DCE row is one (payment × target): a $71,000 mailer naming five
// candidates is five rows, an ATX.1 special report's rows reappear on the
// next regular report, and a corrected report re-lists its period under a
// new report id. PAC report periods do not nest (live: an ATX.1 08-16..10-03
// re-listed on a GPAC 08-16..09-27; a CORPAC 10-28..12-04 covering an 8th-day
// report, three ATX.1s and a GPAC 11-05..12-04), so a Houston-style
// effective-report selection cannot work here. Instead:
//   0. CORRECTION SUPERSESSION, per spender: a correction of a regular PAC
//      report (CORPAC; CORCOH for a candidate filer) re-lists every payment
//      of its period (verified live on Vibrant Austin PAC 2024: all 31 rows
//      of its three ATX.1s and its GPAC reappear on the CORPAC), so rows of
//      the same spender on NON-correction reports whose payment date falls
//      inside a correction's period are dropped — that is what makes a
//      correction that CHANGED a date or amount count once, not twice
//      (PR #763 review). Two corrections of one period keep the latest
//      filing. A correction of a special (CORATX1) lists only the special's
//      payments, so it supersedes nothing here and rides the dedupe below.
//      Report periods come from Report Detail (reportsById); a row whose
//      report is unknown is kept (dedupe only);
//   1. the remaining rows inside the cycle window collapse into ECONOMIC
//      PAYMENTS by (spender key, payee key, payment date, amount) — across
//      reports, which is what folds ATX.1 re-listings. Two genuinely
//      identical same-day payments merge (live: a run of $2 Meta charges on
//      one report) — an undercount, never an overcount — the Phase 0 trade;
//   2. a payment NAMES this candidate when one of its rows carries the
//      candidate's office code (leading token of `office_sought_info`) AND a
//      target that name-matches the roster name under the shared gates —
//      tried in both comma orders, because targets arrive as "Last, First"
//      and occasionally "FIRST, LAST" (Brown D1 2026). A row with no
//      parsable office code can never name a candidate (fail closed);
//   3. SELF: a payment whose spender is the candidate (name-matches, or is
//      the linked filer string) is the campaign's own spending, already in
//      `expend_total` — dropped (counted when it names the candidate; the
//      office gate does not apply, those rows usually carry no office);
//   4. D6 (georgiaOutsideSpendingAggregator): only a payment with EXACTLY
//      ONE distinct target allocates, and it allocates its full amount.
//      Multi-target payments are quarantined and reported as excluded
//      dollars, never attributed in full to every target and never split by
//      guess;
//   5. DIRECTION comes from the city Committee Purpose dataset — the
//      spender's own SUPPORT/OPPOSE declaration for this candidate. A
//      purpose row applies when it is a CANDIDATE purpose by the same filer
//      key naming this candidate ("First,Last" there — swapped before the
//      gates), its office code does not CONTRADICT the link (53 live rows
//      say OTHER; a blank is silence, not a conflict), and it is THIS
//      cycle's declaration: an election-dated row must carry the link's
//      date; a blank-dated row (92 of 220 live) is accepted only when the
//      PAC report it was filed on has a period overlapping the candidate's
//      cycle window — otherwise a 2022 "SUPPORT Qadri" would label 2026
//      spending (PR #763 review). ASSIST is officeholder help, not
//      electioneering. One direction → used; both → ambiguous; none →
//      undirected. The outside tables cannot hold an undirected group
//      (`support_oppose` is NOT NULL), so those dollars are reported as
//      excluded and the loader's coverage note says so. (The plan's TEC
//      purpose fallback for TEC-filed general-purpose committees is a
//      follow-up; nothing here guesses direction from a committee's name.)

import { austinPersonNameMatchesCandidate } from "./austinCandidateFilerResolver.js";
import { normalizeAustinFinanceTextKey } from "./austinFinanceKeys.js";
import {
  parseAustinOfficeSoughtCode,
  type AustinOfficeCode,
} from "./austinFinanceEligibleOffices.js";
import type { AustinOutsideGroupInput } from "./austinFinanceWriter.js";
import type {
  AustinCommitteePurposeRow,
  AustinDirectCampaignExpenditureRow,
  AustinReportDetailRow,
} from "./austinSocrataClient.js";

type Direction = "support" | "oppose";

/** Form type + reporting period of the reports behind DCE / purpose rows. */
export type AustinReportFacts = Pick<
  AustinReportDetailRow,
  "formTypeCode" | "periodFrom" | "periodTo" | "dateFiled"
>;

/** Corrections of REGULAR reports — full re-lists of their period. */
const REGULAR_CORRECTION_FORM_CODES: ReadonlySet<string> = new Set(["CORPAC", "CORCOH"]);

function periodOverlaps(
  report: AustinReportFacts | undefined,
  windowFrom: string,
  windowTo: string,
): boolean {
  return (
    report !== undefined &&
    report.periodFrom !== null &&
    report.periodTo !== null &&
    report.periodFrom <= windowTo &&
    report.periodTo >= windowFrom
  );
}

/** "First,Last" ↔ "Last, First": swap the two sides of the first comma. */
export function swapAustinCommaName(value: string): string | null {
  const commaIndex = value.indexOf(",");
  if (commaIndex <= 0) return null;
  const left = value.slice(0, commaIndex).trim();
  const right = value.slice(commaIndex + 1).trim();
  if (!left || !right) return null;
  return `${right}, ${left}`;
}

/** Person-name gate in either comma order. */
function namesCandidate(value: string, candidateDisplayName: string): boolean {
  if (austinPersonNameMatchesCandidate(value, candidateDisplayName)) return true;
  const swapped = swapAustinCommaName(value);
  return swapped !== null && austinPersonNameMatchesCandidate(swapped, candidateDisplayName);
}

/**
 * Order-insensitive identity for counting DISTINCT targets on one payment:
 * "BROWN, STEVEN" and "STEVEN, BROWN" are one target, "Prop Q, Prop Q" is
 * one target. Deliberately loose — a spelling drift across an original and
 * its correction looks like two targets and quarantines the payment (an
 * undercount, never an overcount).
 */
function targetKey(value: string): string {
  return value
    .split(",")
    .map((part) => normalizeAustinFinanceTextKey(part))
    .filter(Boolean)
    .sort()
    .join(" ");
}

type EconomicPayment = {
  spenderKey: string;
  spenderName: string;
  amountCents: number;
  rows: AustinDirectCampaignExpenditureRow[];
  targetKeys: Set<string>;
};

export type AustinOutsideAggregation = {
  groups: AustinOutsideGroupInput[];
  supportTotalCents: number;
  opposeTotalCents: number;
  /** DCE rows whose payment date lies inside the cycle window. */
  windowRowCount: number;
  /** Rows dropped because `paid_by` is blank (spender unknown). */
  rowsWithoutSpender: number;
  /** Rows dropped because a correction re-lists their period (rule 0). */
  supersededRowCount: number;
  paymentCount: number;
  /** Payments naming this candidate, by outcome. */
  attributedPaymentCount: number;
  selfPaymentCount: number;
  selfCents: number;
  multiTargetPaymentCount: number;
  multiTargetCents: number;
  undirectedCents: number;
  undirectedSpenders: string[];
  ambiguousDirectionCents: number;
  ambiguousDirectionSpenders: string[];
};

/**
 * Committee Purpose direction per spender key for one candidate/election/
 * office. Exported for tests and the sync's diagnostics.
 */
export function austinCommitteeDirections(input: {
  purposeRows: readonly AustinCommitteePurposeRow[];
  reportsById: ReadonlyMap<string, AustinReportFacts>;
  candidateDisplayName: string;
  officeCode: AustinOfficeCode;
  electionDate: string;
  windowFrom: string;
  windowTo: string;
}): Map<string, Direction | "ambiguous"> {
  const directions = new Map<string, Direction | "ambiguous">();
  for (const row of input.purposeRows) {
    if (row.purposeType !== "CANDIDATE" || row.filerName === null || row.recipient === null)
      continue;
    const direction: Direction | null =
      row.committeeActivity === "SUPPORT"
        ? "support"
        : row.committeeActivity === "OPPOSE"
          ? "oppose"
          : null;
    if (direction === null) continue;
    if (row.electionDate !== null) {
      if (row.electionDate !== input.electionDate) continue;
    } else if (
      row.reportId === null ||
      !periodOverlaps(input.reportsById.get(row.reportId), input.windowFrom, input.windowTo)
    ) {
      continue;
    }
    const rowOffice = parseAustinOfficeSoughtCode(row.officeSought);
    if (rowOffice !== null && rowOffice !== input.officeCode) continue;
    // Recipient is "First,Last"; the gate reads comma form as "Last, First".
    const swapped = swapAustinCommaName(row.recipient);
    if (swapped === null || !austinPersonNameMatchesCandidate(swapped, input.candidateDisplayName))
      continue;
    const key = normalizeAustinFinanceTextKey(row.filerName);
    if (!key) continue;
    const existing = directions.get(key);
    if (existing === undefined) directions.set(key, direction);
    else if (existing !== direction) directions.set(key, "ambiguous");
  }
  return directions;
}

export function aggregateAustinOutsideSpending(input: {
  dceRows: readonly AustinDirectCampaignExpenditureRow[];
  purposeRows: readonly AustinCommitteePurposeRow[];
  /** Report Detail facts for the reports behind dceRows + purposeRows. */
  reportsById: ReadonlyMap<string, AustinReportFacts>;
  candidateDisplayName: string;
  /** The linked filer's exact spelling — a self-spend by any spelling drops. */
  filerName: string;
  officeCode: AustinOfficeCode;
  electionDate: string;
  /** Inclusive ISO date window for payment dates. */
  windowFrom: string;
  windowTo: string;
}): AustinOutsideAggregation {
  if (input.windowFrom > input.windowTo)
    throw new Error(
      `Austin outside-spending window is inverted: ${input.windowFrom}..${input.windowTo}`,
    );
  const filerKey = normalizeAustinFinanceTextKey(input.filerName);
  const windowRows: { row: AustinDirectCampaignExpenditureRow; spenderKey: string }[] = [];
  let windowRowCount = 0;
  let rowsWithoutSpender = 0;
  for (const row of input.dceRows) {
    if (row.paymentDate === null || row.paymentDate < input.windowFrom || row.paymentDate > input.windowTo)
      continue;
    windowRowCount += 1;
    const spenderKey = row.paidBy === null ? "" : normalizeAustinFinanceTextKey(row.paidBy);
    if (!spenderKey) {
      rowsWithoutSpender += 1;
      continue;
    }
    windowRows.push({ row, spenderKey });
  }

  // Rule 0: correction supersession per spender. Latest filing wins among
  // corrections of one (spender, period); the winners' periods then void
  // the spender's rows on non-correction reports.
  const correctionWinners = new Map<string, { reportId: string; dateFiled: string; periodFrom: string; periodTo: string }>();
  for (const { row, spenderKey } of windowRows) {
    const report = input.reportsById.get(row.reportId);
    if (!report || !REGULAR_CORRECTION_FORM_CODES.has(report.formTypeCode) || report.periodFrom === null || report.periodTo === null)
      continue;
    const key = `${spenderKey}|${report.periodFrom}|${report.periodTo}`;
    const current = correctionWinners.get(key);
    if (!current || current.dateFiled < report.dateFiled || (current.dateFiled === report.dateFiled && current.reportId < row.reportId))
      correctionWinners.set(key, { reportId: row.reportId, dateFiled: report.dateFiled, periodFrom: report.periodFrom, periodTo: report.periodTo });
  }
  const correctionPeriodsBySpender = new Map<string, { reportId: string; periodFrom: string; periodTo: string }[]>();
  for (const [key, winner] of correctionWinners) {
    const spenderKey = key.slice(0, key.indexOf("|"));
    const list = correctionPeriodsBySpender.get(spenderKey) ?? [];
    list.push(winner);
    correctionPeriodsBySpender.set(spenderKey, list);
  }
  const payments = new Map<string, EconomicPayment>();
  let supersededRowCount = 0;
  for (const { row, spenderKey } of windowRows) {
    const corrections = correctionPeriodsBySpender.get(spenderKey);
    if (corrections) {
      const report = input.reportsById.get(row.reportId);
      const isCorrection = report !== undefined && REGULAR_CORRECTION_FORM_CODES.has(report.formTypeCode);
      const superseded = isCorrection
        ? !corrections.some((winner) => winner.reportId === row.reportId)
        : corrections.some((winner) => winner.periodFrom <= row.paymentDate! && row.paymentDate! <= winner.periodTo);
      if (superseded) {
        supersededRowCount += 1;
        continue;
      }
    }
    const key = `${spenderKey}|${normalizeAustinFinanceTextKey(row.payee)}|${row.paymentDate}|${row.amountCents}`;
    const payment = payments.get(key) ?? {
      spenderKey,
      spenderName: row.paidBy!,
      amountCents: row.amountCents,
      rows: [],
      targetKeys: new Set<string>(),
    };
    payment.rows.push(row);
    payment.targetKeys.add(targetKey(row.candidateOrMeasure));
    payments.set(key, payment);
  }

  const directions = austinCommitteeDirections({
    purposeRows: input.purposeRows,
    reportsById: input.reportsById,
    candidateDisplayName: input.candidateDisplayName,
    officeCode: input.officeCode,
    electionDate: input.electionDate,
    windowFrom: input.windowFrom,
    windowTo: input.windowTo,
  });

  const grouped = new Map<string, AustinOutsideGroupInput>();
  const totals = { support: 0, oppose: 0 };
  let attributedPaymentCount = 0;
  let selfPaymentCount = 0;
  let selfCents = 0;
  let multiTargetPaymentCount = 0;
  let multiTargetCents = 0;
  let undirectedCents = 0;
  const undirectedSpenders = new Set<string>();
  let ambiguousDirectionCents = 0;
  const ambiguousDirectionSpenders = new Set<string>();
  for (const payment of payments.values()) {
    // Self first, by name alone: the candidate's own DCE rows usually carry
    // no office info at all (Qadri 2026: 44 rows, all blank), and they are
    // direct spending whatever the office column says.
    if (
      payment.spenderKey === filerKey ||
      namesCandidate(payment.spenderName, input.candidateDisplayName)
    ) {
      if (payment.rows.some((row) => namesCandidate(row.candidateOrMeasure, input.candidateDisplayName))) {
        selfPaymentCount += 1;
        selfCents += payment.amountCents;
      }
      continue;
    }
    const namesThisCandidate = payment.rows.some(
      (row) =>
        parseAustinOfficeSoughtCode(row.officeSoughtInfo) === input.officeCode &&
        namesCandidate(row.candidateOrMeasure, input.candidateDisplayName),
    );
    if (!namesThisCandidate) continue;
    if (payment.targetKeys.size !== 1) {
      multiTargetPaymentCount += 1;
      multiTargetCents += payment.amountCents;
      continue;
    }
    if (payment.amountCents <= 0) continue; // the schema requires amount >= 0; nothing to attribute
    const direction = directions.get(payment.spenderKey);
    if (direction === undefined) {
      undirectedCents += payment.amountCents;
      undirectedSpenders.add(payment.spenderName);
      continue;
    }
    if (direction === "ambiguous") {
      ambiguousDirectionCents += payment.amountCents;
      ambiguousDirectionSpenders.add(payment.spenderName);
      continue;
    }
    attributedPaymentCount += 1;
    totals[direction] += payment.amountCents;
    const groupKey = `${payment.spenderKey}|${direction}`;
    const group = grouped.get(groupKey) ?? {
      spenderName: payment.spenderName,
      supportOppose: direction,
      amountCents: 0,
    };
    group.amountCents += payment.amountCents;
    grouped.set(groupKey, group);
  }
  const groups = [...grouped.values()].sort(
    (a, b) =>
      b.amountCents - a.amountCents || a.spenderName.localeCompare(b.spenderName),
  );
  return {
    groups,
    supportTotalCents: totals.support,
    opposeTotalCents: totals.oppose,
    windowRowCount,
    rowsWithoutSpender,
    supersededRowCount,
    paymentCount: payments.size,
    attributedPaymentCount,
    selfPaymentCount,
    selfCents,
    multiTargetPaymentCount,
    multiTargetCents,
    undirectedCents,
    undirectedSpenders: [...undirectedSpenders].sort(),
    ambiguousDirectionCents,
    ambiguousDirectionSpenders: [...ambiguousDirectionSpenders].sort(),
  };
}
