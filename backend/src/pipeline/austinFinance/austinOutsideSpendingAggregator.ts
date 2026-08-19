// Outside-spending aggregation for Austin (plan Phase 3, gotchas 3-5). Input
// is the whole Direct Campaign Expenditure (DCE) dataset and the whole
// Committee Purpose dataset (both a few hundred rows, fetched once per batch
// run) plus one linked candidate's identity: display name, exact filer
// name, office code, election date, and the cycle window.
//
// A DCE row is one (payment × target): a $71,000 mailer naming five
// candidates is five rows, and a corrected report re-lists all of them
// under a new report id. So:
//   1. rows inside the cycle window collapse into ECONOMIC PAYMENTS by
//      (spender key, payee key, payment date, amount) — across reports. Two
//      genuinely identical same-day payments would merge (an undercount,
//      never an overcount — the Phase 0 trade);
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
//      gates), and neither its election date nor its office code
//      CONTRADICTS the link (many live rows leave both blank, or say OTHER;
//      a blank is silence, not a conflict). ASSIST is officeholder help,
//      not electioneering. One direction → used; both → ambiguous; none →
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
} from "./austinSocrataClient.js";

type Direction = "support" | "oppose";

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
  candidateDisplayName: string;
  officeCode: AustinOfficeCode;
  electionDate: string;
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
    if (row.electionDate !== null && row.electionDate !== input.electionDate) continue;
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
  const payments = new Map<string, EconomicPayment>();
  let windowRowCount = 0;
  let rowsWithoutSpender = 0;
  for (const row of input.dceRows) {
    if (row.paymentDate === null || row.paymentDate < input.windowFrom || row.paymentDate > input.windowTo)
      continue;
    windowRowCount += 1;
    if (row.paidBy === null) {
      rowsWithoutSpender += 1;
      continue;
    }
    const spenderKey = normalizeAustinFinanceTextKey(row.paidBy);
    if (!spenderKey) {
      rowsWithoutSpender += 1;
      continue;
    }
    const key = `${spenderKey}|${normalizeAustinFinanceTextKey(row.payee)}|${row.paymentDate}|${row.amountCents}`;
    const payment = payments.get(key) ?? {
      spenderKey,
      spenderName: row.paidBy,
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
    candidateDisplayName: input.candidateDisplayName,
    officeCode: input.officeCode,
    electionDate: input.electionDate,
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
