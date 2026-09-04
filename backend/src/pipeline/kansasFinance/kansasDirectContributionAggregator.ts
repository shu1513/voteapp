// Kansas direct-side aggregation (plan-kansas-finance.md, Phase 4).
//
// Step 1 — cover-sourced totals. The Receipts and Expenditures cover
// (K.S.A. 25-4148 form; parseKansasReportCover) carries seven lines:
//   1 cash on hand at beginning of period
//   2 total contributions and other receipts (Schedule A)
//   3 cash available this period (1 + 2)
//   4 total expenditures and other disbursements (Schedule C)
//   5 cash on hand at close of period (3 - 4)
//   6 in-kind (non-monetary) contributions (Schedule B)
//   7 other transactions (Schedule D)
// Totals are the sum of lines 2, 4 and 6 over the CANONICAL version of
// every period in the cycle ledger (kansasReportInventory.ts), and cash on
// hand is line 5 of the latest canonical report. Line 6 stays separate
// from line 2: the form keeps in-kind out of receipts, and so does this.
//
// Step 2 — itemized breakdowns from Schedules A (receipts) and B (in-kind)
// of the same canonical versions. A schedule is used only when it
// reconciles: rows parsed, row sum = its itemized total, its own arithmetic,
// and its grand total = the cover line (2 for A, 6 for B) — so a schedule
// of the wrong report cannot slip in. Schedule A rows are classified by
// tender first: Loan and Refund rows are receipts, not contributions, and
// stay out of the buckets; any other tender in the pinned vocabulary is a
// contribution; an unknown tender fails closed. Occupation buckets take the
// rows that carry an occupation — the form asks it of individuals only
// (K.S.A. 25-4148a), so a filled cell is the form's own individual marker,
// while a blank (an entity, or an individual at or under $150) is outside
// the buckets and inside the coverage denominator, never an "Unknown"
// bucket. Size buckets take every positive itemized contribution row.
// Unitemized lumps are coverage metadata only. Dollars, never contributor
// counts (25-4154(d): no contributor name is kept or compared).
//
// Fail closed, per candidate: a cover that fails its own arithmetic, a
// canonical version with no opened cover (a paper scan — its totals come
// from OCR in a later step), a schedule that does not reconcile, or a
// ledger that is not complete leaves the candidate unpublishable. A cycle
// with no filed report at all (an affidavit of exemption — under $1,000 in
// and out, not $0 — or a first report not yet due) has no figures, never a
// synthetic zero. Last-minute reports duplicate into the next regular
// report and never count; prior-cycle filings are ignored.
//
// Pure. All arithmetic in integer cents.

import {
  checkKansasScheduleA,
  checkKansasScheduleB,
  reconcileKansasCoverArithmetic,
  type KansasReportCover,
  type KansasScheduleA,
  type KansasScheduleB,
} from "./kansasCfrViewerParsers.js";
import { kansasFilingHeaderKey, type KansasFilingHeader, type KansasLedger, type KansasLedgerEntry } from "./kansasReportInventory.js";

export type KansasOpenedCover = {
  /** The header the ledger was built from (kansasCandidateLedger `reports[].header`). */
  header: KansasFilingHeader;
  cover: KansasReportCover;
  /**
   * Schedules of the same report, opened right after its cover. Null when
   * the cover came without them (a paper report recovered by OCR): totals
   * only, no breakdowns.
   */
  scheduleA: KansasScheduleA | null;
  scheduleB: KansasScheduleB | null;
};

export type KansasCoverTotalsPeriod = {
  key: string;
  status: KansasLedgerEntry["status"];
  /** The canonical version's cover; null when the period has no counted filing. */
  cover: KansasReportCover | null;
};

export type KansasDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amountCents: number;
};

export type KansasItemizedContributions = {
  /** Every bucketed row: positive Schedule A contribution-tender rows plus positive Schedule B rows. */
  contributionCents: number;
  /** The part of contributionCents on rows carrying an occupation. */
  occupationCoveredCents: number;
  /** Unitemized receipts + political materials + contributor unknown + unitemized in-kind: never bucketed. */
  unitemizedCents: number;
  /** Schedule A Loan and Refund rows, signed: receipts, not contributions. */
  nonContributionReceiptCents: number;
  /** Occupation buckets (dollars desc, capped) then size buckets (smallest first). */
  breakdowns: KansasDirectBreakdown[];
};

export type KansasDirectFinance =
  | {
      status: "unpublishable";
      /** One line per blocker ("2025-annual: cover arithmetic failed"). */
      reasons: string[];
      periods: KansasCoverTotalsPeriod[];
    }
  | {
      /** Every period accounted for, none by a report: nothing to publish (not $0). */
      status: "no_filed_report";
      periods: KansasCoverTotalsPeriod[];
    }
  | {
      status: "ok";
      /** Sum of line 2 over canonical covers. */
      totalReceiptsCents: number;
      /** Sum of line 4. */
      totalDisbursementsCents: number;
      /** Sum of line 6. */
      inKindCents: number;
      /**
       * Line 5 of the latest canonical report (by period end); null when the
       * figure is negative (the summaries table rejects it — "not reported"
       * is honest).
       */
      cashOnHandCents: number | null;
      /** Null when a counted period's schedules were not opened (a paper report): totals only. */
      itemized: KansasItemizedContributions | null;
      /**
       * Non-blocking observations: a period's line 1 not equal to the prior
       * filed period's line 5; affidavit-exempt periods (under $1,000 of
       * unreported activity each) beside the filed ones; rows left out of
       * the buckets.
       */
      diagnostics: string[];
      periods: KansasCoverTotalsPeriod[];
    };

/** Period statuses whose canonical version's figures count. */
export const KANSAS_COUNTED_PERIOD_STATUSES: ReadonlySet<KansasLedgerEntry["status"]> = new Set(["report_filed", "amended"]);

/** Schedule A "Type of Payment" values seen live (2026-09-01), whitespace/hyphen-folded uppercase. */
const CONTRIBUTION_TENDERS: ReadonlySet<string> = new Set(["CASH", "CHECK", "CREDIT CARD", "E FUNDS", "OTHER"]);
const NON_CONTRIBUTION_TENDERS: ReadonlySet<string> = new Set(["LOAN", "REFUND"]);

function tenderKey(value: string): string {
  return value.toUpperCase().replace(/[\s-]+/g, " ").trim();
}

/** Placeholders filers type instead of an occupation (seen live: "Unknown", "NONE", "Occupation Requested"); treated as blank. */
const NOT_AN_OCCUPATION = /^(N\/?A|NONE|UNKNOWN|NOT AVAILABLE|(OCCUPATION |INFO |INFORMATION )?REQUESTED)$/;

/**
 * Conservative normalization only: whitespace collapsed, trailing
 * punctuation dropped, placeholders and letterless text -> null (blank).
 * Spellings are grouped case-insensitively downstream; nothing else is
 * inferred ("Ownere" stays "Ownere").
 */
export function normalizeKansasOccupation(raw: string): string | null {
  const label = raw
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[\s.,;]+$/, "");
  if (!/[A-Za-z]/.test(label) || NOT_AN_OCCUPATION.test(label.toUpperCase())) return null;
  return label;
}

/** The fleet's contribution-size vocabulary (Delaware edges). */
const SIZE_BUCKETS: readonly { name: string; belowCents: number }[] = [
  { name: "$1-$99", belowCents: 10_000 },
  { name: "$100-$249", belowCents: 25_000 },
  { name: "$250-$499", belowCents: 50_000 },
  { name: "$500-$999", belowCents: 100_000 },
  { name: "$1,000-$4,999", belowCents: 500_000 },
  { name: "$5,000+", belowCents: Number.POSITIVE_INFINITY },
];

function sizeBucket(amountCents: number): string {
  return SIZE_BUCKETS.find((bucket) => amountCents < bucket.belowCents)!.name;
}

/** A period's canonical cover with the schedules to itemize, once the cover passed its checks. */
type CountedPeriod = { key: string; opened: KansasOpenedCover };

/** A schedule's blocker against its cover, or null when it reconciles. */
function scheduleAReason(key: string, schedule: KansasScheduleA, cover: KansasReportCover): string | null {
  const check = checkKansasScheduleA(schedule.rows, schedule.totals);
  if (!check.rowsParsed) return `${key}: Schedule A rows unparsed`;
  if (!check.itemizedSumMatchesTotal) return `${key}: Schedule A rows do not sum to the itemized total`;
  if (!check.totalsArithmeticOk) return `${key}: Schedule A totals arithmetic failed`;
  if (schedule.totals.totalReceiptsCents !== cover.totalContributionsCents) {
    return `${key}: Schedule A total receipts ${schedule.totals.totalReceiptsCents} differ from cover line 2 ${cover.totalContributionsCents}`;
  }
  return null;
}

function scheduleBReason(key: string, schedule: KansasScheduleB, cover: KansasReportCover): string | null {
  const check = checkKansasScheduleB(schedule.rows, schedule.totals);
  if (!check.rowsParsed) return `${key}: Schedule B rows unparsed`;
  if (!check.itemizedSumMatchesTotal) return `${key}: Schedule B rows do not sum to the itemized total`;
  if (!check.totalsArithmeticOk) return `${key}: Schedule B totals arithmetic failed`;
  if (schedule.totals.totalInKindCents !== cover.inKindCents) {
    return `${key}: Schedule B total in-kind ${schedule.totals.totalInKindCents} differs from cover line 6 ${cover.inKindCents}`;
  }
  return null;
}

/** Dollar totals keyed case-insensitively; the display label is the spelling with the most dollars. */
type OccupationTotals = Map<string, { cents: number; spellings: Map<string, number> }>;

function addOccupation(totals: OccupationTotals, label: string, cents: number): void {
  const key = label.toUpperCase();
  const entry = totals.get(key) ?? { cents: 0, spellings: new Map<string, number>() };
  entry.cents += cents;
  entry.spellings.set(label, (entry.spellings.get(label) ?? 0) + cents);
  totals.set(key, entry);
}

function occupationBreakdowns(totals: OccupationTotals, limit: number): KansasDirectBreakdown[] {
  return [...totals.values()]
    .map((entry) => {
      const [label] = [...entry.spellings.entries()].sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))[0]!;
      return { categoryType: "occupation" as const, categoryName: label, amountCents: entry.cents };
    })
    .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
    .slice(0, limit);
}

function aggregateItemized(
  counted: readonly CountedPeriod[],
  reasons: string[],
  diagnostics: string[],
  maxOccupationBreakdowns: number
): KansasItemizedContributions | null {
  const withoutSchedules = counted.filter(({ opened }) => opened.scheduleA === null || opened.scheduleB === null);
  for (const { key, opened } of withoutSchedules) {
    if (opened.header.channel === "efile") reasons.push(`${key}: e-filed cover opened without its schedules`);
  }
  if (withoutSchedules.length > 0) {
    diagnostics.push(`no breakdowns: schedules not opened for ${withoutSchedules.map(({ key }) => key).join(", ")}`);
    return null;
  }

  let contributionCents = 0;
  let occupationCoveredCents = 0;
  let unitemizedCents = 0;
  let nonContributionReceiptCents = 0;
  const occupationTotals: OccupationTotals = new Map();
  const sizeTotals = new Map<string, number>();
  const bucket = (cents: number, occupationCell: string) => {
    contributionCents += cents;
    sizeTotals.set(sizeBucket(cents), (sizeTotals.get(sizeBucket(cents)) ?? 0) + cents);
    const occupation = normalizeKansasOccupation(occupationCell);
    if (occupation !== null) {
      occupationCoveredCents += cents;
      addOccupation(occupationTotals, occupation, cents);
    }
  };

  for (const { key, opened } of counted) {
    const { cover } = opened;
    const scheduleA = opened.scheduleA!;
    const scheduleB = opened.scheduleB!;
    const blockers = [scheduleAReason(key, scheduleA, cover), scheduleBReason(key, scheduleB, cover)].filter(
      (reason): reason is string => reason !== null
    );
    if (blockers.length > 0) {
      reasons.push(...blockers);
      continue;
    }
    let nonPositiveRows = 0;
    let otherTender = { rows: 0, cents: 0 };
    for (const row of scheduleA.rows.rows) {
      const cents = row.amountCents!; // rowsParsed proved it
      const tender = tenderKey(row.tenderType);
      if (NON_CONTRIBUTION_TENDERS.has(tender)) {
        nonContributionReceiptCents += cents;
        continue;
      }
      if (!CONTRIBUTION_TENDERS.has(tender)) {
        reasons.push(`${key}: Schedule A row ${row.index} has an unknown tender "${row.tenderType}"`);
        continue;
      }
      // "Other" is a payment type, not a receipt category, so it counts as a
      // contribution — but live it is a mix (persons with occupations,
      // occupation-less odd amounts), so it is called out for review.
      if (tender === "OTHER") otherTender = { rows: otherTender.rows + 1, cents: otherTender.cents + cents };
      if (cents <= 0) nonPositiveRows += 1;
      else bucket(cents, row.occupation);
    }
    for (const row of scheduleB.rows.rows) {
      const cents = row.valueCents!;
      if (cents <= 0) nonPositiveRows += 1;
      else bucket(cents, row.occupation);
    }
    if (otherTender.rows > 0) diagnostics.push(`${key}: ${otherTender.rows} "Other" tender rows (${otherTender.cents} cents) counted as contributions`);
    if (nonPositiveRows > 0) diagnostics.push(`${key}: ${nonPositiveRows} itemized rows at or below $0 are not in the buckets`);
    // The arithmetic checks proved every total non-null.
    unitemizedCents +=
      scheduleA.totals.totalUnitemizedCents! +
      scheduleA.totals.politicalMaterialsCents! +
      scheduleA.totals.contributorUnknownCents! +
      scheduleB.totals.totalUnitemizedCents!;
  }

  const breakdowns: KansasDirectBreakdown[] = [
    ...occupationBreakdowns(occupationTotals, maxOccupationBreakdowns),
    ...SIZE_BUCKETS.filter((bucketDef) => sizeTotals.has(bucketDef.name)).map((bucketDef) => ({
      categoryType: "contribution_size" as const,
      categoryName: bucketDef.name,
      amountCents: sizeTotals.get(bucketDef.name)!,
    })),
  ];
  return { contributionCents, occupationCoveredCents, unitemizedCents, nonContributionReceiptCents, breakdowns };
}

export function aggregateKansasDirectFinance(input: {
  ledger: KansasLedger;
  covers: readonly KansasOpenedCover[];
  /** Occupation buckets kept, by dollars (the fleet's 50). */
  maxOccupationBreakdowns?: number;
}): KansasDirectFinance {
  const coversByKey = new Map<string, KansasOpenedCover[]>();
  for (const opened of input.covers) {
    const key = kansasFilingHeaderKey(opened.header);
    coversByKey.set(key, [...(coversByKey.get(key) ?? []), opened]);
  }

  const reasons: string[] = [];
  const diagnostics: string[] = [];
  const periods: KansasCoverTotalsPeriod[] = [];
  const counted: CountedPeriod[] = [];
  let totalReceiptsCents = 0;
  let totalDisbursementsCents = 0;
  let inKindCents = 0;
  let latest: { periodEnd: string; cover: KansasReportCover } | null = null;
  let previousClose: { key: string; cents: number } | null = null;

  if (!input.ledger.complete) reasons.push("ledger incomplete");

  for (const entry of input.ledger.entries) {
    const { key } = entry.period;
    if (!KANSAS_COUNTED_PERIOD_STATUSES.has(entry.status)) {
      periods.push({ key, status: entry.status, cover: null });
      continue;
    }
    const canonical = entry.canonical;
    if (canonical === null) {
      // Unreachable by construction (report_filed/amended require a canonical); named so a regression surfaces.
      reasons.push(`${key}: counted period has no canonical version`);
      periods.push({ key, status: entry.status, cover: null });
      continue;
    }
    const matches = coversByKey.get(kansasFilingHeaderKey(canonical)) ?? [];
    if (matches.length !== 1) {
      reasons.push(
        matches.length === 0
          ? `${key}: no opened cover for the canonical ${canonical.channel} version`
          : `${key}: ${matches.length} opened covers match the canonical version`
      );
      periods.push({ key, status: entry.status, cover: null });
      continue;
    }
    const opened = matches[0]!;
    const { cover } = opened;
    periods.push({ key, status: entry.status, cover });
    if (!reconcileKansasCoverArithmetic(cover)) {
      reasons.push(`${key}: cover arithmetic failed`);
      continue;
    }
    if (cover.inKindCents === null) {
      reasons.push(`${key}: cover line 6 (in-kind) unparsed`);
      continue;
    }
    // reconcileKansasCoverArithmetic proved these non-null.
    totalReceiptsCents += cover.totalContributionsCents!;
    totalDisbursementsCents += cover.totalExpendituresCents!;
    inKindCents += cover.inKindCents;
    if (previousClose !== null && cover.cashBeginningCents !== previousClose.cents) {
      diagnostics.push(`${key}: line 1 ${cover.cashBeginningCents} differs from ${previousClose.key} line 5 ${previousClose.cents}`);
    }
    previousClose = { key, cents: cover.cashCloseCents! };
    if (latest === null || entry.period.end > latest.periodEnd) latest = { periodEnd: entry.period.end, cover };
    counted.push({ key, opened });
  }

  if (reasons.length > 0) return { status: "unpublishable", reasons, periods };
  if (latest === null) return { status: "no_filed_report", periods };

  const itemized = aggregateItemized(counted, reasons, diagnostics, input.maxOccupationBreakdowns ?? 50);
  if (reasons.length > 0) return { status: "unpublishable", reasons, periods };

  const exempt = periods.filter((period) => period.status === "affidavit_exempt").map((period) => period.key);
  if (exempt.length > 0) diagnostics.push(`affidavit-exempt periods not in totals: ${exempt.join(", ")}`);
  const close = latest.cover.cashCloseCents!;
  if (close < 0) diagnostics.push(`cash on hand ${close} is negative; reported as null`);
  return {
    status: "ok",
    totalReceiptsCents,
    totalDisbursementsCents,
    inKindCents,
    cashOnHandCents: close < 0 ? null : close,
    itemized,
    diagnostics,
    periods,
  };
}
