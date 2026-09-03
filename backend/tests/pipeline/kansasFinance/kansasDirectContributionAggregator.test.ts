import { describe, expect, it } from "vitest";

import type { KansasReportCover, KansasScheduleARow, KansasScheduleBRow } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import {
  aggregateKansasDirectFinance,
  normalizeKansasOccupation,
  type KansasOpenedCover,
} from "../../../src/pipeline/kansasFinance/kansasDirectContributionAggregator.js";
import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import {
  buildKansasReportLedger,
  kansasLastMinuteWindows,
  kansasReportingPeriods,
  type KansasFilingHeader,
} from "../../../src/pipeline/kansasFinance/kansasReportInventory.js";

const HOUSE = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
const NOW = new Date("2026-09-02T12:00:00.000Z");
const periods = kansasReportingPeriods(HOUSE, 2026);

function header(overrides: Partial<KansasFilingHeader> & Pick<KansasFilingHeader, "periodStart" | "periodEnd">): KansasFilingHeader {
  return { fileDate: "01/09/2026", amendmentDate: null, amended: false, termination: false, channel: "efile", ...overrides };
}

/** A reconciled cover (lines 1+2=3, 3-4=5) from its beginning balance and flows. */
function cover(input: { begin: number; receipts: number; spent: number; inKind?: number }): KansasReportCover {
  return {
    candidateName: "EXAMPLE CANDIDATE",
    officeSought: "State Representative",
    district: "85",
    periodStart: "",
    periodEnd: "",
    amended: false,
    termination: false,
    electronicallyFiledOn: null,
    cashBeginningCents: input.begin,
    totalContributionsCents: input.receipts,
    cashAvailableCents: input.begin + input.receipts,
    totalExpendituresCents: input.spent,
    cashCloseCents: input.begin + input.receipts - input.spent,
    inKindCents: input.inKind ?? 0,
    otherTransactionsCents: 0,
  };
}

// Synthetic contributors on purpose (25-4154(d) posture).
let rowIndex = 0;
function aRow(overrides: Partial<KansasScheduleARow> & Pick<KansasScheduleARow, "amountCents">): KansasScheduleARow {
  rowIndex += 1;
  return {
    index: rowIndex,
    date: "03/01/26",
    contributorName: "Example Person",
    addressLines: ["1 Example St", "Sampleton KS 66000"],
    zip: "66000",
    tenderType: "Check",
    occupation: "",
    primaryTotalCents: overrides.amountCents,
    generalTotalCents: 0,
    ...overrides,
  };
}
function bRow(overrides: Partial<KansasScheduleBRow> & Pick<KansasScheduleBRow, "valueCents">): KansasScheduleBRow {
  rowIndex += 1;
  return {
    index: rowIndex,
    date: "03/01/26",
    contributorName: "Example Person",
    addressLines: ["1 Example St", "Sampleton KS 66000"],
    zip: "66000",
    occupation: "",
    description: "Food and Drink",
    ...overrides,
  };
}

/**
 * An opened e-filed report whose cover and both schedules reconcile: line 2
 * = itemized A rows + unitemized A, line 6 = itemized B rows + unitemized B.
 */
function report(
  filing: KansasFilingHeader,
  input: { begin: number; spent: number; rowsA?: KansasScheduleARow[]; unitemizedA?: number; rowsB?: KansasScheduleBRow[]; unitemizedB?: number }
): KansasOpenedCover {
  const rowsA = input.rowsA ?? [];
  const rowsB = input.rowsB ?? [];
  const itemizedA = rowsA.reduce((sum, row) => sum + row.amountCents!, 0);
  const itemizedB = rowsB.reduce((sum, row) => sum + row.valueCents!, 0);
  const receipts = itemizedA + (input.unitemizedA ?? 0);
  const inKind = itemizedB + (input.unitemizedB ?? 0);
  return {
    header: filing,
    cover: cover({ begin: input.begin, receipts, spent: input.spent, inKind }),
    scheduleA: {
      rows: { rows: rowsA, malformedRowCount: 0 },
      totals: {
        totalItemizedCents: itemizedA,
        totalUnitemizedCents: input.unitemizedA ?? 0,
        politicalMaterialsCents: 0,
        contributorUnknownCents: 0,
        totalReceiptsCents: receipts,
      },
    },
    scheduleB: {
      rows: { rows: rowsB, malformedRowCount: 0 },
      totals: { totalItemizedCents: itemizedB, totalUnitemizedCents: input.unitemizedB ?? 0, totalInKindCents: inKind },
    },
  };
}

const ANNUAL = { periodStart: "1/1/2025", periodEnd: "12/31/2025" };
const PRE_PRIMARY = { periodStart: "1/1/2026", periodEnd: "7/23/2026" };
const NO_ITEMIZED = { contributionCents: 0, occupationCoveredCents: 0, nonContributionReceiptCents: 0, breakdowns: [] };

function ledgerOf(filings: KansasFilingHeader[], extra: Partial<Parameters<typeof buildKansasReportLedger>[0]> = {}) {
  return buildKansasReportLedger({
    periods,
    filings,
    appointmentsOfTreasurer: [],
    affidavitDates: [],
    lastMinuteWindows: kansasLastMinuteWindows(2026),
    now: NOW,
    ...extra,
  });
}

describe("aggregateKansasDirectFinance", () => {
  it("sums lines 2, 4 and 6 over the filed periods and takes cash on hand from the latest cover", () => {
    const annual = header({ ...ANNUAL, fileDate: "01/09/2026" });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const lastMinute = header({ periodStart: "7/24/2026", periodEnd: "7/29/2026", fileDate: "07/30/2026" });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual, prePrimary, lastMinute]),
      covers: [
        report(annual, { begin: 10_000, spent: 60_000, unitemizedA: 250_000, unitemizedB: 1_500 }),
        report(prePrimary, { begin: 200_000, spent: 150_000, unitemizedA: 100_050, unitemizedB: 22_328 }),
        // Last-minute reports duplicate into the next regular report: never counted even when a cover is on hand.
        report(lastMinute, { begin: 150_050, spent: 0, unitemizedA: 999_999 }),
      ],
    });
    expect(result).toEqual({
      status: "ok",
      totalReceiptsCents: 350_050,
      totalDisbursementsCents: 210_000,
      inKindCents: 23_828,
      cashOnHandCents: 150_050,
      itemized: { ...NO_ITEMIZED, unitemizedCents: 373_878 },
      diagnostics: [],
      periods: [
        { key: "2025-annual", status: "report_filed", cover: expect.objectContaining({ cashCloseCents: 200_000 }) },
        { key: "2026-pre_primary", status: "report_filed", cover: expect.objectContaining({ cashCloseCents: 150_050 }) },
        { key: "2026-pre_general", status: "not_yet_due", cover: null },
        { key: "2026-post_general", status: "not_yet_due", cover: null },
      ],
    });
  });

  it("buckets itemized contributions by size and by occupation, classifying Schedule A rows by tender first", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const opened = report(annual, {
      begin: 0,
      spent: 0,
      rowsA: [
        aRow({ amountCents: 50_000, tenderType: "Check", occupation: "Retired" }),
        aRow({ amountCents: 20_000, tenderType: "Credit Card", occupation: "retired." }),
        aRow({ amountCents: 10_000, tenderType: "Cash" }),
        aRow({ amountCents: 500_000, tenderType: "E Funds", occupation: "Farmer" }),
        aRow({ amountCents: 7_500, tenderType: "Other", occupation: "N/A" }),
        // Receipts, not contributions: never bucketed, reported beside the buckets.
        aRow({ amountCents: 1_000_000, tenderType: "Loan", contributorName: "Example Candidate" }),
        aRow({ amountCents: 2_050, tenderType: "Refund", contributorName: "Example Vendor LLC" }),
      ],
      unitemizedA: 1_000,
      rowsB: [bRow({ valueCents: 30_000, occupation: "Farmer", description: "Donation of Signs" }), bRow({ valueCents: 5_000 })],
      unitemizedB: 250,
    });
    const input = {
      ledger: ledgerOf([annual, prePrimary]),
      covers: [opened, report(prePrimary, { begin: opened.cover.cashCloseCents!, spent: 0 })],
    };
    expect(aggregateKansasDirectFinance(input)).toMatchObject({
      status: "ok",
      totalReceiptsCents: 1_590_550,
      inKindCents: 35_250,
      itemized: {
        contributionCents: 622_500,
        occupationCoveredCents: 600_000,
        unitemizedCents: 1_250,
        nonContributionReceiptCents: 1_002_050,
        breakdowns: [
          { categoryType: "occupation", categoryName: "Farmer", amountCents: 530_000 },
          { categoryType: "occupation", categoryName: "Retired", amountCents: 70_000 },
          { categoryType: "contribution_size", categoryName: "$1-$99", amountCents: 12_500 },
          { categoryType: "contribution_size", categoryName: "$100-$249", amountCents: 30_000 },
          { categoryType: "contribution_size", categoryName: "$250-$499", amountCents: 30_000 },
          { categoryType: "contribution_size", categoryName: "$500-$999", amountCents: 50_000 },
          { categoryType: "contribution_size", categoryName: "$5,000+", amountCents: 500_000 },
        ],
      },
      diagnostics: ['2025-annual: 1 "Other" tender rows (7500 cents) counted as contributions'],
    });
    // The cap drops buckets, not coverage.
    const capped = aggregateKansasDirectFinance({ ...input, maxOccupationBreakdowns: 1 });
    expect(capped.status === "ok" && capped.itemized?.breakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      { categoryType: "occupation", categoryName: "Farmer", amountCents: 530_000 },
    ]);
    expect(capped.status === "ok" && capped.itemized?.occupationCoveredCents).toBe(600_000);
  });

  it("leaves rows at or below $0 out of the buckets with a diagnostic", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const opened = report(annual, {
      begin: 0,
      spent: 0,
      rowsA: [aRow({ amountCents: 10_000, occupation: "Teacher" }), aRow({ amountCents: -5_000, occupation: "Teacher" })],
      rowsB: [bRow({ valueCents: 0 })],
    });
    expect(
      aggregateKansasDirectFinance({
        ledger: ledgerOf([annual, prePrimary]),
        covers: [opened, report(prePrimary, { begin: opened.cover.cashCloseCents!, spent: 0 })],
      })
    ).toMatchObject({
      status: "ok",
      totalReceiptsCents: 5_000,
      itemized: {
        contributionCents: 10_000,
        occupationCoveredCents: 10_000,
        breakdowns: [
          { categoryType: "occupation", categoryName: "Teacher", amountCents: 10_000 },
          { categoryType: "contribution_size", categoryName: "$100-$249", amountCents: 10_000 },
        ],
      },
      diagnostics: ["2025-annual: 2 itemized rows at or below $0 are not in the buckets"],
    });
  });

  it("is unpublishable on an unknown tender", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const opened = report(annual, { begin: 0, spent: 0, rowsA: [aRow({ amountCents: 10_000, tenderType: "Wire", index: 3 })] });
    expect(
      aggregateKansasDirectFinance({
        ledger: ledgerOf([annual, prePrimary]),
        covers: [opened, report(prePrimary, { begin: 10_000, spent: 0 })],
      })
    ).toMatchObject({ status: "unpublishable", reasons: ['2025-annual: Schedule A row 3 has an unknown tender "Wire"'] });
  });

  it("is unpublishable when a schedule does not reconcile to its cover", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const annualReport = report(annual, { begin: 0, spent: 0, rowsA: [aRow({ amountCents: 10_000 })] });
    const prePrimaryReport = report(prePrimary, { begin: 10_000, spent: 0, rowsB: [bRow({ valueCents: 5_000 })] });
    expect(
      aggregateKansasDirectFinance({
        ledger: ledgerOf([annual, prePrimary]),
        covers: [
          // A different report's Schedule A (internally consistent): its total receipts are not this cover's line 2.
          { ...annualReport, scheduleA: { ...annualReport.scheduleA!, totals: { ...annualReport.scheduleA!.totals, totalUnitemizedCents: 1, totalReceiptsCents: 10_001 } } },
          // Rows that do not add up to the itemized line.
          { ...prePrimaryReport, scheduleB: { ...prePrimaryReport.scheduleB!, rows: { rows: [bRow({ valueCents: 4_999 })], malformedRowCount: 0 } } },
        ],
      })
    ).toMatchObject({
      status: "unpublishable",
      reasons: [
        "2025-annual: Schedule A total receipts 10001 differ from cover line 2 10000",
        "2026-pre_primary: Schedule B rows do not sum to the itemized total",
      ],
    });
  });

  it("requires schedules on an e-filed cover but publishes totals only for a paper cover", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const paper: KansasFilingHeader = {
      periodStart: "2026-01-01",
      periodEnd: "2026-07-23",
      fileDate: null,
      amendmentDate: null,
      amended: false,
      amendmentOrdinal: null,
      termination: false,
      channel: "paper",
    };
    const withoutSchedules = (opened: KansasOpenedCover): KansasOpenedCover => ({ ...opened, scheduleA: null, scheduleB: null });
    expect(
      aggregateKansasDirectFinance({
        ledger: ledgerOf([annual, prePrimary]),
        covers: [withoutSchedules(report(annual, { begin: 0, spent: 0, unitemizedA: 100 })), report(prePrimary, { begin: 100, spent: 0 })],
      })
    ).toMatchObject({ status: "unpublishable", reasons: ["2025-annual: e-filed cover opened without its schedules"] });
    expect(
      aggregateKansasDirectFinance({
        ledger: ledgerOf([annual, paper]),
        covers: [report(annual, { begin: 0, spent: 0, unitemizedA: 100 }), withoutSchedules(report(paper, { begin: 100, spent: 50 }))],
      })
    ).toMatchObject({
      status: "ok",
      totalReceiptsCents: 100,
      totalDisbursementsCents: 50,
      cashOnHandCents: 50,
      itemized: null,
      diagnostics: ["no breakdowns: schedules not opened for 2026-pre_primary"],
    });
  });

  it("counts only the canonical version of an amended period", () => {
    const original = header({ ...ANNUAL, fileDate: "01/09/2026" });
    const amendment = header({ ...ANNUAL, fileDate: "01/09/2026", amendmentDate: "01/15/2026", amended: true });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([original, amendment]),
      covers: [report(original, { begin: 0, spent: 0, unitemizedA: 100_000 }), report(amendment, { begin: 0, spent: 5_000, unitemizedA: 120_000 })],
    });
    // The ledger is incomplete (pre-primary missing), so only the period rows are checked here.
    expect(result.status).toBe("unpublishable");
    expect(result.periods[0]).toEqual({ key: "2025-annual", status: "amended", cover: expect.objectContaining({ totalContributionsCents: 120_000 }) });
  });

  it("is unpublishable when the ledger is incomplete, naming the reason", () => {
    const annual = header({ ...ANNUAL });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual]),
      covers: [report(annual, { begin: 0, spent: 0, unitemizedA: 1 })],
    });
    expect(result).toMatchObject({ status: "unpublishable", reasons: ["ledger incomplete"] });
  });

  it("is unpublishable when a canonical cover fails its arithmetic or lacks line 6", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const annualReport = report(annual, { begin: 0, spent: 0, unitemizedA: 100 });
    const prePrimaryReport = report(prePrimary, { begin: 100, spent: 0 });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual, prePrimary]),
      covers: [
        { ...annualReport, cover: { ...annualReport.cover, cashCloseCents: 99 } },
        { ...prePrimaryReport, cover: { ...prePrimaryReport.cover, inKindCents: null } },
      ],
    });
    expect(result).toMatchObject({
      status: "unpublishable",
      reasons: ["2025-annual: cover arithmetic failed", "2026-pre_primary: cover line 6 (in-kind) unparsed"],
    });
  });

  it("is unpublishable when the canonical version has no opened cover (a paper scan)", () => {
    const annual = header({ ...ANNUAL });
    const paper: KansasFilingHeader = {
      periodStart: "2026-01-01",
      periodEnd: "2026-07-23",
      fileDate: null,
      amendmentDate: null,
      amended: false,
      amendmentOrdinal: null,
      termination: false,
      channel: "paper",
    };
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual, paper]),
      covers: [report(annual, { begin: 0, spent: 0, unitemizedA: 100 })],
    });
    expect(result).toMatchObject({
      status: "unpublishable",
      reasons: ["2026-pre_primary: no opened cover for the canonical paper version"],
    });
  });

  it("is unpublishable when two opened covers claim the canonical version", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual, prePrimary]),
      covers: [
        report(annual, { begin: 0, spent: 0, unitemizedA: 100 }),
        report({ ...annual }, { begin: 0, spent: 0, unitemizedA: 200 }),
        report(prePrimary, { begin: 100, spent: 0 }),
      ],
    });
    expect(result).toMatchObject({ status: "unpublishable", reasons: ["2025-annual: 2 opened covers match the canonical version"] });
  });

  it("reports a negative close as null cash on hand and a balance discontinuity as diagnostics", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual, prePrimary]),
      covers: [
        report(annual, { begin: 0, spent: 0, unitemizedA: 100 }),
        // Begins at 90, not the 100 the annual closed with; overspends to -10.
        report(prePrimary, { begin: 90, spent: 100 }),
      ],
    });
    expect(result).toMatchObject({
      status: "ok",
      totalReceiptsCents: 100,
      totalDisbursementsCents: 100,
      cashOnHandCents: null,
      diagnostics: ["2026-pre_primary: line 1 90 differs from 2025-annual line 5 100", "cash on hand -10 is negative; reported as null"],
    });
  });

  it("publishes nothing, never $0, for a cycle with no filed report (affidavit of exemption, or first report not yet due)", () => {
    // Exempt = under $1,000 in and out, not zero.
    expect(aggregateKansasDirectFinance({ ledger: ledgerOf([], { affidavitDates: ["01/05/2026"] }), covers: [] })).toEqual({
      status: "no_filed_report",
      periods: periods.map((period) => ({ key: period.key, status: "affidavit_exempt", cover: null })),
    });
    // A committee appointed after every due period so far: nothing owed yet.
    expect(
      aggregateKansasDirectFinance({
        ledger: ledgerOf([], { appointmentsOfTreasurer: [{ fileDate: "08/01/2026", amendmentNo: "" }] }),
        covers: [],
      })
    ).toMatchObject({ status: "no_filed_report" });
  });

  it("notes affidavit-exempt periods beside filed ones as a diagnostic (their activity is under $1,000, not zero)", () => {
    const annual = header({ ...ANNUAL });
    const result = aggregateKansasDirectFinance({
      ledger: ledgerOf([annual], { affidavitDates: ["03/01/2026"] }),
      covers: [report(annual, { begin: 0, spent: 0, unitemizedA: 100 })],
    });
    expect(result).toMatchObject({
      status: "ok",
      totalReceiptsCents: 100,
      diagnostics: ["affidavit-exempt periods not in totals: 2026-pre_primary, 2026-pre_general, 2026-post_general"],
    });
  });
});

describe("normalizeKansasOccupation", () => {
  it("collapses whitespace and trailing punctuation, and treats placeholders as blank", () => {
    expect(normalizeKansasOccupation("  Self-employed. ")).toBe("Self-employed");
    expect(normalizeKansasOccupation("Not  Employed")).toBe("Not Employed");
    expect(normalizeKansasOccupation("Ownere")).toBe("Ownere");
    expect(normalizeKansasOccupation("N/A")).toBeNull();
    expect(normalizeKansasOccupation("none")).toBeNull();
    expect(normalizeKansasOccupation("Occupation Requested")).toBeNull();
    expect(normalizeKansasOccupation("---")).toBeNull();
    expect(normalizeKansasOccupation("")).toBeNull();
  });
});
