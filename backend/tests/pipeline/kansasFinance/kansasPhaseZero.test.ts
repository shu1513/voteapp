import { describe, expect, it } from "vitest";

import type { KansasContributionExportRow } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import {
  checkKansasCoverAgainstSchedules,
  extractKansasOcrMoneyValues,
  reconcileKansasIeStatements,
  recoverKansasOcrCover,
  summarizeKansasContributionExport,
} from "../../../src/pipeline/kansasFinance/kansasPhaseZero.js";

function exportRow(overrides: Partial<KansasContributionExportRow>): KansasContributionExportRow {
  return {
    candidateName: "Cindy Holscher",
    contributorName: "Someone",
    city: "",
    state: "",
    zip: "",
    occupation: "",
    industry: "",
    date: "01/02/2026",
    tenderType: "Check",
    amountCents: null,
    inKindAmountCents: null,
    inKindDescription: "",
    periodStart: "01/01/2026",
    periodEnd: "07/23/2026",
    ...overrides,
  };
}

describe("summarizeKansasContributionExport", () => {
  it("aggregates dollars, not counts, for occupation coverage", () => {
    const summary = summarizeKansasContributionExport([
      exportRow({ amountCents: 30000, occupation: "Attorney" }),
      exportRow({ amountCents: 10000 }),
      exportRow({ inKindAmountCents: 5000, occupation: "Retired" }),
    ]);
    expect(summary).toMatchObject({
      rowCount: 3,
      monetaryRowCount: 2,
      monetaryTotalCents: 40000,
      inKindTotalCents: 5000,
      unparsedAmountRowCount: 0,
      occupationFilledRowCount: 2,
    });
    expect(summary.occupationCoveredMonetaryShare).toBeCloseTo(0.75);
  });

  it("counts rows whose amounts failed to parse", () => {
    const summary = summarizeKansasContributionExport([exportRow({})]);
    expect(summary.unparsedAmountRowCount).toBe(1);
    expect(summary.occupationCoveredMonetaryShare).toBeNull();
  });
});

describe("checkKansasCoverAgainstSchedules", () => {
  const cover = {
    candidateName: "Dale R Helwig",
    officeSought: "State Representative",
    district: "1",
    periodStart: "1/1/2026",
    periodEnd: "7/23/2026",
    amended: false,
    termination: false,
    electronicallyFiledOn: "7/27/2026",
    cashBeginningCents: 307759,
    totalContributionsCents: 435000,
    cashAvailableCents: 742759,
    totalExpendituresCents: 186065,
    cashCloseCents: 556694,
    inKindCents: 0,
    otherTransactionsCents: 160000,
  };

  it("passes the live Helwig fixture", () => {
    expect(
      checkKansasCoverAgainstSchedules(
        cover,
        {
          totalItemizedCents: 435000,
          totalUnitemizedCents: 0,
          politicalMaterialsCents: 0,
          contributorUnknownCents: 0,
          totalReceiptsCents: 435000,
        },
        { totalItemizedCents: 186065, totalUnitemizedCents: 0, totalExpendituresCents: 186065 }
      )
    ).toEqual({ coverArithmeticOk: true, scheduleAMatchesCover: true, scheduleCMatchesCover: true });
  });

  it("flags a schedule total that disagrees with the cover", () => {
    const checks = checkKansasCoverAgainstSchedules(
      cover,
      {
        totalItemizedCents: 435000,
        totalUnitemizedCents: 0,
        politicalMaterialsCents: 0,
        contributorUnknownCents: 0,
        totalReceiptsCents: 435001,
      },
      { totalItemizedCents: 186065, totalUnitemizedCents: 0, totalExpendituresCents: 186065 }
    );
    expect(checks.scheduleAMatchesCover).toBe(false);
    expect(checks.scheduleCMatchesCover).toBe(true);
  });
});

describe("recoverKansasOcrCover", () => {
  it("recovers the five summary amounts from noisy OCR money values", () => {
    // Live H002KC values with interleaved noise ($150 limit note, a $500 row).
    const text =
      "$7,152.91 junk $7,449.22 x $14,602.13 y $10,453.26 z $4,148.87 $0.00 $150 $500.00";
    const recovery = recoverKansasOcrCover(extractKansasOcrMoneyValues(text));
    expect(recovery).toMatchObject({
      beginCents: 715291,
      receiptsCents: 744922,
      availableCents: 1460213,
      expendituresCents: 1045326,
      closeCents: 414887,
      usedUncertainRead: false,
    });
  });

  it("accepts an uncertain read only via the arithmetic identities", () => {
    // Live H001DH shape: "$4,350.001" carries a border-artifact digit.
    const text = "$3,077.59 $4,350.001 $7,427.59 $1,860.65 $5,566.94";
    const recovery = recoverKansasOcrCover(extractKansasOcrMoneyValues(text));
    expect(recovery).toMatchObject({ receiptsCents: 435000, usedUncertainRead: true });
  });

  it("returns null when no 5-tuple satisfies both identities", () => {
    expect(recoverKansasOcrCover(extractKansasOcrMoneyValues("$1.00 $2.00 $4.00"))).toBeNull();
  });

  it("returns null when two distinct value-tuples both satisfy the identities", () => {
    // 1+2=3, 3-1=2 twice over with different values.
    const text = "$1.00 $2.00 $3.00 $1.00 $2.00 $10.00 $20.00 $30.00 $10.00 $20.00";
    expect(recoverKansasOcrCover(extractKansasOcrMoneyValues(text))).toBeNull();
  });

  it("sends over-long money-value lists to the manual queue instead of searching", () => {
    // A $0.00-dense document satisfies the identities combinatorially; the
    // tuple search must refuse rather than grind through it.
    const values = extractKansasOcrMoneyValues(Array(101).fill("$0.00").join(" "));
    expect(values).toHaveLength(101);
    expect(recoverKansasOcrCover(values)).toBeNull();
  });
});

describe("reconcileKansasIeStatements", () => {
  const comeback = [
    { label: "KC2", periodKey: "p1", rowAmountsCents: [35963300, 1081063], totalThisPeriodCents: 37044363 },
    { label: "KC1", periodKey: "p1", rowAmountsCents: [850000], totalThisPeriodCents: 37894363 },
    { label: "KC3", periodKey: "p1", rowAmountsCents: [500000], totalThisPeriodCents: 38394363 },
    { label: "KC4", periodKey: "p2", rowAmountsCents: [13827000], totalThisPeriodCents: 13827000 },
  ];

  it("validates cumulative-within-period totals and the period reset", () => {
    const result = reconcileKansasIeStatements(comeback);
    expect(result.ok).toBe(true);
    expect(result.totalRowCents).toBe(52221363);
  });

  it("fails when a stated total does not match the running sum", () => {
    const broken = comeback.map((statement) =>
      statement.label === "KC3" ? { ...statement, totalThisPeriodCents: 38394364 } : statement
    );
    const result = reconcileKansasIeStatements(broken);
    expect(result.ok).toBe(false);
    expect(result.failures[0]).toContain("KC3");
  });

  it("fails if a new period does NOT reset (guards against cross-period summing)", () => {
    const noReset = comeback.map((statement) =>
      statement.label === "KC4" ? { ...statement, totalThisPeriodCents: 52221363 } : statement
    );
    expect(reconcileKansasIeStatements(noReset).ok).toBe(false);
  });
});
