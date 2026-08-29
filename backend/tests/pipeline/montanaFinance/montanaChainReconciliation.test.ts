import { describe, expect, it } from "vitest";

import type {
  MontanaCersDetailRow,
  MontanaCersReportDetailArtifact,
  MontanaCersReportInventoryRow,
} from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import { MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import {
  MONTANA_CHAIN_LUMP_ABSOLUTE_FLOOR_CENTS,
  computeMontanaReportCashFlows,
  reconcileMontanaCashBeginChain,
  type MontanaChainReport,
} from "../../../src/pipeline/montanaFinance/montanaChainReconciliation.js";

function detailRow(overrides: Partial<MontanaCersDetailRow>): MontanaCersDetailRow {
  return {
    amountTypeDescr: "Primary",
    cashAmtCents: 0,
    inKindAmtCents: 0,
    totalAmtCents: 0,
    debtAmtCents: 0,
    entityName: "Doe, Jane",
    occupationDescr: null,
    employerDescr: null,
    datePaid: null,
    lineItemCompositeDescr: null,
    purposeDescr: null,
    electioneeringInd: "N",
    candidateContrInd: "N",
    ...overrides,
  };
}

function emptyArtifact(reportId: number): MontanaCersReportDetailArtifact {
  const lists = Object.fromEntries(
    MONTANA_CERS_ALL_CANDIDATE_DETAIL_LISTS.map((name) => [name, [] as MontanaCersDetailRow[]])
  ) as MontanaCersReportDetailArtifact["lists"];
  return { reportId, lists };
}

function inventoryRow(overrides: Partial<MontanaCersReportInventoryRow>): MontanaCersReportInventoryRow {
  return {
    reportId: 1,
    entitySubId: 21020,
    formTypeCode: "C5",
    formTypeDescr: null,
    fromDateStr: "01/01/2026",
    toDateStr: "03/15/2026",
    reportTypeDescr: "Periodic",
    statusCode: "FILED",
    statusDescr: "Filed",
    primCashBegCents: 0,
    genCashBegCents: 0,
    receivedDate: 1_000,
    amendedDate: null,
    ...overrides,
  };
}

function report(input: {
  reportId: number;
  primBeginCents: number;
  genBeginCents?: number;
  primaryInCents?: number;
  primaryOutCents?: number;
  generalInCents?: number;
  generalOutCents?: number;
}): MontanaChainReport {
  return {
    inventory: inventoryRow({
      reportId: input.reportId,
      primCashBegCents: input.primBeginCents,
      genCashBegCents: input.genBeginCents ?? 0,
    }),
    flows: {
      inflowCashCents: { primary: input.primaryInCents ?? 0, general: input.generalInCents ?? 0 },
      outflowCashCents: { primary: input.primaryOutCents ?? 0, general: input.generalOutCents ?? 0 },
    },
  };
}

describe("computeMontanaReportCashFlows", () => {
  it("sums cash only, by side, with the plan's list classification", () => {
    const artifact = emptyArtifact(76535);
    artifact.lists.individual = [
      detailRow({ cashAmtCents: 10_000, totalAmtCents: 10_000 }),
      // In-kind never touches the bank.
      detailRow({ inKindAmtCents: 17_220, totalAmtCents: 17_220 }),
      detailRow({ amountTypeDescr: "General", cashAmtCents: 5_000, totalAmtCents: 5_000 }),
    ];
    // Cash loan proceeds and misc receipts (refunds family) are chain
    // inflows even though directContributionTotal excludes them.
    artifact.lists.loan = [detailRow({ cashAmtCents: 1_000_000, totalAmtCents: 1_000_000 })];
    artifact.lists.refunds = [detailRow({ cashAmtCents: 33, totalAmtCents: 33 })];
    // Debt tracking feeds neither side.
    artifact.lists.debtLoan = [detailRow({ debtAmtCents: 18_216 })];
    artifact.lists.expendOther = [detailRow({ cashAmtCents: 59_060, totalAmtCents: 59_060 })];
    artifact.lists.payment = [detailRow({ amountTypeDescr: "General", cashAmtCents: 2_500, totalAmtCents: 2_500 })];

    const flows = computeMontanaReportCashFlows(artifact);
    expect(flows.inflowCashCents).toEqual({ primary: 1_010_033, general: 5_000 });
    expect(flows.outflowCashCents).toEqual({ primary: 59_060, general: 2_500 });
  });
});

describe("reconcileMontanaCashBeginChain", () => {
  it("passes when the chain closes to the cent (live-verified shape)", () => {
    // Bedey 76535 -> 79373, verified live 2026-08-27:
    // 17,840.09 + 17,030.33 - 15,642.08 = 19,228.34 exactly.
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 76535, primBeginCents: 1_784_009, genBeginCents: 5_000, primaryInCents: 1_703_033, primaryOutCents: 1_564_208 }),
      report({ reportId: 77491, primBeginCents: 1_922_834, genBeginCents: 5_000 }),
    ]);
    expect(result.ok).toBe(true);
    expect(result.links).toHaveLength(2);
    expect(result.links.find((link) => link.side === "primary")!.lumpCents).toBe(0);
    expect(result.links.find((link) => link.side === "general")!.lumpCents).toBe(0);
    expect(result.derivedUnitemizedTotalCents).toBe(0);
  });

  it("treats a small positive residual as the derived unitemized lump", () => {
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 0, primaryInCents: 100_000, primaryOutCents: 40_000 }),
      // Next begin is $49 above the itemized-derived ending: hidden
      // small-donor money -> POSITIVE lump (the plan's sign convention).
      report({ reportId: 2, primBeginCents: 64_900 }),
    ]);
    expect(result.ok).toBe(true);
    expect(result.links.find((link) => link.side === "primary")!.lumpCents).toBe(4_900);
    expect(result.derivedUnitemizedTotalCents).toBe(4_900);
  });

  it("fails closed on a negative residual", () => {
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 0, primaryInCents: 100_000 }),
      report({ reportId: 2, primBeginCents: 66_420 }),
    ]);
    expect(result.ok).toBe(false);
    expect(result.links.find((link) => link.side === "primary")).toMatchObject({
      lumpCents: -33_580,
      failure: "negative_residual",
    });
    expect(result.derivedEndingBalanceCents).toBeNull();
    expect(result.derivedUnitemizedTotalCents).toBeNull();
  });

  it("fails closed on a residual too large for the receipts", () => {
    const inflow = 400_000;
    const excessive = Math.round(inflow * 0.25) + MONTANA_CHAIN_LUMP_ABSOLUTE_FLOOR_CENTS;
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 0, primaryInCents: inflow }),
      report({ reportId: 2, primBeginCents: inflow + excessive }),
    ]);
    expect(result.ok).toBe(false);
    expect(result.links.find((link) => link.side === "primary")!.failure).toBe("excessive_residual");
  });

  it("accepts a lump under the absolute floor even with tiny receipts", () => {
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 0, primaryInCents: 100 }),
      report({ reportId: 2, primBeginCents: 100 + MONTANA_CHAIN_LUMP_ABSOLUTE_FLOOR_CENTS }),
    ]);
    expect(result.ok).toBe(true);
  });

  it("fails closed when a begin anchor is missing", () => {
    const broken = report({ reportId: 1, primBeginCents: 0 });
    broken.inventory.genCashBegCents = null;
    const result = reconcileMontanaCashBeginChain([broken, report({ reportId: 2, primBeginCents: 0 })]);
    expect(result.ok).toBe(false);
  });

  it("derives the latest ending balance from the last report, both sides", () => {
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 100_000, primaryInCents: 50_000, primaryOutCents: 25_000 }),
      report({
        reportId: 2,
        primBeginCents: 125_000,
        genBeginCents: 5_000,
        primaryInCents: 10_000,
        generalInCents: 2_000,
        primaryOutCents: 4_000,
        generalOutCents: 1_000,
      }),
    ]);
    expect(result.ok).toBe(true);
    // 125,000 + 5,000 + 12,000 - 5,000; the last period's own lump is
    // unknowable and deliberately absent (accepted understatement).
    expect(result.derivedEndingBalanceCents).toBe(137_000);
  });

  it("accepts a side rollover via the combined equation (Eddy / Supreme Court shape)", () => {
    // CERS moved the whole balance to the general side after the primary:
    // prim 2,400 -> 0, gen 39 -> 2,127 while money is conserved (spent 312).
    const result = reconcileMontanaCashBeginChain([
      report({
        reportId: 77699,
        primBeginCents: 240_000,
        genBeginCents: 3_900,
        primaryOutCents: 31_200,
      }),
      report({ reportId: 79155, primBeginCents: 0, genBeginCents: 212_700 }),
    ]);
    expect(result.ok).toBe(true);
    expect(result.links).toEqual([
      expect.objectContaining({ side: "combined", lumpCents: 0, ok: true }),
    ]);
    expect(result.derivedEndingBalanceCents).toBe(212_700);
  });

  it("still fails closed when a rollover does not conserve money", () => {
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 240_000, genBeginCents: 3_900 }),
      // Combined balance shrank by more than any lump could explain.
      report({ reportId: 2, primBeginCents: 0, genBeginCents: 100_000 }),
    ]);
    expect(result.ok).toBe(false);
    // The per-side failures remain the diagnostics.
    expect(result.links.some((link) => link.side === "combined")).toBe(false);
  });

  it("reconciles a single report chain with no checkable links", () => {
    const result = reconcileMontanaCashBeginChain([
      report({ reportId: 1, primBeginCents: 100, primaryInCents: 50 }),
    ]);
    expect(result.ok).toBe(true);
    expect(result.links).toEqual([]);
    expect(result.derivedEndingBalanceCents).toBe(150);
  });
});
