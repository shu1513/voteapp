import { describe, expect, it } from "vitest";

import type { KansasReportCover } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import { aggregateKansasCoverTotals } from "../../../src/pipeline/kansasFinance/kansasDirectContributionAggregator.js";
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

const ANNUAL = { periodStart: "1/1/2025", periodEnd: "12/31/2025" };
const PRE_PRIMARY = { periodStart: "1/1/2026", periodEnd: "7/23/2026" };

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

describe("aggregateKansasCoverTotals", () => {
  it("sums lines 2, 4 and 6 over the filed periods and takes cash on hand from the latest cover", () => {
    const annual = header({ ...ANNUAL, fileDate: "01/09/2026" });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const lastMinute = header({ periodStart: "7/24/2026", periodEnd: "7/29/2026", fileDate: "07/30/2026" });
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([annual, prePrimary, lastMinute]),
      covers: [
        { header: annual, cover: cover({ begin: 10_000, receipts: 250_000, spent: 60_000, inKind: 1_500 }) },
        { header: prePrimary, cover: cover({ begin: 200_000, receipts: 100_050, spent: 150_000, inKind: 22_328 }) },
        // Last-minute reports duplicate into the next regular report: never counted even when a cover is on hand.
        { header: lastMinute, cover: cover({ begin: 150_050, receipts: 999_999, spent: 0 }) },
      ],
    });
    expect(result).toEqual({
      status: "ok",
      totalReceiptsCents: 350_050,
      totalDisbursementsCents: 210_000,
      inKindCents: 23_828,
      cashOnHandCents: 150_050,
      diagnostics: [],
      periods: [
        { key: "2025-annual", status: "report_filed", cover: expect.objectContaining({ cashCloseCents: 200_000 }) },
        { key: "2026-pre_primary", status: "report_filed", cover: expect.objectContaining({ cashCloseCents: 150_050 }) },
        { key: "2026-pre_general", status: "not_yet_due", cover: null },
        { key: "2026-post_general", status: "not_yet_due", cover: null },
      ],
    });
  });

  it("counts only the canonical version of an amended period", () => {
    const original = header({ ...ANNUAL, fileDate: "01/09/2026" });
    const amendment = header({ ...ANNUAL, fileDate: "01/09/2026", amendmentDate: "01/15/2026", amended: true });
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([original, amendment]),
      covers: [
        { header: original, cover: cover({ begin: 0, receipts: 100_000, spent: 0 }) },
        { header: amendment, cover: cover({ begin: 0, receipts: 120_000, spent: 5_000 }) },
      ],
    });
    // The ledger is incomplete (pre-primary missing), so only the period rows are checked here.
    expect(result.status).toBe("unpublishable");
    expect(result.periods[0]).toEqual({ key: "2025-annual", status: "amended", cover: expect.objectContaining({ totalContributionsCents: 120_000 }) });
  });

  it("is unpublishable when the ledger is incomplete, naming the reason", () => {
    const annual = header({ ...ANNUAL });
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([annual]),
      covers: [{ header: annual, cover: cover({ begin: 0, receipts: 1, spent: 0 }) }],
    });
    expect(result).toMatchObject({ status: "unpublishable", reasons: ["ledger incomplete"] });
  });

  it("is unpublishable when a canonical cover fails its arithmetic or lacks line 6", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const broken = { ...cover({ begin: 0, receipts: 100, spent: 0 }), cashCloseCents: 99 };
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([annual, prePrimary]),
      covers: [
        { header: annual, cover: broken },
        { header: prePrimary, cover: { ...cover({ begin: 100, receipts: 0, spent: 0 }), inKindCents: null } },
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
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([annual, paper]),
      covers: [{ header: annual, cover: cover({ begin: 0, receipts: 100, spent: 0 }) }],
    });
    expect(result).toMatchObject({
      status: "unpublishable",
      reasons: ["2026-pre_primary: no opened cover for the canonical paper version"],
    });
  });

  it("is unpublishable when two opened covers claim the canonical version", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([annual, prePrimary]),
      covers: [
        { header: annual, cover: cover({ begin: 0, receipts: 100, spent: 0 }) },
        { header: { ...annual }, cover: cover({ begin: 0, receipts: 200, spent: 0 }) },
        { header: prePrimary, cover: cover({ begin: 100, receipts: 0, spent: 0 }) },
      ],
    });
    expect(result).toMatchObject({ status: "unpublishable", reasons: ["2025-annual: 2 opened covers match the canonical version"] });
  });

  it("reports a negative close as null cash on hand and a balance discontinuity as diagnostics", () => {
    const annual = header({ ...ANNUAL });
    const prePrimary = header({ ...PRE_PRIMARY, fileDate: "07/27/2026" });
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([annual, prePrimary]),
      covers: [
        { header: annual, cover: cover({ begin: 0, receipts: 100, spent: 0 }) },
        // Begins at 90, not the 100 the annual closed with; overspends to -10.
        { header: prePrimary, cover: cover({ begin: 90, receipts: 0, spent: 100 }) },
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

  it("publishes zeros with null cash for a cycle with no filed period (affidavit of exemption)", () => {
    const result = aggregateKansasCoverTotals({
      ledger: ledgerOf([], { affidavitDates: ["01/05/2026"] }),
      covers: [],
    });
    expect(result).toEqual({
      status: "ok",
      totalReceiptsCents: 0,
      totalDisbursementsCents: 0,
      inKindCents: 0,
      cashOnHandCents: null,
      diagnostics: [],
      periods: periods.map((period) => ({ key: period.key, status: "affidavit_exempt", cover: null })),
    });
  });
});
