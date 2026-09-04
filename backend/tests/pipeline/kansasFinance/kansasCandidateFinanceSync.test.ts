import { describe, expect, it, vi } from "vitest";

import { KANSAS_CFR_LINK_SOURCE_URL } from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceAutoLink.js";
import {
  kansasSnapshotFigures,
  syncKansasCandidateFinance,
  type KansasCandidateFinanceSyncInput,
} from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceSync.js";
import type { KansasCandidateLedgerResult, KansasCandidateReport } from "../../../src/pipeline/kansasFinance/kansasCandidateLedger.js";
import type { KansasReportCover, KansasScheduleARow, KansasScheduleBRow } from "../../../src/pipeline/kansasFinance/kansasCfrViewerParsers.js";
import type { KansasDirectFinance } from "../../../src/pipeline/kansasFinance/kansasDirectContributionAggregator.js";
import { kansasCfrOfficeForRace } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import {
  buildKansasReportLedger,
  kansasLastMinuteWindows,
  kansasReportingPeriods,
  type KansasFilingHeader,
} from "../../../src/pipeline/kansasFinance/kansasReportInventory.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-09-02T12:00:00.000Z");
const HOUSE = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
const periods = kansasReportingPeriods(HOUSE, 2026);
const ANNUAL = { periodStart: "1/1/2025", periodEnd: "12/31/2025" };
const PRE_PRIMARY = { periodStart: "1/1/2026", periodEnd: "7/23/2026" };

function header(overrides: Partial<KansasFilingHeader> & Pick<KansasFilingHeader, "periodStart" | "periodEnd">): KansasFilingHeader {
  return { fileDate: "01/09/2026", amendmentDate: null, amended: false, termination: false, channel: "efile", ...overrides };
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

/** An opened e-filed report whose cover and both schedules reconcile (line 2 = A rows + unitemized, line 6 = B rows). */
function report(
  filing: KansasFilingHeader,
  input: { begin: number; spent: number; rowsA?: KansasScheduleARow[]; unitemizedA?: number; rowsB?: KansasScheduleBRow[] }
): KansasCandidateReport {
  const rowsA = input.rowsA ?? [];
  const rowsB = input.rowsB ?? [];
  const itemizedA = rowsA.reduce((sum, row) => sum + row.amountCents!, 0);
  const inKind = rowsB.reduce((sum, row) => sum + row.valueCents!, 0);
  const receipts = itemizedA + (input.unitemizedA ?? 0);
  const cover: KansasReportCover = {
    candidateName: "HOLLOWAY MARGARET",
    officeSought: "State Representative",
    district: "85",
    periodStart: filing.periodStart,
    periodEnd: filing.periodEnd,
    amended: false,
    termination: false,
    electronicallyFiledOn: null,
    cashBeginningCents: input.begin,
    totalContributionsCents: receipts,
    cashAvailableCents: input.begin + receipts,
    totalExpendituresCents: input.spent,
    cashCloseCents: input.begin + receipts - input.spent,
    inKindCents: inKind,
    otherTransactionsCents: 0,
  };
  return {
    row: {
      index: 0,
      fileDate: filing.fileDate!,
      amendmentDate: "",
      amendmentNo: "",
      name: "HOLLOWAY MARGARET",
      officeSought: "State Representative",
      district: "85",
      channel: "efile",
      postbackTarget: "grdviewCfrResults$ctl02$lnkbtnName",
    },
    cover,
    header: filing,
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
      totals: { totalItemizedCents: inKind, totalUnitemizedCents: 0, totalInKindCents: inKind },
    },
  };
}

function resolved(reports: KansasCandidateReport[], extra: { affidavitDates?: string[]; complete?: boolean } = {}): KansasCandidateLedgerResult {
  const ledger = buildKansasReportLedger({
    periods,
    filings: reports.map((entry) => entry.header),
    appointmentsOfTreasurer: [],
    affidavitDates: extra.affidavitDates ?? [],
    lastMinuteWindows: kansasLastMinuteWindows(2026),
    now: NOW,
  });
  return {
    status: "resolved",
    match: { surname: "HOLLOWAY", firstName: "MARGARET", committeeName: "HOLLOWAY MARGARET", filedNames: ["HOLLOWAY MARGARET"], rowCount: reports.length, confidence: "name_exact" },
    reports,
    paperReports: [],
    paper: null,
    appointments: [],
    affidavitDates: extra.affidavitDates ?? [],
    ledger,
    complete: extra.complete ?? ledger.complete,
  };
}

/** The two filed periods of a complete House 2026 ledger on 2026-09-02: a loan, two occupations, one in-kind row. */
function filedReports(): KansasCandidateReport[] {
  return [
    report(header({ ...ANNUAL }), { begin: 0, spent: 1_000, rowsA: [aRow({ amountCents: 20_000, occupation: "Retired" })], unitemizedA: 5_000 }),
    report(header({ ...PRE_PRIMARY, fileDate: "07/27/2026" }), {
      begin: 24_000,
      spent: 50_000,
      rowsA: [aRow({ amountCents: 100_000, tenderType: "Loan" }), aRow({ amountCents: 15_000, occupation: "Farmer" })],
      rowsB: [bRow({ valueCents: 3_000, occupation: "retired" })],
    }),
  ];
}

function writingDb() {
  const client = {
    query: vi.fn((sql: unknown) =>
      String(sql).includes("INSERT INTO public.ks_candidate_finance_links")
        ? Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 })
        : Promise.resolve({ rows: [], rowCount: 0 })
    ),
    release: vi.fn(),
  };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

function baseInput(db: { query: unknown; connect: unknown }, ledger: KansasCandidateLedgerResult): {
  input: KansasCandidateFinanceSyncInput;
  buildLedger: ReturnType<typeof vi.fn>;
} {
  const buildLedger = vi.fn(async () => ledger);
  const loadFilingPool = vi.fn();
  const loadKpdcRows = vi.fn();
  return {
    buildLedger,
    input: {
      db: db as never,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Margaret Holloway",
      electionYear: 2026,
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      district: "85",
      link: { committeeId: "7:85:HOLLOWAY:MARGARET", committeeName: "HOLLOWAY MARGARET", linkSource: "manual", sourceUrl: null },
      now: NOW,
      loadFilingPool: loadFilingPool as never,
      loadKpdcRows: loadKpdcRows as never,
      buildLedger: buildLedger as never,
    },
  };
}

describe("syncKansasCandidateFinance", () => {
  it("builds the ledger with schedules, aggregates, and writes totals + buckets (direct = line 2 + line 6 − loans)", async () => {
    const { db, client } = writingDb();
    const { input, buildLedger } = baseInput(db, resolved(filedReports()));

    const result = await syncKansasCandidateFinance(input);

    expect(buildLedger).toHaveBeenCalledWith({
      target: { committeeId: "7:85:HOLLOWAY:MARGARET", committeeName: "HOLLOWAY MARGARET", office: HOUSE, electionYear: 2026 },
      now: NOW,
      loadFilingPool: input.loadFilingPool,
      loadKpdcRows: input.loadKpdcRows,
      openSchedules: true,
    });
    expect(result).toMatchObject({
      status: "synced",
      dryRun: false,
      committeeId: "7:85:HOLLOWAY:MARGARET",
      periods: { "2025-annual": "report_filed", "2026-pre_primary": "report_filed" },
      totalReceipts: 1400,
      directContributionTotal: 430,
      totalDisbursements: 510,
      cashOnHand: 890,
      coverage: { contributionCents: 38_000, occupationCoveredCents: 38_000, unitemizedCents: 5_000, nonContributionReceiptCents: 100_000 },
      breakdownCounts: { occupation: 2, contribution_size: 2 },
      diagnostics: [],
      summaryWritten: true,
      directBreakdownsWritten: 4,
    });

    const linkInsert = client.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_links"));
    expect(linkInsert?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "MARGARET HOLLOWAY",
      "State Lower Chamber Legislator",
      "85",
      "7:85:HOLLOWAY:MARGARET",
      "HOLLOWAY MARGARET",
      "active",
      "manual",
      KANSAS_CFR_LINK_SOURCE_URL,
      NOW.toISOString(),
    ]);
    const summaryInsert = client.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_summaries"));
    expect(summaryInsert?.[1]).toEqual([LINK_ID, 2026, 1400, 430, 510, 890, null, null, KANSAS_CFR_LINK_SOURCE_URL, NOW.toISOString()]);
    const breakdowns = client.query.mock.calls
      .filter(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_direct_breakdowns"))
      .map(([, params]) => (params as unknown[]).slice(2, 5));
    expect(breakdowns).toEqual([
      ["occupation", "Retired", 230],
      ["occupation", "Farmer", 150],
      ["contribution_size", "$1-$99", 30],
      ["contribution_size", "$100-$249", 350],
    ]);
    expect(client.query.mock.calls.some(([sql]) => String(sql).includes("ks_candidate_finance_outside_groups"))).toBe(false);
  });

  it("writes null figures (never $0) when every period is affidavit-exempt", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, resolved([], { affidavitDates: ["01/05/2026"] }));
    const result = await syncKansasCandidateFinance(input);
    expect(result).toMatchObject({
      status: "no_filed_report",
      periods: { "2025-annual": "affidavit_exempt", "2026-pre_primary": "affidavit_exempt" },
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      cashOnHand: null,
      coverage: null,
      breakdownCounts: { occupation: 0, contribution_size: 0 },
      summaryWritten: true,
      directBreakdownsWritten: 0,
    });
    const summaryInsert = client.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_summaries"));
    expect(summaryInsert?.[1]?.slice(2, 6)).toEqual([null, null, null, null]);
    expect(client.query.mock.calls.some(([sql]) => String(sql).includes("DELETE FROM public.ks_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("fails closed without touching the database when the candidate is unpublishable", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, resolved(filedReports(), { complete: false }));
    await expect(syncKansasCandidateFinance(input)).rejects.toThrow("unpublishable: ledger incomplete");
    expect(client.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("fails closed when the link no longer resolves or the office is not eligible", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, { status: "unresolved", reason: "no_matching_filer" });
    await expect(syncKansasCandidateFinance(input)).rejects.toThrow("7:85:HOLLOWAY:MARGARET does not resolve in the viewer: no_matching_filer");
    await expect(syncKansasCandidateFinance({ ...input, officeScope: "county", officeName: "County Attorney" })).rejects.toThrow("not Kansas-finance eligible");
    await expect(syncKansasCandidateFinance({ ...input, electionYear: 2023 })).rejects.toThrow("invalid election year");
    expect(client.query).not.toHaveBeenCalled();
  });

  it("dry run aggregates without touching the database", async () => {
    const { db, client } = writingDb();
    const { input } = baseInput(db, resolved(filedReports()));
    const result = await syncKansasCandidateFinance({ ...input, dryRun: true });
    expect(result).toMatchObject({ status: "synced", dryRun: true, totalReceipts: 1400, directContributionTotal: 430, summaryWritten: false, directBreakdownsWritten: 0 });
    expect(client.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });
});

describe("kansasSnapshotFigures", () => {
  const ok = (overrides: Partial<Extract<KansasDirectFinance, { status: "ok" }>> = {}): KansasDirectFinance => ({
    status: "ok",
    totalReceiptsCents: 140_000,
    totalDisbursementsCents: 51_000,
    inKindCents: 3_000,
    cashOnHandCents: null,
    itemized: { contributionCents: 38_000, occupationCoveredCents: 38_000, unitemizedCents: 5_000, nonContributionReceiptCents: 100_000, breakdowns: [] },
    diagnostics: ["cash on hand -1 is negative; reported as null"],
    periods: [],
    ...overrides,
  });

  it("reports no direct total when a counted cover came without schedules", () => {
    expect(kansasSnapshotFigures(ok({ itemized: null }))).toMatchObject({
      status: "synced",
      summary: { totalReceipts: 1400, directContributionTotal: null, totalDisbursements: 510, cashOnHand: null },
      coverage: null,
      diagnostics: ["cash on hand -1 is negative; reported as null", "no breakdowns: direct contribution total not reported"],
    });
  });

  it("refuses negative figures", () => {
    expect(() =>
      kansasSnapshotFigures(ok({ itemized: { contributionCents: 0, occupationCoveredCents: 0, unitemizedCents: 0, nonContributionReceiptCents: 150_000, breakdowns: [] } }))
    ).toThrow("direct contribution total is negative");
    expect(() => kansasSnapshotFigures(ok({ totalReceiptsCents: -1 }))).toThrow("total receipts is negative");
    expect(() => kansasSnapshotFigures({ status: "unpublishable", reasons: ["a", "b"], periods: [] })).toThrow("unpublishable: a; b");
  });
});
