import { describe, expect, it, vi } from "vitest";

import type {
  NewHampshireFilingEntityRow,
  NewHampshireIndependentExpenditureRow,
  NewHampshireReceiptRow,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";
import {
  syncNewHampshireCandidateFinance,
  type NewHampshireCfsDataClient,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-08-21T12:00:00.000Z");

function filingEntity(
  overrides: Partial<NewHampshireFilingEntityRow> = {}
): NewHampshireFilingEntityRow {
  return {
    registrationGuid: "00000000-0000-4000-8000-000000000001",
    filingEntityId: 50_450,
    filerName: "Sample Candidate Committee",
    candidateName: "Sample Candidate",
    firstName: "Sample",
    lastName: "Candidate",
    committeeName: "Sample Candidate Committee",
    filerTypeCode: "PAC",
    filerSubTypeCode: "PACCC",
    filerSubTypeName: "Candidate Committee",
    officeName: "State Senate",
    county: null,
    district: "1",
    electionCycleId: 110,
    electionYear: 2026,
    electionCycle: "2026 Election Cycle",
    status: "Active",
    ...overrides,
  };
}

function receipt(overrides: Partial<NewHampshireReceiptRow> = {}): NewHampshireReceiptRow {
  return {
    transactionId: 1,
    transactionVersionId: 1,
    guid: "00000000-0000-4000-8000-000000000002",
    filerReportId: 10,
    filerReportVersionId: 1,
    filerEntityId: 50_450,
    filerName: "Sample Candidate Committee",
    transactionAmount: 100,
    transactionDate: "2026-07-01T00:00:00",
    transactionTypeDescription: "Receipt",
    transactionSubType: "Monetary Contribution",
    transactionSubTypeCode: "MTCB",
    reportName: "2026 R&E Report - 08/19/2026",
    reportVersion: false,
    reportVersionFilter: "RPTFLD",
    reportVersionDescription: "No",
    isAmended: false,
    electionCycle: "2026 Election Cycle",
    employerName: "Google",
    occupation: null,
    ...overrides,
  };
}

function expenditure(
  overrides: Partial<NewHampshireIndependentExpenditureRow> = {}
): NewHampshireIndependentExpenditureRow {
  return {
    transactionId: 2,
    transactionVersionId: 1,
    guid: "00000000-0000-4000-8000-000000000003",
    filerReportId: 20,
    filerReportVersionId: 1,
    filerEntityId: 70_070,
    filerName: "Example IE Committee",
    transactionAmount: 25,
    transactionDate: "2026-07-02T00:00:00",
    reportName: "2026 R&E Report - 08/19/2026",
    reportVersion: false,
    reportVersionFilter: "RPTFLD",
    isAmended: false,
    transactionTypeCode: "TIE",
    transactionSubTypeCode: "TIE",
    candidateMeasure: "Candidate, Sample",
    stance: "Support",
    electionCycle: "2026 Election Cycle",
    transactionCategory: "Canvassing",
    ...overrides,
  };
}

function createClient(overrides: Partial<NewHampshireCfsDataClient> = {}) {
  return {
    getFilingEntities: vi.fn().mockResolvedValue([filingEntity()]),
    getReceipts: vi.fn().mockResolvedValue([receipt()]),
    getIndependentExpenditures: vi.fn().mockResolvedValue([expenditure()]),
    ...overrides,
  };
}

function successfulQuery(sql: unknown) {
  if (String(sql).includes("INSERT INTO public.nh_candidate_finance_links")) {
    return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
  }
  return Promise.resolve({ rows: [], rowCount: 0 });
}

function createDb() {
  const client = { query: vi.fn(successfulQuery), release: vi.fn() };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { client, db };
}

function baseInput(input: {
  cfsClient: Partial<NewHampshireCfsDataClient>;
  db: ReturnType<typeof createDb>["db"];
  dryRun?: boolean;
}) {
  return {
    db: input.db as never,
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Sample Candidate",
    electionYear: 2026,
    electionCycleId: 110,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "District 1",
    cfsClient: input.cfsClient,
    now: NOW,
    dryRun: input.dryRun,
  };
}

describe("newHampshireCandidateFinanceSync", () => {
  it("resolves the official filer and writes both successful source sections once", async () => {
    const cfsClient = createClient();
    const { client, db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      resolution: { status: "matched", filingEntityId: 50_450 },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 25,
      outsideOpposeTotal: 0,
      directSkippedReason: null,
      outsideSkippedReason: null,
    });
    expect(cfsClient.getFilingEntities).toHaveBeenCalledWith(
      { electionCycleId: 110 },
      undefined
    );
    expect(cfsClient.getReceipts).toHaveBeenCalledWith(
      { filerName: "Sample Candidate Committee", electionCycleId: 110 },
      undefined
    );
    expect(cfsClient.getIndependentExpenditures).toHaveBeenCalledWith(
      { electionCycleId: 110 },
      undefined
    );
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("stops before money fetches and database writes when identity is unresolved", async () => {
    const cfsClient = createClient({
      getFilingEntities: vi.fn().mockResolvedValue([]),
    });
    const { db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      resolution: { status: "unmatched", reason: "no_candidate_filer_match" },
      linkWritten: false,
      summaryWritten: false,
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
    });
    expect(cfsClient.getReceipts).not.toHaveBeenCalled();
    expect(cfsClient.getIndependentExpenditures).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("skips outside attribution when the candidate name belongs to another registered race", async () => {
    const cfsClient = createClient({
      getFilingEntities: vi.fn().mockResolvedValue([
        filingEntity(),
        filingEntity({
          registrationGuid: "00000000-0000-4000-8000-000000000004",
          filingEntityId: 60_060,
          filerName: "Sample Candidate for County Treasurer",
          committeeName: null,
          filerTypeCode: "CAN",
          filerSubTypeCode: null,
          filerSubTypeName: null,
          officeName: "County Treasurer",
          county: "Merrimack",
          district: null,
        }),
      ]),
    });
    const { client, db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      resolution: { status: "matched", filingEntityId: 50_450, candidateAliases: [] },
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      outsideSkippedReason:
        "candidate name is ambiguous across registered New Hampshire race targets",
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 0,
    });
    expect(cfsClient.getReceipts).toHaveBeenCalledOnce();
    expect(cfsClient.getIndependentExpenditures).not.toHaveBeenCalled();
    expect(
      client.query.mock.calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_outside_groups")
      )
    ).toBe(false);
  });

  it("preserves the failed direct section while replacing successful outside spending", async () => {
    const cfsClient = createClient({
      getReceipts: vi.fn().mockRejectedValue(new Error("receipt API unavailable")),
    });
    const { client, db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: 25,
      outsideOpposeTotal: 0,
      directSkippedReason: "receipt API unavailable",
      outsideSkippedReason: null,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 1,
    });
    const calls = client.query.mock.calls;
    const summary = calls.find((call) =>
      String(call[0]).includes("nh_candidate_finance_summaries")
    );
    expect(summary?.[1]?.slice(2, 9)).toEqual([null, null, null, null, 25, 0, expect.any(String)]);
    expect(
      calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_direct_breakdowns")
      )
    ).toBe(false);
    expect(
      calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_outside_groups")
      )
    ).toBe(true);
  });

  it("preserves the failed outside section while replacing successful direct data", async () => {
    const cfsClient = createClient({
      getIndependentExpenditures: vi.fn().mockRejectedValue(new Error("IE API unavailable")),
    });
    const { client, db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      directSkippedReason: null,
      outsideSkippedReason: "IE API unavailable",
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 0,
    });
    const calls = client.query.mock.calls;
    const summary = calls.find((call) =>
      String(call[0]).includes("nh_candidate_finance_summaries")
    );
    expect(summary?.[1]?.slice(2, 9)).toEqual([100, 100, null, null, null, null, expect.any(String)]);
    expect(
      calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_direct_breakdowns")
      )
    ).toBe(true);
    expect(
      calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_outside_groups")
      )
    ).toBe(false);
  });

  it("does not write a misleading snapshot when both money sources fail", async () => {
    const cfsClient = createClient({
      getReceipts: vi.fn().mockRejectedValue(new Error("receipts failed")),
      getIndependentExpenditures: vi.fn().mockRejectedValue(new Error("IE failed")),
    });
    const { db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      resolution: { status: "matched" },
      linkWritten: false,
      summaryWritten: false,
      directSkippedReason: "receipts failed",
      outsideSkippedReason: "IE failed",
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("writes zeros and clears stale details after successful empty fetches", async () => {
    const cfsClient = createClient({
      getReceipts: vi.fn().mockResolvedValue([]),
      getIndependentExpenditures: vi.fn().mockResolvedValue([]),
    });
    const { client, db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      totalReceipts: 0,
      directContributionTotal: 0,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
    });
    expect(
      client.query.mock.calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_direct_breakdowns")
      )
    ).toBe(true);
    expect(
      client.query.mock.calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_outside_groups")
      )
    ).toBe(true);
  });

  it("clears classified outside totals when successful rows have no official stance", async () => {
    const cfsClient = createClient({
      getIndependentExpenditures: vi.fn().mockResolvedValue([
        expenditure({ stance: null }),
      ]),
    });
    const { client, db } = createDb();

    const result = await syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }));

    expect(result).toMatchObject({
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      outsideSkippedReason: null,
      outsideAggregation: {
        summary: null,
        matchedTargetRowCount: 1,
        blankStanceRowCount: 1,
      },
      outsideGroupsWritten: 0,
    });
    expect(
      client.query.mock.calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_outside_groups")
      )
    ).toBe(true);
  });

  it("computes a dry run without opening a database transaction", async () => {
    const cfsClient = createClient();
    const { db } = createDb();

    const result = await syncNewHampshireCandidateFinance(
      baseInput({ cfsClient, db, dryRun: true })
    );

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      totalReceipts: 100,
      outsideSupportTotal: 25,
    });
    expect(result.directAggregation).not.toBeNull();
    expect(result.outsideAggregation).not.toBeNull();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects a registration response from the wrong exact cycle before money fetches", async () => {
    const cfsClient = createClient({
      getFilingEntities: vi.fn().mockResolvedValue([
        filingEntity({
          electionYear: 2024,
          electionCycle: "2024 Election Cycle",
        }),
      ]),
    });
    const { db } = createDb();

    await expect(
      syncNewHampshireCandidateFinance(baseInput({ cfsClient, db }))
    ).rejects.toThrow("does not match 2026 Election Cycle");
    expect(cfsClient.getReceipts).not.toHaveBeenCalled();
    expect(cfsClient.getIndependentExpenditures).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });
});
