import { describe, expect, it, vi } from "vitest";

import { syncTennesseeCandidateFinance } from "../../../src/pipeline/tennesseeFinance/tennesseeCandidateFinanceSync.js";
import type { TennesseeCampContributionRecord } from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";
import type { TennesseeCampExpenditureRecord } from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";

function contribution(overrides: Partial<TennesseeCampContributionRecord> = {}): TennesseeCampContributionRecord {
  return {
    type: "Monetary",
    adjustment: "N",
    amount: 250,
    date: "02/18/2022",
    electionYear: 2022,
    reportName: "1st Quarter",
    recipientName: "LEE, BILL",
    contributorName: "DOE, JANE",
    contributorOccupation: "Attorney",
    contributorEmployer: "Acme",
    ...overrides,
  };
}

function expenditure(overrides: Partial<TennesseeCampExpenditureRecord> = {}): TennesseeCampExpenditureRecord {
  return {
    type: "Independent",
    adjustment: "N",
    amount: 533,
    date: "10/01/2022",
    electionYear: 2022,
    reportName: "Pre-General",
    candidatePacName: "RIGHT TENNESSEE",
    vendorName: "Media Vendor",
    purpose: "Mail",
    candidateFor: "LEE, BILL",
    supportOpposeCode: "S",
    ...overrides,
  };
}

function createMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: "33333333-3333-4333-8333-333333333333" }], rowCount: 1 });
  const client = {
    query,
    release: vi.fn(),
  };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

describe("tennesseeCandidateFinanceSync", () => {
  it("aggregates and skips writes during dry runs", async () => {
    const db = { query: vi.fn() };
    await expect(
      syncTennesseeCandidateFinance({
        db,
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "Bill Lee",
        electionYear: 2022,
        officeName: "Governor",
        campCandidateId: "6496",
        ownerName: "LEE, BILL",
        contributions: [contribution()],
        expenditures: [expenditure()],
        outsideGroupContributionRecords: [
          contribution({
            amount: 50000,
            recipientName: "RIGHT TENNESSEE",
            contributorName: "TENNESSEE BANK PAC",
            contributorOccupation: null,
            contributorEmployer: null,
          }),
        ],
        now: new Date("2026-01-01T00:00:00.000Z"),
        dryRun: true,
      })
    ).resolves.toMatchObject({
      dryRun: true,
      linkWritten: false,
      totalReceipts: 250,
      directContributionTotal: 250,
      outsideSupportTotal: 533,
      outsideOpposeTotal: 0,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideGroupCount: 1,
      matchedOutsideContributionRowCount: 1,
      includedOutsideContributionRowCount: 1,
      skippedOutsideContributionRowCount: 0,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("preserves unknown outside totals as null when outside spending data is absent", async () => {
    const db = createMockDb();

    await syncTennesseeCandidateFinance({
      db,
      candidateId: "candidate-1",
      electionId: "election-1",
      candidateName: "Bill Lee",
      electionYear: 2022,
      officeName: "Governor",
      campCandidateId: "6496",
      ownerName: "LEE, BILL",
      contributions: [contribution()],
      now: new Date("2026-01-01T00:00:00.000Z"),
    });

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.tn_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]?.slice(4, 6)).toEqual([null, null]);
  });
});
