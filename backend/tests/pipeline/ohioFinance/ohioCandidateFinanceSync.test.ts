import { describe, expect, it, vi } from "vitest";

import { syncOhioCandidateFinance } from "../../../src/pipeline/ohioFinance/ohioCandidateFinanceSync.js";
import type { OhioDirectContributionAggregationResult } from "../../../src/pipeline/ohioFinance/ohioDirectContributionAggregator.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73";

function createMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
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

function directFinance(
  overrides: Partial<OhioDirectContributionAggregationResult> = {}
): OhioDirectContributionAggregationResult {
  return {
    summary: {
      totalReceipts: 5350,
      directContributionTotal: 5250,
      totalDisbursements: 1100,
      cashOnHand: 4250,
      sourceUrl: SOURCE_URL,
    },
    directBreakdowns: [
      {
        categoryType: "contribution_size",
        categoryName: "$5,000+",
        amount: 5000,
        contributorCount: 1,
        sourceUrl: SOURCE_URL,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$250-$499",
        amount: 250,
        contributorCount: 1,
        sourceUrl: SOURCE_URL,
      },
    ],
    matchedContributionRowCount: 3,
    includedContributionRowCount: 2,
    skippedContributionRowCount: 1,
    missingAmountRowCount: 0,
    nonPositiveAmountRowCount: 0,
    outOfCycleRowCount: 0,
    otherIncomeRowCount: 1,
    unknownShortDescriptionRowCount: 0,
    coverReportCount: 2,
    blankCoverRowCount: 0,
    negativeBalanceOnHand: false,
    itemizedReceiptsTotal: 5350,
    coverReceiptsTotal: 5350,
    ...overrides,
  };
}

function baseInput(db: ReturnType<typeof createMockDb>) {
  return {
    db,
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Daniel Kalmbach",
    electionYear: 2026,
    officeName: "State Lower Chamber Legislator",
    district: "87",
    committee: {
      committeeId: "15877",
      committeeName: "CITIZENS FOR KALMBACH",
      sourceUrl: SOURCE_URL,
    },
    directFinance: directFinance(),
    outsideFinance: {
      supportTotal: 1000,
      opposeTotal: 0,
      groups: [
        {
          committeeId: "1792",
          committeeName: "NFIB OHIO PAC",
          supportOppose: "support" as const,
          amount: 1000,
          sourceUrl: SOURCE_URL,
        },
      ],
    },
    now: new Date("2026-08-05T12:00:00.000Z"),
  };
}

function executedSql(db: ReturnType<typeof createMockDb>): string {
  return db.query.mock.calls.map((call) => String(call[0])).join("\n");
}

describe("syncOhioCandidateFinance", () => {
  it("writes the link, summary, direct breakdowns, and outside groups", async () => {
    const db = createMockDb();
    const result = await syncOhioCandidateFinance(baseInput(db));

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      committeeId: "15877",
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      totalReceipts: 5350,
      directContributionTotal: 5250,
      totalDisbursements: 1100,
      cashOnHand: 4250,
      outsideSupportTotal: 1000,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 3,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
      negativeBalanceOnHand: false,
    });

    const sql = executedSql(db);
    expect(sql).toContain("oh_candidate_finance_links");
    expect(sql).toContain("oh_candidate_finance_summaries");
    expect(sql).toContain("oh_candidate_finance_direct_breakdowns");
    expect(sql).toContain("oh_candidate_finance_outside_groups");
  });

  it("leaves outside-group rows untouched when outside data is unavailable", async () => {
    const db = createMockDb();
    const result = await syncOhioCandidateFinance({
      ...baseInput(db),
      outsideFinance: null,
    });

    expect(result.outsideSupportTotal).toBeNull();
    expect(result.outsideOpposeTotal).toBeNull();
    expect(result.outsideGroupsWritten).toBe(0);
    expect(executedSql(db)).not.toContain("oh_candidate_finance_outside_groups");
  });

  it("clears stale outside groups when the aggregation ran and found none", async () => {
    const db = createMockDb();
    const result = await syncOhioCandidateFinance({
      ...baseInput(db),
      outsideFinance: { supportTotal: 0, opposeTotal: 0, groups: [] },
    });

    expect(result.outsideSupportTotal).toBe(0);
    expect(result.outsideOpposeTotal).toBe(0);
    expect(result.outsideGroupsWritten).toBe(0);
    // The delete-stale pass must run so previously stored groups do not
    // survive a cycle where the committee stopped spending.
    expect(executedSql(db)).toContain("oh_candidate_finance_outside_groups");
  });

  it("does not touch the database on a dry run", async () => {
    const db = createMockDb();
    const result = await syncOhioCandidateFinance({
      ...baseInput(db),
      dryRun: true,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      totalReceipts: 5350,
      outsideSupportTotal: 1000,
    });
    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("writes a manual link back as manual instead of claiming sos_bulk_export", async () => {
    const db = createMockDb();
    const input = baseInput(db);
    await syncOhioCandidateFinance({
      ...input,
      committee: { ...input.committee, linkSource: "manual" },
    });

    const linkUpsertParams = db.query.mock.calls
      .filter((call) => String(call[0]).includes("oh_candidate_finance_links"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(linkUpsertParams).toContain("manual");
    expect(linkUpsertParams).not.toContain("sos_bulk_export");
  });

  it("rejects a non-numeric committee id before writing anything", async () => {
    const db = createMockDb();
    await expect(
      syncOhioCandidateFinance({
        ...baseInput(db),
        committee: { committeeId: "MD-123", committeeName: "WRONG STATE", sourceUrl: null },
      })
    ).rejects.toThrow(/numeric Ohio SOS master key/);
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("validates the required identity fields", async () => {
    const db = createMockDb();
    await expect(syncOhioCandidateFinance({ ...baseInput(db), candidateId: " " })).rejects.toThrow(
      "candidate id is required"
    );
    await expect(syncOhioCandidateFinance({ ...baseInput(db), electionYear: 1990 })).rejects.toThrow(
      "Invalid Ohio finance sync election year"
    );
  });
});
