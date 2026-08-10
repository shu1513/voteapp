import { describe, expect, it, vi } from "vitest";

import { syncNorthCarolinaCandidateFinance } from "../../../src/pipeline/northCarolinaFinance/northCarolinaCandidateFinanceSync.js";
import type { NorthCarolinaDirectAggregationResult } from "../../../src/pipeline/northCarolinaFinance/northCarolinaDirectContributionAggregator.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = "https://cf.ncsbe.gov/CFOrgLkup/DocumentGeneralResult/?OGID=57190&SID=STA-AB12CD-C-001";

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
  overrides: Partial<NorthCarolinaDirectAggregationResult> = {}
): NorthCarolinaDirectAggregationResult {
  return {
    status: "ok",
    summary: {
      totalReceipts: 1500,
      directContributionTotal: 1200,
      totalDisbursements: 600,
      cashOnHand: 900,
      sourceUrl: SOURCE_URL,
    },
    directBreakdowns: [
      {
        categoryType: "occupation",
        categoryName: "Teacher",
        amount: 1100,
        contributorCount: 1,
        sourceUrl: SOURCE_URL,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$500-$999",
        amount: 800,
        contributorCount: 1,
        sourceUrl: SOURCE_URL,
      },
    ],
    selectedReportIds: ["300001", "300002"],
    supersededUnavailablePeriods: [],
    quarantinedGroups: [],
    missingReportIds: [],
    unusablePeriodRowCount: 0,
    itemizedReceiptsCents: 120_000,
    coverTotalReceiptsCents: 150_000,
    itemizedIndividualCents: 110_000,
    coverIndividualContributionCents: 120_000,
    cycleChainMismatches: [],
    coverIdentityMismatchReportIds: [],
    derivedBreakdownsQuarantined: false,
    unknownReceiptTypeCodes: [],
    includedIndividualRowCount: 2,
    aggregatedIndividualRowCount: 1,
    nonPositiveIndividualRowCount: 0,
    placeholderOccupationRowCount: 0,
    placeholderOccupationCents: 0,
    occupationAttributedCents: 110_000,
    fortyEightHourNoticeSumCents: 0,
    negativeCashOnHand: false,
    ieTypedRegularReportRowCount: 0,
    ieTypedRegularReportCents: 0,
    ...overrides,
  };
}

function baseInput(db: ReturnType<typeof createMockDb>) {
  return {
    db,
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeName: "State Lower Chamber Legislator",
    district: "27",
    committee: {
      committeeId: "STA-AB12CD-C-001",
      committeeName: "COMMITTEE TO ELECT JANE DOE",
      sourceUrl: SOURCE_URL,
    },
    directFinance: directFinance(),
    outsideFinance: {
      supportTotal: 150,
      opposeTotal: 100,
      groups: [
        {
          committeeId: "NC-IE-FILER:0000",
          committeeName: "ADVANCE CAROLINA ACTION",
          supportOppose: "support" as const,
          amount: 150,
          sourceUrl: SOURCE_URL,
        },
      ],
    },
    now: new Date("2026-08-07T12:00:00.000Z"),
  };
}

function executedSql(db: ReturnType<typeof createMockDb>): string {
  return db.query.mock.calls.map((call) => String(call[0])).join("\n");
}

function executedParams(db: ReturnType<typeof createMockDb>): string {
  return JSON.stringify(db.query.mock.calls.map((call) => call[1] ?? []));
}

describe("syncNorthCarolinaCandidateFinance", () => {
  it("writes the link, summary, direct breakdowns, and outside groups", async () => {
    const db = createMockDb();
    const result = await syncNorthCarolinaCandidateFinance(baseInput(db));

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      committeeId: "STA-AB12CD-C-001",
      directStatus: "ok",
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      totalReceipts: 1500,
      directContributionTotal: 1200,
      totalDisbursements: 600,
      cashOnHand: 900,
      outsideSupportTotal: 150,
      outsideOpposeTotal: 100,
      selectedReportCount: 2,
    });

    const sql = executedSql(db);
    expect(sql).toContain("nc_candidate_finance_links");
    expect(sql).toContain("nc_candidate_finance_summaries");
    expect(sql).toContain("nc_candidate_finance_direct_breakdowns");
    expect(sql).toContain("nc_candidate_finance_outside_groups");
    // Auto flows default to the portal provenance.
    expect(executedParams(db)).toContain('"ncsbe_portal"');
  });

  it("keeps a manual link's provenance when the due row carries it", async () => {
    const db = createMockDb();
    await syncNorthCarolinaCandidateFinance({
      ...baseInput(db),
      committee: { ...baseInput(db).committee, linkSource: "manual" as const },
    });
    expect(executedParams(db)).toContain('"manual"');
  });

  it("writes nothing on a dry run", async () => {
    const db = createMockDb();
    const result = await syncNorthCarolinaCandidateFinance({ ...baseInput(db), dryRun: true });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
    });
    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("leaves stored outside data alone when the outside leg was unavailable", async () => {
    const db = createMockDb();
    const result = await syncNorthCarolinaCandidateFinance({ ...baseInput(db), outsideFinance: null });

    expect(result).toMatchObject({
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      outsideGroupsWritten: 0,
    });
    // Undefined outsideGroups → the writer never touches the group tables.
    expect(executedSql(db)).not.toContain("nc_candidate_finance_outside_groups");
  });

  it("writes the honest-null snapshot when the aggregator proves supersession", async () => {
    const db = createMockDb();
    const result = await syncNorthCarolinaCandidateFinance({
      ...baseInput(db),
      directFinance: directFinance({
        status: "honest_null",
        summary: {
          totalReceipts: null,
          directContributionTotal: null,
          totalDisbursements: null,
          cashOnHand: null,
          sourceUrl: SOURCE_URL,
        },
        directBreakdowns: [],
        supersededUnavailablePeriods: [
          { reportType: "First Quarter", periodStartRaw: "01/01/2026", periodEndRaw: "02/14/2026" },
        ],
      }),
    });

    expect(result).toMatchObject({
      directStatus: "honest_null",
      summaryWritten: true,
      directBreakdownsWritten: 0,
      totalReceipts: null,
      cashOnHand: null,
      supersededUnavailablePeriodCount: 1,
    });
    // The write happens — stale money must not stay visible.
    expect(executedSql(db)).toContain("nc_candidate_finance_summaries");
  });

  it("writes funder breakdowns and persists classification rows for the manual queue", async () => {
    const db = createMockDb();
    const input = baseInput(db);
    const result = await syncNorthCarolinaCandidateFinance({
      ...input,
      outsideFinance: {
        ...input.outsideFinance,
        funders: {
          breakdowns: [
            {
              committeeId: "NC-IE-FILER:0000",
              supportOppose: "support" as const,
              categoryType: "donor" as const,
              categoryName: "ROLLING SEA FUND",
              amount: 24_506,
              contributorCount: 1,
              sourceUrl: SOURCE_URL,
            },
          ],
          matchedReceiptRowCount: 2,
          includedReceiptRowCount: 1,
          skippedReceiptRowCount: 1,
        },
      },
    });

    expect(result).toMatchObject({
      outsideGroupBreakdownsWritten: 1,
      matchedOutsideReceiptRowCount: 2,
      includedOutsideReceiptRowCount: 1,
      skippedOutsideReceiptRowCount: 1,
    });
    const sql = executedSql(db);
    expect(sql).toContain("nc_candidate_finance_outside_group_breakdowns");
    // The unresolved donor persists an 'unknown' classification row — the
    // manual industry-label queue reads exactly these.
    expect(sql).toContain("finance_label_classifications");
    const classificationParams = db.client.query.mock.calls
      .filter((call) => String(call[0]).includes("INSERT INTO public.finance_label_classifications"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(classificationParams).toContain("ROLLING SEA FUND");
    expect(classificationParams).toContain("unknown");
  });

  it("classifies every donor but caps the persisted donor rows per group", async () => {
    const db = createMockDb();
    const input = baseInput(db);
    const result = await syncNorthCarolinaCandidateFinance({
      ...input,
      // Cap of 1: the smaller IBEW donor must be dropped from the WRITTEN
      // donor rows, yet still feed the classifications and the rebuilt
      // labor_unions industry total.
      outsideMaxDonorBreakdownsPerGroup: 1,
      outsideFinance: {
        ...input.outsideFinance,
        funders: {
          breakdowns: [
            {
              committeeId: "NC-IE-FILER:0000",
              supportOppose: "support" as const,
              categoryType: "donor" as const,
              categoryName: "IBEW LOCAL 540",
              amount: 50_000,
              contributorCount: 1,
              sourceUrl: SOURCE_URL,
            },
            {
              committeeId: "NC-IE-FILER:0000",
              supportOppose: "support" as const,
              categoryType: "donor" as const,
              categoryName: "IBEW LOCAL 8",
              amount: 25_000,
              contributorCount: 1,
              sourceUrl: SOURCE_URL,
            },
          ],
          matchedReceiptRowCount: 2,
          includedReceiptRowCount: 2,
          skippedReceiptRowCount: 0,
        },
      },
    });

    // 1 capped donor row + 1 industry row built from BOTH donors.
    expect(result.outsideGroupBreakdownsWritten).toBe(2);
    const breakdownInsertParams = db.client.query.mock.calls
      .filter((call) => String(call[0]).includes("nc_candidate_finance_outside_group_breakdowns"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(breakdownInsertParams).toContain("IBEW LOCAL 540");
    expect(breakdownInsertParams).not.toContain("IBEW LOCAL 8");
    // The rebuilt industry total covers the dropped donor too.
    expect(breakdownInsertParams).toContain("labor_unions");
    expect(breakdownInsertParams).toContain(75_000);
    // Both donors persisted classification rows.
    const classificationParams = db.client.query.mock.calls
      .filter((call) => String(call[0]).includes("INSERT INTO public.finance_label_classifications"))
      .flatMap((call) => (Array.isArray(call[1]) ? call[1] : []));
    expect(classificationParams).toContain("IBEW LOCAL 540");
    expect(classificationParams).toContain("IBEW LOCAL 8");
  });

  it("keeps stored funder breakdowns when the funder leg is unavailable", async () => {
    const db = createMockDb();
    const input = baseInput(db);
    const result = await syncNorthCarolinaCandidateFinance({
      ...input,
      outsideFinance: { ...input.outsideFinance, funders: null },
    });

    expect(result.outsideGroupBreakdownsWritten).toBe(0);
    // Groups still refresh; the breakdowns table is never touched directly
    // (no upsert, no delete-stale pass).
    const sql = executedSql(db);
    expect(sql).toContain("nc_candidate_finance_outside_groups");
    expect(sql).not.toContain("nc_candidate_finance_outside_group_breakdowns");
  });

  it("clears stale funder breakdowns when the funder aggregation ran and found none", async () => {
    const db = createMockDb();
    const input = baseInput(db);
    const result = await syncNorthCarolinaCandidateFinance({
      ...input,
      outsideFinance: {
        ...input.outsideFinance,
        funders: {
          breakdowns: [],
          matchedReceiptRowCount: 0,
          includedReceiptRowCount: 0,
          skippedReceiptRowCount: 0,
        },
      },
    });

    expect(result.outsideGroupBreakdownsWritten).toBe(0);
    // The delete-stale pass must run so previously stored funder rows do
    // not survive a refresh where the donors disappeared.
    expect(executedSql(db)).toContain("nc_candidate_finance_outside_group_breakdowns");
  });

  it("refuses to write incomplete artifacts and names the suspects", async () => {
    const db = createMockDb();
    await expect(
      syncNorthCarolinaCandidateFinance({
        ...baseInput(db),
        directFinance: directFinance({
          status: "incomplete_artifacts",
          missingReportIds: ["300002"],
          coverIdentityMismatchReportIds: ["300001"],
        }),
      })
    ).rejects.toThrow(/missing report 300002.*mispaired cover for report 300001/);
    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });
});
