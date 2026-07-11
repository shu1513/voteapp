import { describe, expect, it, vi } from "vitest";

import { syncNewYorkCandidateFinance } from "../../../src/pipeline/newYorkFinance/newYorkCandidateFinanceSync.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    release: vi.fn(),
  };
  return {
    query: vi.fn().mockResolvedValue({ rows: [], rowCount: 0 }),
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

const MATCHED_RESOLUTION = {
  status: "matched" as const,
  filerId: "16851",
  filerName: "Friends for Kathy Hochul",
  candidateFilerId: "27197",
  confidence: "exact" as const,
  source: "ny_soda_api" as const,
  sourceUrl: "https://data.ny.gov/d/7x2g-h32p",
};

const CFAR_GROUP = {
  filerId: "590891",
  filerName: "Citizens for Affordable Rates PAC",
  supportOppose: "support" as const,
  amount: 12_320_650.23,
  allocationCount: 47,
  sourceUrl: "https://data.ny.gov/d/e9ss-239a",
};

const OUTSIDE_COUNTERS = {
  allocationRowCount: 51,
  nameMatchedRowCount: 51,
  duplicateTransactionRowCount: 0,
  nonIeCommitteeRowCount: 4,
  unresolvedMappingRowCount: 5,
  acceptedRowCount: 42,
};

function baseSyncInput(db: ReturnType<typeof createMockDb>) {
  return {
    db,
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Kathy Hochul",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
  };
}

describe("syncNewYorkCandidateFinance", () => {
  it("writes an outside-spending-only snapshot with rule-classified industries", async () => {
    const db = createMockDb();
    const nyClient = {
      searchAndResolveCandidateCommittee: vi.fn(async () => MATCHED_RESOLUTION),
      collectOutsideSpending: vi.fn(async () => ({ groups: [CFAR_GROUP], counters: OUTSIDE_COUNTERS })),
      getOutsideGroupFunderBreakdowns: vi.fn(async () => ({
        funders: [
          {
            categoryType: "donor" as const,
            categoryName: "Uber Technologies Inc.",
            amount: 11_686_700.23,
            contributorCount: 23,
            sourceUrl: "https://data.ny.gov/d/e9ss-239a",
          },
        ],
        receiptRowCount: 28,
        organizationRowCount: 28,
        skippedIndividualRowCount: 0,
      })),
    };

    const result = await syncNewYorkCandidateFinance({
      ...baseSyncInput(db),
      nyClient,
      sodaClientOptions: {},
      now: new Date("2026-07-11T12:00:00.000Z"),
    });

    expect(result).toMatchObject({
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      outsideSupportTotal: 12_320_650.23,
      outsideOpposeTotal: 0,
      outsideGroupCount: 1,
      outsideFunderRowCount: 28,
      outsideCounters: OUTSIDE_COUNTERS,
    });

    const statements = db.client.query.mock.calls.map((call) => [String(call[0]), call[1]] as const);
    const linkInsert = statements.find(([sql]) => sql.includes("INSERT INTO public.ny_candidate_finance_links"));
    expect(linkInsert?.[1]).toContain("16851");

    const summaryInsert = statements.find(([sql]) => sql.includes("INSERT INTO public.ny_candidate_finance_summaries"));
    // Phase 1: direct-campaign money fields stay null.
    expect(summaryInsert?.[1]?.slice(2, 6)).toEqual([null, null, null, null]);
    expect(summaryInsert?.[1]).toContain(12_320_650.23);

    const breakdownInserts = statements.filter(([sql]) =>
      sql.includes("INSERT INTO public.ny_candidate_finance_outside_group_breakdowns")
    );
    expect(breakdownInserts).toHaveLength(2);
    const categoryPairs = breakdownInserts.map(([, params]) => [params?.[4], params?.[5]]);
    expect(categoryPairs).toContainEqual(["donor", "Uber Technologies Inc."]);
    // The exact classifier rule pins Uber to transportation without AI.
    expect(categoryPairs).toContainEqual(["industry", "transportation"]);
  });

  it("returns an empty result without writes when the committee does not resolve", async () => {
    const db = createMockDb();
    const nyClient = {
      searchAndResolveCandidateCommittee: vi.fn(async () => ({
        status: "unmatched" as const,
        reason: "no_candidate_committee_match" as const,
        candidateNameNormalized: "KATHY HOCHUL",
        officeNameNormalized: "Governor",
      })),
      collectOutsideSpending: vi.fn(),
      getOutsideGroupFunderBreakdowns: vi.fn(),
    };

    const result = await syncNewYorkCandidateFinance({ ...baseSyncInput(db), nyClient, sodaClientOptions: {} });

    expect(result).toMatchObject({ linkWritten: false, outsideGroupCount: 0, outsideCounters: null });
    expect(nyClient.collectOutsideSpending).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("uses the trusted committee without re-resolving and fetches funders once per filer", async () => {
    const db = createMockDb();
    const nyClient = {
      searchAndResolveCandidateCommittee: vi.fn(),
      collectOutsideSpending: vi.fn(async () => ({
        groups: [CFAR_GROUP, { ...CFAR_GROUP, supportOppose: "oppose" as const, amount: 10 }],
        counters: OUTSIDE_COUNTERS,
      })),
      getOutsideGroupFunderBreakdowns: vi.fn(async () => ({
        funders: [],
        receiptRowCount: 0,
        organizationRowCount: 0,
        skippedIndividualRowCount: 0,
      })),
    };

    const result = await syncNewYorkCandidateFinance({
      ...baseSyncInput(db),
      nyClient,
      sodaClientOptions: {},
      trustedCommittee: { filerId: "16851", filerName: "Friends for Kathy Hochul" },
      dryRun: true,
    });

    expect(nyClient.searchAndResolveCandidateCommittee).not.toHaveBeenCalled();
    expect(nyClient.getOutsideGroupFunderBreakdowns).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      outsideGroupsWritten: 0,
      outsideSupportTotal: 12_320_650.23,
      outsideOpposeTotal: 10,
    });
    expect(db.connect).not.toHaveBeenCalled();
  });
});
