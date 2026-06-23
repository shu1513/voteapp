import { describe, expect, it, vi } from "vitest";

import { normalizeFinanceLabel, type FinanceLabelClassification } from "../../../src/pipeline/finance/financeLabelClassifier.js";
import { syncHawaiiCandidateFinance } from "../../../src/pipeline/hawaiiFinance/hawaiiCandidateFinanceSync.js";
import type { HawaiiCandidateCommitteeResolution } from "../../../src/pipeline/hawaiiFinance/hawaiiCandidateCommitteeResolver.js";
import type {
  HawaiiCscAggregate,
  HawaiiCscIndependentSpendingGroup,
} from "../../../src/pipeline/hawaiiFinance/hawaiiCscClient.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SOURCE_URL = "https://hicscdata.hawaii.gov/resource/jexd-xbcg.json";

function createMockDb() {
  const query = vi.fn(async (sql: string) => {
    if (String(sql).includes("FROM public.finance_label_classifications AS classification")) {
      return { rows: [], rowCount: 0 };
    }
    return { rows: [{ id: LINK_ID }], rowCount: 1 };
  });
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

function matchedResolution(overrides: Partial<Extract<HawaiiCandidateCommitteeResolution, { status: "matched" }>> = {}) {
  return {
    status: "matched" as const,
    committeeId: "CC10174",
    committeeName: "Green, Josh",
    electionPeriod: "2018-2022",
    totalAmount: 4070153.38,
    confidence: "exact" as const,
    source: "csc_api" as const,
    sourceUrl: SOURCE_URL,
    matchedSummaryRowCount: 1,
    ...overrides,
  };
}

function createCscClient(input: {
  resolution?: HawaiiCandidateCommitteeResolution;
  occupations?: HawaiiCscAggregate[];
  contributionSizes?: HawaiiCscAggregate[];
  outsideGroups?: HawaiiCscIndependentSpendingGroup[];
  outsideFunders?: HawaiiCscAggregate[];
} = {}) {
  return {
    searchAndResolveCandidateCommittee: vi.fn(async () => input.resolution ?? matchedResolution()),
    getDirectOccupationAggregates: vi.fn(async () =>
      input.occupations ?? [
        { categoryName: "Attorney", amount: 332962.31, count: 1200 },
        { categoryName: "Retired", amount: 257625.47, count: 900 },
      ]
    ),
    getContributionSizeAggregates: vi.fn(async () =>
      input.contributionSizes ?? [{ categoryName: "1000_4999", amount: 150000, count: 30 }]
    ),
    getIndependentExpenditureGroups: vi.fn(async () =>
      input.outsideGroups ?? [
        {
          committeeId: "NC101",
          committeeName: "Be Change Now",
          supportOppose: "support" as const,
          amount: 500557,
          expenditureCount: 1,
          electionPeriod: "2020-2022 (KP2)",
        },
        {
          committeeId: "NC202",
          committeeName: "Hawaii Future PAC",
          supportOppose: "oppose" as const,
          amount: 10000,
          expenditureCount: 1,
          electionPeriod: "2020-2022 (KP2)",
        },
      ]
    ),
    getNoncandidateCommitteeFunders: vi.fn(async () =>
      input.outsideFunders ?? [
        { categoryName: "Hawaii Carpenters Market Recovery Program Fund", amount: 2086436.92, count: 1 },
      ]
    ),
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Josh Green",
    electionYear: 2022,
    officeScope: "statewide",
    officeName: "Governor",
    sourceUrl: SOURCE_URL,
    now: new Date("2026-06-02T03:04:05.000Z"),
  };
}

describe("hawaiiCandidateFinanceSync", () => {
  it("resolves a candidate committee, fetches direct CSC aggregates, and writes a direct snapshot", async () => {
    const db = createMockDb();
    const cscClient = createCscClient();
    const normalizedCarpenters = normalizeFinanceLabel("Hawaii Carpenters Market Recovery Program Fund", "donor");
    const financeIndustryClassifier = vi.fn(async (): Promise<FinanceLabelClassification[]> => [
      {
        rawLabel: "Hawaii Carpenters Market Recovery Program Fund",
        labelType: "donor",
        normalizedLabel: normalizedCarpenters,
        industrySlug: "construction",
        confidence: "high",
        classificationSource: "ai",
        matchedRule: null,
      },
    ]);

    const result = await syncHawaiiCandidateFinance({
      db,
      ...baseInput(),
      cscClient,
      financeIndustryClassifier,
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 4,
      totalReceipts: 4070153.38,
      directContributionTotal: 4070153.38,
      outsideSupportTotal: 500557,
      outsideOpposeTotal: 10000,
      directOccupationRowCount: 2,
      directContributionSizeRowCount: 1,
      outsideGroupCount: 2,
      outsideFunderRowCount: 2,
      skippedOutsideGroupFunderLookupCount: 0,
      resolution: {
        status: "matched",
        committeeId: "CC10174",
        electionPeriod: "2018-2022",
      },
    });

    expect(cscClient.searchAndResolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Josh Green", officeName: "Governor", electionYear: 2022 }),
      undefined
    );
    expect(cscClient.getDirectOccupationAggregates).toHaveBeenCalledWith(
      { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 20 },
      undefined
    );
    expect(cscClient.getContributionSizeAggregates).toHaveBeenCalledWith(
      { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 20 },
      undefined
    );
    expect(cscClient.getIndependentExpenditureGroups).toHaveBeenCalledWith(
      { candidateName: "Josh Green", electionYear: 2022, limit: 20 },
      undefined
    );
    expect(cscClient.getNoncandidateCommitteeFunders).toHaveBeenCalledWith(
      { committeeId: "NC101", electionPeriod: "2020-2022 (KP2)", limit: 20 },
      undefined
    );
    expect(cscClient.getNoncandidateCommitteeFunders).toHaveBeenCalledWith(
      { committeeId: "NC202", electionPeriod: "2020-2022 (KP2)", limit: 20 },
      undefined
    );
    expect(financeIndustryClassifier).not.toHaveBeenCalled();

    expect(db.query.mock.calls.some((call) => call[0] === "BEGIN")).toBe(true);
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "JOSH GREEN",
      "Governor",
      null,
      "CC10174",
      "Green, Josh",
      "2018-2022",
      "active",
      "csc_api",
      SOURCE_URL,
      "2026-06-02T03:04:05.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2022,
      4070153.38,
      4070153.38,
      null,
      null,
      500557,
      10000,
      SOURCE_URL,
      "2026-06-02T03:04:05.000Z",
    ]);

    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.hi_candidate_finance_direct_breakdowns")
      )
    ).toHaveLength(3);
    const outsideGroupCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_outside_groups")
    );
    expect(outsideGroupCalls).toHaveLength(2);
    expect(outsideGroupCalls.map((call) => call[1])).toEqual([
      [
        LINK_ID,
        2022,
        "NC101",
        "Be Change Now",
        "support",
        500557,
        "https://hicscdata.hawaii.gov/resource/riiu-7d4b.json",
        "2026-06-02T03:04:05.000Z",
      ],
      [
        LINK_ID,
        2022,
        "NC202",
        "Hawaii Future PAC",
        "oppose",
        10000,
        "https://hicscdata.hawaii.gov/resource/riiu-7d4b.json",
        "2026-06-02T03:04:05.000Z",
      ],
    ]);
    const outsideBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownCalls).toHaveLength(4);
    expect(outsideBreakdownCalls.map((call) => call[1])).toEqual([
      [
        LINK_ID,
        2022,
        "NC101",
        "support",
        "donor",
        "Hawaii Carpenters Market Recovery Program Fund",
        2086436.92,
        1,
        "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
        "2026-06-02T03:04:05.000Z",
      ],
      [
        LINK_ID,
        2022,
        "NC202",
        "oppose",
        "donor",
        "Hawaii Carpenters Market Recovery Program Fund",
        2086436.92,
        1,
        "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
        "2026-06-02T03:04:05.000Z",
      ],
      [
        LINK_ID,
        2022,
        "NC101",
        "support",
        "industry",
        "construction",
        2086436.92,
        1,
        "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
        "2026-06-02T03:04:05.000Z",
      ],
      [
        LINK_ID,
        2022,
        "NC202",
        "oppose",
        "industry",
        "construction",
        2086436.92,
        1,
        "https://hicscdata.hawaii.gov/resource/rajm-32md.json",
        "2026-06-02T03:04:05.000Z",
      ],
    ]);
    const classificationCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCall?.[1]).toEqual([
      "Hawaii Carpenters Market Recovery Program Fund",
      "donor",
      normalizedCarpenters,
      "construction",
      "medium",
      "rule",
    ]);
  });

  it("uses a trusted committee link without re-resolving by candidate name", async () => {
    const db = createMockDb();
    const cscClient = createCscClient();

    await syncHawaiiCandidateFinance({
      db,
      ...baseInput(),
      cscClient,
      trustedCommittee: {
        committeeId: "CC10174",
        committeeName: "Green, Josh",
        electionPeriod: "2018-2022",
        sourceUrl: SOURCE_URL,
        totalAmount: 4070153.38,
      },
    });

    expect(cscClient.searchAndResolveCandidateCommittee).not.toHaveBeenCalled();
    expect(cscClient.getDirectOccupationAggregates).toHaveBeenCalledWith(
      { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 20 },
      undefined
    );
    expect(cscClient.getIndependentExpenditureGroups).toHaveBeenCalledWith(
      { candidateName: "Josh Green", electionYear: 2022, limit: 20 },
      undefined
    );
    expect(cscClient.getNoncandidateCommitteeFunders).toHaveBeenCalled();
  });

  it("hydrates totals for a trusted committee when the link does not carry a total amount", async () => {
    const db = createMockDb();
    const cscClient = createCscClient();

    const result = await syncHawaiiCandidateFinance({
      db,
      ...baseInput(),
      cscClient,
      trustedCommittee: {
        committeeId: "CC10174",
        committeeName: "Green, Josh",
        electionPeriod: "2018-2022",
        sourceUrl: SOURCE_URL,
      },
    });

    expect(cscClient.searchAndResolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Josh Green", officeName: "Governor", electionYear: 2022 }),
      undefined
    );
    expect(result.totalReceipts).toBe(4070153.38);
    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.hi_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]?.[2]).toBe(4070153.38);
  });

  it("returns an empty result and writes nothing when committee resolution does not match", async () => {
    const db = createMockDb();
    const cscClient = createCscClient({
      resolution: {
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "JOSH GREEN",
        officeNameNormalized: "GOVERNOR",
      },
    });

    const result = await syncHawaiiCandidateFinance({
      db,
      ...baseInput(),
      cscClient,
    });

    expect(result).toMatchObject({
      dryRun: false,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      directOccupationRowCount: 0,
      directContributionSizeRowCount: 0,
      outsideGroupCount: 0,
      outsideFunderRowCount: 0,
      skippedOutsideGroupFunderLookupCount: 0,
      resolution: { status: "unmatched", reason: "no_candidate_committee_match" },
    });
    expect(db.connect).not.toHaveBeenCalled();
    expect(cscClient.getDirectOccupationAggregates).not.toHaveBeenCalled();
    expect(cscClient.getIndependentExpenditureGroups).not.toHaveBeenCalled();
    expect(cscClient.getNoncandidateCommitteeFunders).not.toHaveBeenCalled();
  });

  it("supports dry-run without writing", async () => {
    const db = createMockDb();
    const cscClient = createCscClient();

    const result = await syncHawaiiCandidateFinance({
      db,
      ...baseInput(),
      cscClient,
      dryRun: true,
      directMaxBreakdownsPerCategory: 5,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 4070153.38,
      directContributionTotal: 4070153.38,
      outsideSupportTotal: 500557,
      outsideOpposeTotal: 10000,
      directOccupationRowCount: 2,
      directContributionSizeRowCount: 1,
      outsideGroupCount: 2,
      outsideFunderRowCount: 2,
      skippedOutsideGroupFunderLookupCount: 0,
    });
    expect(cscClient.getDirectOccupationAggregates).toHaveBeenCalledWith(
      { committeeId: "CC10174", electionPeriod: "2018-2022", limit: 5 },
      undefined
    );
    expect(cscClient.getIndependentExpenditureGroups).toHaveBeenCalledWith(
      { candidateName: "Josh Green", electionYear: 2022, limit: 20 },
      undefined
    );
    expect(cscClient.getNoncandidateCommitteeFunders).toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });
});
